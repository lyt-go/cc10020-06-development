"""转换对账领域服务：生成包含原始值、目标值和校验结果的转换对账数据包。

能力边界：
1. 按「转换规则 + 批次号」建立转换对账数据包，逐行固化来源行号、
   实际使用的映射版本与映射指纹；
2. 导出按行断点续跑：导出期间字段映射更换版本时，后来取得的行自动采用
   新映射，并在包内按映射版本分组可追溯；
3. 交付闸门：只有明确声明映射版本且无同版本指纹冲突的文件可交付；
4. 下载生成版本绑定的数据文件（zip 包），下载记录进入包台账；
5. 接入质量校验：复查结论写入质检台账，并与已下载的数据文件版本绑定核对。

存储分区：
- transform_recon_package：对账数据包
- transform_recon_row：行级转换复核数据
- quality_ledger：质检台账
"""
from __future__ import annotations

import io
import json
import os
import threading
import zipfile
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.data_transform import DataTransformService


REVIEW_PASS = "通过"
REVIEW_REJECT = "驳回"
REVIEW_CONCLUSIONS = (REVIEW_PASS, REVIEW_REJECT)

# 包状态
STATUS_PENDING = "待导出"
STATUS_EXPORTING = "导出中"
STATUS_DELIVERABLE = "可交付"
STATUS_BLOCKED = "不可交付"
STATUS_REVIEWING = "质检中"
STATUS_REVIEWED = "质检完成"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class TransformReconService:
    """转换对账数据包与质检台账服务（线程安全，支持同批次并发导出）。"""

    PACKAGE_KEY = "transform_recon_package"
    ROW_KEY = "transform_recon_row"
    LEDGER_KEY = "quality_ledger"

    _batch_locks_guard = threading.Lock()
    _batch_locks: dict[tuple[int, str], threading.Lock] = {}

    def __init__(self) -> None:
        self.transform_svc = DataTransformService()
        base = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "data_store", "recon_files",
        )
        self._file_base = base
        os.makedirs(self._file_base, exist_ok=True)

    # ------------------------------------------------------------------
    # 并发控制：同一批次（规则+批次号）串行化，不同批次互不阻塞
    # ------------------------------------------------------------------
    @classmethod
    def _lock_for_batch(cls, rule_id: int, batch_no: str) -> threading.Lock:
        key = (rule_id, batch_no)
        with cls._batch_locks_guard:
            lock = cls._batch_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                cls._batch_locks[key] = lock
            return lock

    # ------------------------------------------------------------------
    # 数据包建立
    # ------------------------------------------------------------------
    def create_package(
        self,
        rule_id: int,
        batch_no: str,
        source_records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """建立转换对账数据包并登记来源行（幂等：同批次重复请求返回原包）。

        来源行只登记行号与原始载荷，实际映射快照在导出时固化，
        从而能识别导出期间发生的映射版本切换。
        """
        batch_no = str(batch_no or "").strip()
        if not batch_no:
            return {"ok": False, "message": "批次号不能为空"}
        if not source_records:
            return {"ok": False, "message": "来源记录为空，无法建立对账数据包"}

        rule = json_store.get_by_id(DataTransformService.MODULE, rule_id)
        if rule is None:
            return {"ok": False, "message": f"转换规则 {rule_id} 不存在"}

        lock = self._lock_for_batch(rule_id, batch_no)
        with lock:
            existing = self._find_package(rule_id, batch_no)
            if existing is not None:
                return {
                    "ok": True,
                    "message": "对账数据包已存在，返回既有包",
                    "package": existing,
                    "reused": True,
                }

            package = {
                "rule_id": rule_id,
                "rule_no": rule.get("规则编号"),
                "batch_no": batch_no,
                "status": STATUS_PENDING,
                "total_lines": len(source_records),
                "exported_lines": 0,
                "delivered_lines": 0,
                "blocked_lines": 0,
                "reviewed_lines": 0,
                "mapping_versions": [],
                "version_conflicts": [],
                "download_logs": [],
                "created_at": _now(),
                "updated_at": _now(),
            }
            package = json_store.insert(self.PACKAGE_KEY, package)

            rows = []
            for index, record in enumerate(source_records, start=1):
                row = {
                    "package_id": package["id"],
                    "rule_id": rule_id,
                    "batch_no": batch_no,
                    "source_line": index,
                    "source_record": record,
                    "export_state": "待导出",
                    "deliverable": False,
                    "block_reason": "",
                    "mapping_version": "",
                    "mapping_fingerprint": "",
                    "field_traces": [],
                    "transform_errors": [],
                    "quality_passed": None,
                    "exported_at": None,
                    "downloaded": False,
                    "last_download_file_id": "",
                    "download_history": [],
                }
                rows.append(json_store.insert(self.ROW_KEY, row))

            return {
                "ok": True,
                "message": f"对账数据包已建立，登记来源行 {len(rows)} 行",
                "package": self.get_package(package["id"]),
                "reused": False,
            }

    # ------------------------------------------------------------------
    # 断点续跑导出：后来取得的行按当前映射版本固化
    # ------------------------------------------------------------------
    def export_rows(self, package_id: int, limit: int | None = None) -> dict[str, Any]:
        """导出（或续跑）对账数据。

        - 已导出行保持首次导出时固化的映射版本不动；
        - 未导出行按规则当前映射生成轨迹，自动识别导出期间的版本切换；
        - limit 控制本次最多处理多少行，便于分批下载/并发协作。
        """
        package = json_store.get_by_id(self.PACKAGE_KEY, package_id)
        if package is None:
            return {"ok": False, "message": f"对账数据包 {package_id} 不存在"}

        lock = self._lock_for_batch(int(package["rule_id"]), str(package["batch_no"]))
        with lock:
            pending = [
                row for row in json_store.get_all(self.ROW_KEY)
                if row.get("package_id") == package_id and row.get("export_state") == "待导出"
            ]
            pending.sort(key=lambda r: int(r["source_line"]))
            if limit is not None and limit > 0:
                pending = pending[:limit]

            rule_id = int(package["rule_id"])
            rule = json_store.get_by_id(DataTransformService.MODULE, rule_id)
            if rule is None:
                return {"ok": False, "message": f"转换规则 {rule_id} 不存在或已删除"}

            exported_now = 0
            version_counter: dict[str, int] = {}
            for row in pending:
                trace_result = self.transform_svc.trace_record(
                    rule_id=rule_id,
                    source_record=row["source_record"],
                    source_line=int(row["source_line"]),
                )
                mapping_version = str(trace_result.get("mapping_version", "") or "")
                fingerprint = str(trace_result.get("mapping_fingerprint", "") or "")
                field_traces = trace_result.get("field_traces", [])
                failed_fields = [
                    {
                        "target_field": t.get("target_field"),
                        "source_path": t.get("source_path"),
                        "source_line": t.get("source_line"),
                        "original_value": t.get("original_value"),
                        "transform_ok": t.get("transform_ok"),
                        "quality_passed": t.get("quality_passed"),
                        "quality_message": t.get("quality_message"),
                        "mapping_version": t.get("mapping_version"),
                    }
                    for t in field_traces
                    if t.get("transform_ok") is False or t.get("quality_passed") is False
                ]

                deliverable = bool(mapping_version)
                block_reason = "" if deliverable else "规则未声明明确的映射版本，文件不具备交付资格"

                updates = {
                    "export_state": "已导出",
                    "deliverable": deliverable,
                    "block_reason": block_reason,
                    "mapping_version": mapping_version,
                    "mapping_fingerprint": fingerprint,
                    "field_traces": field_traces,
                    "failed_fields": failed_fields,
                    "transform_errors": trace_result.get("errors", []),
                    "target_data": trace_result.get("data", {}),
                    "quality_passed": (trace_result.get("quality_check") or {}).get("passed"),
                    "exported_at": _now(),
                }
                json_store.update(self.ROW_KEY, int(row["id"]), updates)
                exported_now += 1
                if mapping_version:
                    version_counter[mapping_version] = version_counter.get(mapping_version, 0) + 1

            refreshed = self._refresh_package(package_id)
            return {
                "ok": True,
                "message": f"本次导出 {exported_now} 行，剩余待导出 {refreshed['total_lines'] - refreshed['exported_lines']} 行",
                "exported_now": exported_now,
                "versions_this_round": version_counter,
                "package": self.get_package(package_id),
            }

    # ------------------------------------------------------------------
    # 下载：仅交付明确版本且无冲突的文件，版本与下载记录绑定
    # ------------------------------------------------------------------
    def download_package(self, package_id: int) -> dict[str, Any]:
        """生成版本绑定的转换对账数据 zip 包并登记下载台账。"""
        package = json_store.get_by_id(self.PACKAGE_KEY, package_id)
        if package is None:
            return {"ok": False, "message": f"对账数据包 {package_id} 不存在"}

        lock = self._lock_for_batch(int(package["rule_id"]), str(package["batch_no"]))
        with lock:
            package = self._refresh_package(package_id)
            rows = self._rows_of(package_id)
            deliverable_rows = [r for r in rows if r.get("deliverable")]
            if not deliverable_rows:
                return {
                    "ok": False,
                    "message": (
                        f"数据包当前状态为「{package['status']}」，只有明确版本的文件可交付；"
                        f"阻断行 {package['blocked_lines']} 行，版本冲突 "
                        f"{len(package['version_conflicts'])} 处"
                    ),
                }
            if package["status"] == STATUS_BLOCKED:
                return {
                    "ok": False,
                    "message": (
                        "数据包存在版本冲突或未声明版本的行，整包不可交付："
                        f"阻断行 {package['blocked_lines']} 行，"
                        f"版本冲突 {len(package['version_conflicts'])} 处"
                    ),
                }
            versions = sorted({r["mapping_version"] for r in deliverable_rows})
            file_seq = len(package.get("download_logs", [])) + 1
            file_id = f"F-{package['batch_no']}-{file_seq:03d}"

            manifest = {
                "file_id": file_id,
                "package_id": package_id,
                "package_no": package["batch_no"],
                "rule_id": package["rule_id"],
                "rule_no": package["rule_no"],
                "generated_at": _now(),
                "mapping_versions": versions,
                "version_fingerprints": {
                    r["mapping_version"]: r["mapping_fingerprint"]
                    for r in deliverable_rows
                },
                "total_lines": package["total_lines"],
                "delivered_lines": len(deliverable_rows),
                "blocked_lines": package["blocked_lines"],
                "rows": [
                    {
                        "source_line": r["source_line"],
                        "mapping_version": r["mapping_version"],
                        "mapping_fingerprint": r["mapping_fingerprint"],
                        "file_name": f"recon_rows/row_{r['source_line']:06d}.json",
                    }
                    for r in deliverable_rows
                ],
            }
            blocked_manifest = [
                {
                    "source_line": r["source_line"],
                    "mapping_version": r["mapping_version"],
                    "block_reason": r["block_reason"],
                }
                for r in rows if not r.get("deliverable")
            ]

            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
                zf.writestr(
                    "undeliverable_rows.json",
                    json.dumps(blocked_manifest, ensure_ascii=False, indent=2),
                )
                zf.writestr("recon_rows.csv", self._build_csv(deliverable_rows))
                for r in deliverable_rows:
                    payload = {
                        "file_id": file_id,
                        "package_id": package_id,
                        "batch_no": package["batch_no"],
                        "source_line": r["source_line"],
                        "source_record": r["source_record"],
                        "target_data": r.get("target_data", {}),
                        "mapping_version": r["mapping_version"],
                        "mapping_fingerprint": r["mapping_fingerprint"],
                        "quality_passed": r.get("quality_passed"),
                        "failed_fields": r.get("failed_fields", []),
                        "field_traces": r.get("field_traces", []),
                        "exported_at": r.get("exported_at"),
                    }
                    zf.writestr(
                        f"recon_rows/row_{r['source_line']:06d}.json",
                        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
                    )
            content = buffer.getvalue()

            download_log = {
                "file_id": file_id,
                "downloaded_at": _now(),
                "mapping_versions": versions,
                "source_lines": [r["source_line"] for r in deliverable_rows],
                "size_bytes": len(content),
            }
            logs = list(package.get("download_logs", []))
            logs.append(download_log)
            json_store.update(self.PACKAGE_KEY, package_id, {"download_logs": logs})
            for r in deliverable_rows:
                history = list(r.get("download_history", []))
                history.append({
                    "file_id": file_id,
                    "mapping_version": r["mapping_version"],
                    "mapping_fingerprint": r["mapping_fingerprint"],
                    "downloaded_at": download_log["downloaded_at"],
                })
                json_store.update(self.ROW_KEY, int(r["id"]), {
                    "downloaded": True,
                    "last_download_file_id": file_id,
                    "download_history": history,
                })

            return {
                "ok": True,
                "file_id": file_id,
                "filename": f"recon_{package['batch_no']}_{file_id}.zip",
                "media_type": "application/zip",
                "content": content,
                "mapping_versions": versions,
                "delivered_lines": len(deliverable_rows),
                "package": self.get_package(package_id),
            }

    # ------------------------------------------------------------------
    # 质量复查：结论写入质检台账并绑定已下载文件版本
    # ------------------------------------------------------------------
    def review_row(
        self,
        package_id: int,
        source_line: int,
        conclusion: str,
        reviewer: str,
        remark: str = "",
    ) -> dict[str, Any]:
        """对单行转换复核数据登记质检结论。

        校验：结论合法、行可交付、该行已被某个版本文件下载；
        台账记录下载时的映射版本/指纹与当前规则映射的核对结果。
        """
        conclusion = str(conclusion or "").strip()
        if conclusion not in REVIEW_CONCLUSIONS:
            return {"ok": False, "message": f"质检结论必须是 {REVIEW_CONCLUSIONS} 之一"}

        package = json_store.get_by_id(self.PACKAGE_KEY, package_id)
        if package is None:
            return {"ok": False, "message": f"对账数据包 {package_id} 不存在"}

        lock = self._lock_for_batch(int(package["rule_id"]), str(package["batch_no"]))
        with lock:
            row = self._find_row(package_id, int(source_line))
            if row is None:
                return {"ok": False, "message": f"来源行 {source_line} 不在数据包内"}
            if not row.get("deliverable"):
                return {"ok": False, "message": f"来源行 {source_line} 不具备交付资格：{row.get('block_reason')}"}
            if not row.get("downloaded"):
                return {"ok": False, "message": f"来源行 {source_line} 尚未进入任何已下载文件，无法复查"}

            rule = json_store.get_by_id(DataTransformService.MODULE, int(package["rule_id"]))
            current_mapping = self.transform_svc._parse_mapping(str(rule.get("字段映射", "{}"))) if rule else {}
            current_fingerprint = DataTransformService.mapping_fingerprint(current_mapping) if current_mapping else ""
            current_version = DataTransformService.mapping_version_of(rule) if rule else ""
            version_match = (
                row.get("mapping_fingerprint") == current_fingerprint
                and bool(current_fingerprint)
            )

            download_history = list(row.get("download_history", []))
            first_file = download_history[0] if download_history else {}
            existing_ledger = self._find_ledger(package_id, int(source_line))
            entry = {
                "package_id": package_id,
                "batch_no": package["batch_no"],
                "rule_id": package["rule_id"],
                "rule_no": package["rule_no"],
                "source_line": int(source_line),
                "conclusion": conclusion,
                "reviewer": str(reviewer or "").strip() or "质检同事",
                "remark": str(remark or ""),
                # 复查人员首次下载的文件版本（对账基准）与最近一次下载文件都留痕
                "downloaded_file_id": row.get("last_download_file_id", ""),
                "first_download_file_id": first_file.get("file_id", row.get("last_download_file_id", "")),
                "downloaded_mapping_version": row.get("mapping_version", ""),
                "downloaded_fingerprint": row.get("mapping_fingerprint", ""),
                "download_history": download_history,
                "current_rule_version": current_version,
                "current_fingerprint": current_fingerprint,
                "version_match": version_match,
                "reviewed_at": _now(),
            }

            if existing_ledger is None:
                saved = json_store.insert(self.LEDGER_KEY, entry)
                ledger_id = saved["id"]
                history: list[dict[str, Any]] = []
            else:
                ledger_id = int(existing_ledger["id"])
                history = list(existing_ledger.get("review_history", []))
                history.append({
                    "conclusion": existing_ledger.get("conclusion"),
                    "reviewer": existing_ledger.get("reviewer"),
                    "remark": existing_ledger.get("remark"),
                    "reviewed_at": existing_ledger.get("reviewed_at"),
                    "downloaded_file_id": existing_ledger.get("downloaded_file_id"),
                    "downloaded_mapping_version": existing_ledger.get("downloaded_mapping_version"),
                })
                entry["review_history"] = history
                json_store.update(self.LEDGER_KEY, ledger_id, entry)

            self._refresh_package(package_id)
            return {
                "ok": True,
                "message": (
                    f"来源行 {source_line} 复查结论「{conclusion}」已写入质检台账"
                    + ("" if version_match else "；注意：当前规则映射与下载时版本不一致，结论按下载文件核对")
                ),
                "ledger": self.get_ledger(ledger_id),
                "version_match": version_match,
            }

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def list_packages(self, batch_no: str | None = None) -> list[dict[str, Any]]:
        packages = json_store.get_all(self.PACKAGE_KEY)
        if batch_no:
            packages = [p for p in packages if str(p.get("batch_no", "")) == str(batch_no)]
        return [self._decorate_package(p) for p in packages]

    def get_package(self, package_id: int) -> dict[str, Any] | None:
        package = json_store.get_by_id(self.PACKAGE_KEY, package_id)
        return self._decorate_package(package) if package else None

    def list_rows(self, package_id: int) -> dict[str, Any]:
        package = json_store.get_by_id(self.PACKAGE_KEY, package_id)
        if package is None:
            return {"ok": False, "message": f"对账数据包 {package_id} 不存在", "rows": []}
        rows = self._rows_of(package_id)
        return {"ok": True, "package_id": package_id, "total": len(rows), "rows": rows}

    def get_failed_fields(self, package_id: int) -> dict[str, Any]:
        """导出失败字段清单，每条失败都关联到收到的源记录与来源行号。"""
        package = json_store.get_by_id(self.PACKAGE_KEY, package_id)
        if package is None:
            return {"ok": False, "message": f"对账数据包 {package_id} 不存在", "failures": []}
        failures = []
        for row in self._rows_of(package_id):
            for failed in row.get("failed_fields", []):
                failures.append({
                    **failed,
                    "package_id": package_id,
                    "batch_no": package["batch_no"],
                    "source_record": row.get("source_record"),
                })
        return {"ok": True, "package_id": package_id, "total": len(failures), "failures": failures}

    def list_ledger(self, package_id: int | None = None) -> list[dict[str, Any]]:
        entries = json_store.get_all(self.LEDGER_KEY)
        if package_id is not None:
            entries = [e for e in entries if e.get("package_id") == package_id]
        return entries

    def get_ledger(self, ledger_id: int) -> dict[str, Any] | None:
        return json_store.get_by_id(self.LEDGER_KEY, ledger_id)

    # ------------------------------------------------------------------
    # 内部辅助
    # ------------------------------------------------------------------
    def _find_package(self, rule_id: int, batch_no: str) -> dict[str, Any] | None:
        for package in json_store.get_all(self.PACKAGE_KEY):
            if int(package.get("rule_id", 0)) == rule_id and str(package.get("batch_no")) == batch_no:
                return package
        return None

    def _rows_of(self, package_id: int) -> list[dict[str, Any]]:
        rows = [r for r in json_store.get_all(self.ROW_KEY) if r.get("package_id") == package_id]
        rows.sort(key=lambda r: int(r["source_line"]))
        return rows

    def _find_row(self, package_id: int, source_line: int) -> dict[str, Any] | None:
        for row in json_store.get_all(self.ROW_KEY):
            if row.get("package_id") == package_id and int(row.get("source_line", -1)) == source_line:
                return row
        return None

    def _find_ledger(self, package_id: int, source_line: int) -> dict[str, Any] | None:
        for entry in json_store.get_all(self.LEDGER_KEY):
            if entry.get("package_id") == package_id and int(entry.get("source_line", -1)) == source_line:
                return entry
        return None

    def _refresh_package(self, package_id: int) -> dict[str, Any]:
        """根据行状态重算数据包汇总、版本冲突与交付状态。"""
        package = json_store.get_by_id(self.PACKAGE_KEY, package_id)
        rows = self._rows_of(package_id)
        exported = [r for r in rows if r.get("export_state") == "已导出"]
        deliverable = [r for r in exported if r.get("deliverable")]
        blocked = [r for r in exported if not r.get("deliverable")]

        # 同版本号出现不同指纹 → 版本无法唯一标识映射，判定为冲突，阻断交付
        fingerprints_by_version: dict[str, set[str]] = {}
        for r in exported:
            version = r.get("mapping_version", "")
            if not version:
                continue
            fingerprints_by_version.setdefault(version, set()).add(r.get("mapping_fingerprint", ""))
        conflicts = [
            {"mapping_version": version, "fingerprints": sorted(fp)}
            for version, fp in fingerprints_by_version.items() if len(fp) > 1
        ]
        conflict_versions = {c["mapping_version"] for c in conflicts}
        for r in exported:
            if r.get("deliverable") and r.get("mapping_version") in conflict_versions:
                json_store.update(self.ROW_KEY, int(r["id"]), {
                    "deliverable": False,
                    "block_reason": f"映射版本 {r['mapping_version']} 存在多个映射指纹，版本不唯一",
                })
        if conflict_versions:
            rows = self._rows_of(package_id)
            deliverable = [r for r in rows if r.get("deliverable")]
            blocked = [r for r in rows if r.get("export_state") == "已导出" and not r.get("deliverable")]

        versions = sorted({r["mapping_version"] for r in deliverable})
        all_exported = len(exported) == len(rows) and len(rows) > 0
        ledger_entries = self.list_ledger(package_id)
        reviewed_lines = len({e["source_line"] for e in ledger_entries})

        if not all_exported:
            status = STATUS_PENDING if not exported else STATUS_EXPORTING
        elif blocked or conflicts:
            status = STATUS_BLOCKED
        elif reviewed_lines == 0:
            status = STATUS_DELIVERABLE
        elif reviewed_lines >= len(deliverable):
            status = STATUS_REVIEWED
        else:
            status = STATUS_REVIEWING

        updates = {
            "status": status,
            "exported_lines": len(exported),
            "delivered_lines": len(deliverable),
            "blocked_lines": len(blocked),
            "reviewed_lines": reviewed_lines,
            "mapping_versions": versions,
            "version_conflicts": conflicts,
            "updated_at": _now(),
        }
        json_store.update(self.PACKAGE_KEY, package_id, updates)
        return json_store.get_by_id(self.PACKAGE_KEY, package_id)

    def _decorate_package(self, package: dict[str, Any]) -> dict[str, Any]:
        """读取时顺带刷新派生状态，保证列表与实时状态一致。"""
        refreshed = self._refresh_package(int(package["id"]))
        return refreshed

    @staticmethod
    def _build_csv(rows: list[dict[str, Any]]) -> str:
        import csv

        output = io.StringIO()
        output.write("﻿")
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow([
            "批次号", "来源行号", "目标字段", "源字段路径", "原始值", "目标值",
            "转换结果", "质量校验结果", "校验说明", "映射版本", "映射指纹",
        ])
        for row in rows:
            for trace in row.get("field_traces", []):
                writer.writerow([
                    row.get("batch_no"),
                    row.get("source_line"),
                    trace.get("target_field"),
                    trace.get("source_path"),
                    json.dumps(trace.get("original_value"), ensure_ascii=False, default=str),
                    json.dumps(trace.get("target_value"), ensure_ascii=False, default=str),
                    "成功" if trace.get("transform_ok") else "失败",
                    "通过" if trace.get("quality_passed") else "不通过",
                    trace.get("quality_message") if trace.get("quality_passed") is False
                    else trace.get("transform_message"),
                    trace.get("mapping_version"),
                    trace.get("mapping_fingerprint"),
                ])
        return output.getvalue()
