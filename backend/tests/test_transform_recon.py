"""转换对账数据包测试：覆盖版本化交付、断点续跑与质检台账串联。

对应需求：
1. 从转换前后字段数据导出包含原始值、目标值、校验结果的对账数据包；
2. 并发导出同一批次时只有明确版本的文件可交付；
3. 文件保存实际使用的映射版本与来源行号，质检结论可与下载记录核对；
4. 质量校验串联对账数据包，复查结果同步质检台账并绑定下载文件版本；
5. 导出期间映射换成两个版本：已下载旧版的行保持旧版，后来取得的行识别新映射；
6. 导出失败字段关联到收到的源记录。
"""
from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import threading
import unittest
import zipfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.persistence import JsonStore, json_store as global_store
from app.services.data_quality_rule import DataQualityRuleService
from app.services.data_transform import DataTransformService
from app.services.transform_recon import (
    STATUS_BLOCKED,
    STATUS_DELIVERABLE,
    STATUS_EXPORTING,
    STATUS_REVIEWED,
    STATUS_REVIEWING,
    TransformReconService,
)


class TransformReconTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = JsonStore(base_dir=self._tmp.name)
        import app.persistence
        app.persistence.json_store = self.store

        import app.services.data_transform as dt_mod
        import app.services.data_quality_rule as dq_mod
        import app.services.transform_recon as recon_mod
        import importlib
        importlib.reload(dq_mod)
        importlib.reload(dt_mod)
        importlib.reload(recon_mod)

        self.transform_svc = DataTransformService()
        self.quality_svc = DataQualityRuleService()
        self.recon_svc = TransformReconService()

        self.records = [
            {"id": 1, "name": "订单A", "value": "100"},
            {"id": 2, "name": "订单B", "value": "200"},
            {"id": 3, "name": "", "value": "300"},  # name 为空，触发质量失败
            {"id": 4, "name": "订单D", "value": "bad"},  # 数字转换失败
        ]

    def tearDown(self) -> None:
        import app.persistence
        app.persistence.json_store = global_store
        self._tmp.cleanup()

    def _create_rule(self, version: str = "v1",
                     mapping: dict | None = None,
                     func: str = "passthrough",
                     status: str = "已生效") -> dict:
        mapping = mapping or {"target_id": "id", "target_name": "name", "target_value": "value"}
        rule = self.store.insert("data_transform", {
            "规则编号": "TRANS-RECON-001",
            "源Schema": "json",
            "目标Schema": "json",
            "字段映射": json.dumps(mapping, ensure_ascii=False),
            "转换函数": func,
            "默认值填充": "使用默认值",
            "异常策略": "跳过",
            "规则版本": version,
            "status": status,
            "pending": False,
            "abnormal": False,
        })
        return rule

    def _create_quality_rule(self, field: str = "target_name",
                             expression: str = "not_empty") -> dict:
        return self.store.insert("data_quality_rule", {
            "规则编号": "QR-RECON-001",
            "数据来源": "data_transform",
            "校验字段": field,
            "校验表达式": expression,
            "阈值下界": "0",
            "阈值上界": "0",
            "阻断策略": "警告",
            "规则版本": "v1",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        })

    def _make_package(self, batch_no: str = "BATCH-001", version: str = "v1"):
        rule = self._create_rule(version=version)
        result = self.recon_svc.create_package(rule["id"], batch_no, self.records)
        self.assertTrue(result["ok"], result.get("message"))
        return rule, result["package"]


class TestTraceableTransform(TransformReconTestBase):
    def test_field_traces_capture_original_target_and_check(self):
        """轨迹包含原始值、目标值、校验结果、映射版本与来源行号。"""
        self._create_quality_rule()
        rule = self._create_rule(version="v1")
        trace = self.transform_svc.trace_record(rule["id"], self.records[0], source_line=1)

        self.assertTrue(trace["ok"])
        self.assertEqual(trace["source_line"], 1)
        self.assertEqual(trace["mapping_version"], "v1")
        self.assertTrue(trace["mapping_fingerprint"])
        by_target = {t["target_field"]: t for t in trace["field_traces"]}
        self.assertEqual(by_target["target_id"]["original_value"], 1)
        self.assertEqual(by_target["target_id"]["target_value"], 1)
        self.assertTrue(by_target["target_id"]["transform_ok"])
        self.assertTrue(by_target["target_name"]["quality_passed"])

    def test_failed_quality_marked_at_field_level(self):
        self._create_quality_rule()
        rule = self._create_rule(version="v1")
        trace = self.transform_svc.trace_record(rule["id"], self.records[2], source_line=3)
        by_target = {t["target_field"]: t for t in trace["field_traces"]}
        self.assertFalse(by_target["target_name"]["quality_passed"])
        self.assertIn("为空", by_target["target_name"]["quality_message"])
        self.assertEqual(by_target["target_name"]["original_value"], "")

    def test_apply_transform_keeps_legacy_contract(self):
        rule = self._create_rule(version="v1")
        result = self.transform_svc.apply_transform(rule["id"], self.records[0])
        # 既有契约字段保持不变
        for key in ("ok", "data", "errors", "fields_transformed", "total_fields", "quality_check"):
            self.assertIn(key, result)
        self.assertEqual(result["data"]["target_name"], "订单A")
        self.assertIn("field_traces", result)


class TestReconPackageDelivery(TransformReconTestBase):
    def test_export_full_package_is_deliverable(self):
        rule, package = self._make_package()
        export = self.recon_svc.export_rows(package["id"])
        self.assertTrue(export["ok"])
        self.assertEqual(export["exported_now"], 4)

        refreshed = self.recon_svc.get_package(package["id"])
        self.assertEqual(refreshed["status"], STATUS_DELIVERABLE)
        self.assertEqual(refreshed["exported_lines"], 4)
        self.assertEqual(refreshed["delivered_lines"], 4)
        self.assertEqual(refreshed["mapping_versions"], ["v1"])

        rows = self.recon_svc.list_rows(package["id"])["rows"]
        for row in rows:
            self.assertTrue(row["deliverable"])
            self.assertTrue(row["mapping_version"])
            self.assertIsInstance(row["source_line"], int)

    def test_missing_explicit_version_blocks_delivery(self):
        """规则未声明明确映射版本时，文件不具备交付资格。"""
        rule = self._create_rule(version="")  # 空版本
        result = self.recon_svc.create_package(rule["id"], "BATCH-NO-VER", self.records)
        package = result["package"]
        self.recon_svc.export_rows(package["id"])

        refreshed = self.recon_svc.get_package(package["id"])
        self.assertEqual(refreshed["status"], STATUS_BLOCKED)
        self.assertEqual(refreshed["blocked_lines"], 4)
        self.assertEqual(refreshed["delivered_lines"], 0)

        download = self.recon_svc.download_package(package["id"])
        self.assertFalse(download["ok"])
        self.assertIn("明确版本", download["message"])

    def test_create_package_idempotent_for_same_batch(self):
        rule, package = self._make_package()
        again = self.recon_svc.create_package(rule["id"], "BATCH-001", self.records)
        self.assertTrue(again["reused"])
        self.assertEqual(again["package"]["id"], package["id"])
        packages = self.recon_svc.list_packages()
        self.assertEqual(len(packages), 1)

    def test_concurrent_export_same_batch_serializes(self):
        """并发导出同一批次：锁串行化，行不重复导出，结果一致。"""
        rule, package = self._make_package()
        errors: list[str] = []

        def worker() -> None:
            try:
                self.recon_svc.export_rows(package["id"], limit=2)
            except Exception as exc:  # pragma: no cover - 并发回归保护
                errors.append(str(exc))

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        rows = self.recon_svc.list_rows(package["id"])["rows"]
        exported = [r for r in rows if r["export_state"] == "已导出"]
        self.assertEqual(len(exported), 4)
        self.assertEqual(len({r["source_line"] for r in exported}), 4)


class TestResumableExportWithVersionSwitch(TransformReconTestBase):
    def _switch_rule_mapping(self, rule_id: int, version: str, mapping: dict) -> None:
        self.store.update("data_transform", rule_id, {
            "规则版本": version,
            "字段映射": json.dumps(mapping, ensure_ascii=False),
        })

    def test_resumable_export_recognizes_later_mapping_version(self):
        """导出期间映射换成两个版本：旧行保持旧版，新行识别新映射。"""
        rule, package = self._make_package(version="v1")

        # 第一批只导出前两行（模拟复查人员先拿到旧版部分记录）
        first = self.recon_svc.export_rows(package["id"], limit=2)
        self.assertEqual(first["exported_now"], 2)
        first_download = self.recon_svc.download_package(package["id"])
        self.assertTrue(first_download["ok"])
        self.assertEqual(first_download["mapping_versions"], ["v1"])
        first_file_id = first_download["file_id"]

        rows = self.recon_svc.list_rows(package["id"])["rows"]
        self.assertTrue(all(r["downloaded"] for r in rows[:2]))
        self.assertEqual(rows[0]["last_download_file_id"], first_file_id)

        # 导出期间规则映射切换到 v2（字段目标名也改了）
        new_mapping = {
            "target_id": "id",
            "customer_name": "name",
            "target_value": "value",
        }
        self._switch_rule_mapping(rule["id"], "v2", new_mapping)

        # 续跑剩余行，应自动采用 v2
        second = self.recon_svc.export_rows(package["id"])
        self.assertEqual(second["exported_now"], 2)
        self.assertEqual(second["versions_this_round"], {"v2": 2})

        refreshed = self.recon_svc.get_package(package["id"])
        self.assertEqual(sorted(refreshed["mapping_versions"]), ["v1", "v2"])
        self.assertEqual(refreshed["status"], STATUS_DELIVERABLE)
        self.assertEqual(refreshed["version_conflicts"], [])

        rows = self.recon_svc.list_rows(package["id"])["rows"]
        self.assertEqual(rows[0]["mapping_version"], "v1")
        self.assertEqual(rows[1]["mapping_version"], "v1")
        self.assertEqual(rows[2]["mapping_version"], "v2")
        self.assertEqual(rows[3]["mapping_version"], "v2")
        # 旧行字段轨迹仍是旧目标字段，新行采用新目标字段
        old_targets = {t["target_field"] for t in rows[0]["field_traces"]}
        new_targets = {t["target_field"] for t in rows[2]["field_traces"]}
        self.assertIn("target_name", old_targets)
        self.assertIn("customer_name", new_targets)
        self.assertNotIn("customer_name", old_targets)

        # 二次下载的包同时带两个明确版本，并在清单中逐行标注
        second_download = self.recon_svc.download_package(package["id"])
        self.assertTrue(second_download["ok"])
        self.assertEqual(sorted(second_download["mapping_versions"]), ["v1", "v2"])
        with zipfile.ZipFile(io.BytesIO(second_download["content"])) as zf:
            manifest = json.loads(zf.read("manifest.json"))
            self.assertEqual(manifest["file_id"], second_download["file_id"])
            line_versions = {item["source_line"]: item["mapping_version"]
                             for item in manifest["rows"]}
            self.assertEqual(line_versions[1], "v1")
            self.assertEqual(line_versions[3], "v2")
            row3 = json.loads(zf.read("recon_rows/row_000003.json"))
            self.assertEqual(row3["mapping_version"], "v2")
            self.assertIn("customer_name", row3["target_data"])

    def test_same_version_with_changed_mapping_becomes_conflict(self):
        """版本号不变但映射内容被改动：同版本出现两个指纹，判定冲突阻断交付。"""
        rule, package = self._make_package(version="v1")
        self.recon_svc.export_rows(package["id"], limit=2)

        changed_mapping = {"target_id": "id", "target_name": "name", "amount": "value"}
        self._switch_rule_mapping(rule["id"], "v1", changed_mapping)
        self.recon_svc.export_rows(package["id"])

        refreshed = self.recon_svc.get_package(package["id"])
        self.assertEqual(refreshed["status"], STATUS_BLOCKED)
        self.assertEqual(len(refreshed["version_conflicts"]), 1)
        self.assertEqual(refreshed["version_conflicts"][0]["mapping_version"], "v1")
        self.assertGreaterEqual(len(refreshed["version_conflicts"][0]["fingerprints"]), 2)

        download = self.recon_svc.download_package(package["id"])
        self.assertFalse(download["ok"])


class TestDownloadedFilesAndQualityLedger(TransformReconTestBase):
    def test_download_binds_version_and_file_id_per_row(self):
        rule, package = self._make_package()
        self.recon_svc.export_rows(package["id"])
        download = self.recon_svc.download_package(package["id"])
        self.assertTrue(download["ok"])

        with zipfile.ZipFile(io.BytesIO(download["content"])) as zf:
            names = zf.namelist()
            self.assertIn("manifest.json", names)
            self.assertIn("recon_rows.csv", names)
            self.assertIn("recon_rows/row_000001.json", names)
            self.assertIn("undeliverable_rows.json", names)
            row1 = json.loads(zf.read("recon_rows/row_000001.json"))
            self.assertEqual(row1["source_line"], 1)
            self.assertEqual(row1["mapping_version"], "v1")
            self.assertEqual(row1["source_record"], self.records[0])

        refreshed = self.recon_svc.get_package(package["id"])
        self.assertEqual(len(refreshed["download_logs"]), 1)
        log = refreshed["download_logs"][0]
        self.assertEqual(log["file_id"], download["file_id"])
        self.assertEqual(log["mapping_versions"], ["v1"])
        self.assertEqual(log["source_lines"], [1, 2, 3, 4])

    def test_review_requires_prior_download(self):
        rule, package = self._make_package()
        self.recon_svc.export_rows(package["id"])
        # 未下载先复查 → 拒绝
        review = self.recon_svc.review_row(package["id"], 1, "通过", "质检员甲")
        self.assertFalse(review["ok"])
        self.assertIn("尚未进入任何已下载文件", review["message"])

    def test_review_syncs_ledger_with_downloaded_file_version(self):
        rule, package = self._make_package()
        self.recon_svc.export_rows(package["id"])
        self.recon_svc.download_package(package["id"])

        review = self.recon_svc.review_row(package["id"], 1, "通过", "质检员甲", "核对一致")
        self.assertTrue(review["ok"], review.get("message"))
        self.assertTrue(review["version_match"])

        ledger = self.recon_svc.list_ledger(package["id"])
        self.assertEqual(len(ledger), 1)
        entry = ledger[0]
        self.assertEqual(entry["source_line"], 1)
        self.assertEqual(entry["conclusion"], "通过")
        self.assertEqual(entry["reviewer"], "质检员甲")
        self.assertEqual(entry["downloaded_mapping_version"], "v1")
        self.assertTrue(entry["downloaded_fingerprint"])
        self.assertTrue(entry["downloaded_file_id"].startswith("F-"))
        self.assertEqual(entry["batch_no"], package["batch_no"])

        # 部分复查 → 质检中；全部复查 → 质检完成
        self.assertEqual(self.recon_svc.get_package(package["id"])["status"], STATUS_REVIEWING)
        for line in (2, 3, 4):
            r = self.recon_svc.review_row(package["id"], line, "通过", "质检员甲")
            self.assertTrue(r["ok"])
        self.assertEqual(self.recon_svc.get_package(package["id"])["status"], STATUS_REVIEWED)
        self.assertEqual(self.recon_svc.get_package(package["id"])["reviewed_lines"], 4)

    def test_ledger_flags_version_drift_after_download(self):
        """下载后规则继续演进：台账标记版本不一致，但结论仍绑定下载时文件。"""
        rule, package = self._make_package()
        self.recon_svc.export_rows(package["id"])
        self.recon_svc.download_package(package["id"])

        self.store.update("data_transform", rule["id"], {
            "字段映射": json.dumps({
                "target_id": "id",
                "target_name": "name",
                "target_value": "value",
                "extra": "id",
            }, ensure_ascii=False),
        })
        review = self.recon_svc.review_row(package["id"], 1, "驳回", "质检员乙", "映射已变更需复核")
        self.assertTrue(review["ok"])
        self.assertFalse(review["version_match"])
        entry = self.recon_svc.list_ledger(package["id"])[0]
        self.assertFalse(entry["version_match"])
        # 绑定的仍是旧下载文件与旧指纹
        self.assertEqual(entry["downloaded_mapping_version"], "v1")
        self.assertNotEqual(entry["downloaded_fingerprint"], entry["current_fingerprint"])

    def test_invalid_conclusion_rejected(self):
        rule, package = self._make_package()
        self.recon_svc.export_rows(package["id"])
        self.recon_svc.download_package(package["id"])
        review = self.recon_svc.review_row(package["id"], 1, "待定", "质检员甲")
        self.assertFalse(review["ok"])

    def test_re_review_keeps_history(self):
        rule, package = self._make_package()
        self.recon_svc.export_rows(package["id"])
        self.recon_svc.download_package(package["id"])
        self.recon_svc.review_row(package["id"], 1, "驳回", "质检员甲", "首次驳回")
        self.recon_svc.review_row(package["id"], 1, "通过", "质检员甲", "整改后通过")
        ledger = self.recon_svc.list_ledger(package["id"])
        self.assertEqual(len(ledger), 1)
        self.assertEqual(ledger[0]["conclusion"], "通过")
        self.assertEqual(len(ledger[0]["review_history"]), 1)
        self.assertEqual(ledger[0]["review_history"][0]["conclusion"], "驳回")

    def test_ledger_keeps_first_and_latest_downloaded_files(self):
        """复查人员先下载旧版部分文件、再下载完整文件：台账保留两次下载痕迹。"""
        rule, package = self._make_package()
        self.recon_svc.export_rows(package["id"], limit=2)
        first = self.recon_svc.download_package(package["id"])
        self.assertTrue(first["ok"])

        self.recon_svc.export_rows(package["id"])
        second = self.recon_svc.download_package(package["id"])
        self.assertTrue(second["ok"])
        self.assertNotEqual(first["file_id"], second["file_id"])

        self.recon_svc.review_row(package["id"], 1, "通过", "质检员甲")
        entry = self.recon_svc.list_ledger(package["id"])[0]
        self.assertEqual(entry["first_download_file_id"], first["file_id"])
        self.assertEqual(entry["downloaded_file_id"], second["file_id"])
        self.assertEqual(
            [h["file_id"] for h in entry["download_history"]],
            [first["file_id"], second["file_id"]],
        )
        # 行级下载历史同样完整
        row = self.recon_svc._find_row(package["id"], 1)
        self.assertEqual(len(row["download_history"]), 2)
        self.assertEqual(row["last_download_file_id"], second["file_id"])


class TestFailedFieldsTieToSourceRecords(TransformReconTestBase):
    def test_failed_fields_reference_source_record_and_line(self):
        self._create_quality_rule()
        rule, package = self._make_package()
        self.recon_svc.export_rows(package["id"])
        failures_result = self.recon_svc.get_failed_fields(package["id"])
        self.assertTrue(failures_result["ok"])
        failures = failures_result["failures"]
        self.assertGreaterEqual(len(failures), 1)

        # 第3行 name 为空触发 not_empty 失败
        name_failure = next(f for f in failures if f["target_field"] == "target_name")
        self.assertEqual(name_failure["source_line"], 3)
        self.assertEqual(name_failure["source_record"], self.records[2])
        self.assertFalse(name_failure["quality_passed"])
        self.assertEqual(name_failure["mapping_version"], "v1")
        self.assertIn("为空", name_failure["quality_message"])

    def test_transform_failure_keeps_original_value(self):
        # to_integer 遇到非数字会抛 ValueError，失败轨迹仍保留原始值
        rule = self._create_rule(version="v9", func="to_integer")
        result = self.recon_svc.create_package(rule["id"], "BATCH-INT", [
            {"id": 1, "name": "x", "value": "10"},
            {"id": 2, "name": "y", "value": "oops"},
        ])
        package = result["package"]
        self.recon_svc.export_rows(package["id"])
        failures = self.recon_svc.get_failed_fields(package["id"])["failures"]
        bad = next(f for f in failures if f["source_line"] == 2 and f["target_field"] == "target_value")
        self.assertEqual(bad["original_value"], "oops")
        self.assertFalse(bad["transform_ok"])
        self.assertEqual(bad["source_record"]["id"], 2)


class TestReconRouter(unittest.TestCase):
    """直接调用路由处理函数验证 HTTP 层串联（避免引入额外测试依赖）。"""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = JsonStore(base_dir=self._tmp.name)
        import app.persistence
        app.persistence.json_store = self.store

        import app.services.data_quality_rule as dq_mod
        import app.services.data_transform as dt_mod
        import app.services.transform_recon as recon_mod
        import app.routers.recon as router_mod
        import importlib
        importlib.reload(dq_mod)
        importlib.reload(dt_mod)
        importlib.reload(recon_mod)
        importlib.reload(router_mod)
        self.router_mod = router_mod

        rule = self.store.insert("data_transform", {
            "规则编号": "TRANS-API-001",
            "源Schema": "json",
            "目标Schema": "json",
            "字段映射": json.dumps({"target_name": "name"}, ensure_ascii=False),
            "转换函数": "passthrough",
            "默认值填充": "使用默认值",
            "异常策略": "跳过",
            "规则版本": "v1",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        })
        self.rule_id = rule["id"]

    def tearDown(self) -> None:
        import app.persistence
        app.persistence.json_store = global_store
        self._tmp.cleanup()

    def test_full_http_flow(self):
        create = self.router_mod.create_package(self.router_mod.CreatePackagePayload(
            rule_id=self.rule_id,
            batch_no="API-BATCH-1",
            source_records=[{"name": "甲"}, {"name": "乙"}],
        ))
        self.assertTrue(create["ok"])
        package_id = create["package"]["id"]

        export = self.router_mod.export_rows(package_id, self.router_mod.ExportPayload())
        self.assertTrue(export["ok"])
        self.assertEqual(export["exported_now"], 2)

        response = self.router_mod.download_package(package_id)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["x-mapping-versions"], "v1")

        async def _collect() -> bytes:
            chunks = []
            async for chunk in response.body_iterator:
                chunks.append(chunk)
            return b"".join(chunks)

        import asyncio
        body = asyncio.run(_collect())
        self.assertTrue(body)
        with zipfile.ZipFile(io.BytesIO(body)) as zf:
            self.assertIn("manifest.json", zf.namelist())

        review = self.router_mod.review_row(package_id, self.router_mod.ReviewPayload(
            source_line=1,
            conclusion="通过",
            reviewer="API质检员",
        ))
        self.assertTrue(review["ok"])

        ledger = self.router_mod.list_ledger(package_id=package_id)
        self.assertEqual(ledger["total"], 1)
        self.assertEqual(ledger["items"][0]["downloaded_mapping_version"], "v1")

    def test_download_undeliverable_raises_http_exception(self):
        from fastapi import HTTPException
        rule = self.store.insert("data_transform", {
            "规则编号": "TRANS-API-002",
            "源Schema": "json",
            "目标Schema": "json",
            "字段映射": json.dumps({"target_name": "name"}, ensure_ascii=False),
            "转换函数": "passthrough",
            "默认值填充": "使用默认值",
            "异常策略": "跳过",
            "规则版本": "  ",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        })
        create = self.router_mod.create_package(self.router_mod.CreatePackagePayload(
            rule_id=rule["id"],
            batch_no="API-BATCH-BLOCKED",
            source_records=[{"name": "甲"}],
        ))
        package_id = create["package"]["id"]
        self.router_mod.export_rows(package_id, self.router_mod.ExportPayload())
        with self.assertRaises(HTTPException) as ctx:
            self.router_mod.download_package(package_id)
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertIn("明确版本", ctx.exception.detail)


if __name__ == "__main__":
    unittest.main(verbosity=2)
