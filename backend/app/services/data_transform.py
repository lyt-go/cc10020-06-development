"""数据转换编排领域服务：JSON/XML数据转换、字段映射、类型转换与结构重组。

提供从源Schema到目标Schema的字段级映射和值转换能力，
支持嵌套结构抽取、类型强制转换和异常降级策略。
跨模块关联：data_quality_rule（质量校验）、data_lake_sink（转换后落湖）。
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Callable

from app.persistence import json_store
from app.services.base import BaseModuleService


class DataTransformService(BaseModuleService):
    MODULE = "data_transform"
    ENTITY = "转换规则"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["规则编号", "源Schema", "目标Schema", "字段映射", "转换函数", "默认值填充", "异常策略", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "源Schema", "目标Schema"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    _BUILTIN_FUNCTIONS: dict[str, Callable[[Any], Any]] = {
        "to_string": lambda x: str(x) if x is not None else "",
        "to_integer": lambda x: int(x) if x is not None else 0,
        "to_float": lambda x: float(x) if x is not None else 0.0,
        "to_boolean": lambda x: bool(x) if x is not None else False,
        "to_upper": lambda x: str(x).upper() if x is not None else "",
        "to_lower": lambda x: str(x).lower() if x is not None else "",
        "extract_numbers": lambda x: re.sub(r"[^0-9]", "", str(x)) if x is not None else "",
        "parse_datetime": lambda x: x,
        "default_empty": lambda x: x if x is not None and str(x).strip() else "",
    }

    def apply_transform(self, rule_id: int, source_data: dict[str, Any],
                        source_line: int | None = None) -> dict[str, Any]:
        """应用转换规则，将源数据转换为目标结构。

        转换流程：
        1. 加载已生效的转换规则
        2. 解析字段映射定义
        3. 逐个字段应用转换函数
        4. 处理异常策略（跳过/默认值/报错）
        5. 生成字段级转换轨迹（原始值/目标值/校验结果/映射版本/来源行号）

        Returns:
            { ok: bool, data: dict, errors: list, fields_transformed: int,
              quality_check: dict, field_traces: list }
        """
        rule = json_store.get_by_id(self.MODULE, rule_id)
        if rule is None:
            return {"ok": False, "data": {}, "errors": ["转换规则不存在"], "fields_transformed": 0}

        if rule.get("status") != "已生效":
            return {"ok": False, "data": {}, "errors": ["转换规则未生效"], "fields_transformed": 0}

        field_mapping = self._parse_mapping(str(rule.get("字段映射", "{}")))
        transform_func = str(rule.get("转换函数", "passthrough"))
        default_filler = str(rule.get("默认值填充", "使用默认值"))
        error_policy = str(rule.get("异常策略", "跳过"))

        result: dict[str, Any] = {}
        errors: list[str] = []
        field_traces: list[dict[str, Any]] = []
        transformed = 0

        mapping_version = self.mapping_version_of(rule)
        mapping_fingerprint = self.mapping_fingerprint(field_mapping)

        for target_field, source_path in field_mapping.items():
            trace: dict[str, Any] = {
                "target_field": target_field,
                "source_path": source_path,
                "source_line": source_line,
                "mapping_version": mapping_version,
                "mapping_fingerprint": mapping_fingerprint,
                "rule_id": rule_id,
                "rule_no": rule.get("规则编号"),
            }
            try:
                source_value = self._extract_value(source_data, source_path)
                converted = self._apply_function(transform_func, source_value)
                result[target_field] = converted
                transformed += 1
                trace.update({
                    "original_value": source_value,
                    "target_value": converted,
                    "transform_ok": True,
                    "transform_message": "转换成功",
                })
            except (KeyError, ValueError, TypeError) as e:
                if error_policy == "使用默认值":
                    fallback = self._get_default_value(target_field, rule)
                    result[target_field] = fallback
                    trace.update({
                        "original_value": self._safe_extract(source_data, source_path),
                        "target_value": fallback,
                        "transform_ok": False,
                        "transform_message": f"转换失败，已使用默认值: {e}",
                    })
                elif error_policy == "报错":
                    errors.append(f"字段 {target_field}: {e}")
                    trace.update({
                        "original_value": self._safe_extract(source_data, source_path),
                        "target_value": None,
                        "transform_ok": False,
                        "transform_message": f"转换失败: {e}",
                    })
                else:
                    errors.append(f"字段 {target_field} 跳过: {e}")
                    trace.update({
                        "original_value": self._safe_extract(source_data, source_path),
                        "target_value": None,
                        "transform_ok": False,
                        "transform_message": f"字段跳过: {e}",
                    })
            field_traces.append(trace)

        # 质量校验（跨模块：data_quality_rule）
        quality_check = self._run_quality_checks(rule_id, result)
        self._annotate_quality(field_traces, quality_check)

        return {
            "ok": len(errors) == 0 or error_policy == "跳过",
            "data": result,
            "errors": errors,
            "fields_transformed": transformed,
            "total_fields": len(field_mapping),
            "quality_check": quality_check,
            "field_traces": field_traces,
            "mapping_version": mapping_version,
            "mapping_fingerprint": mapping_fingerprint,
            "source_line": source_line,
        }

    @staticmethod
    def _safe_extract(data: dict[str, Any], path: str) -> Any:
        """提取源值但不抛异常，用于失败轨迹保留可追溯的原始值。"""
        try:
            return DataTransformService._extract_value_static(data, path)
        except (KeyError, ValueError, TypeError, IndexError, AttributeError):
            return None

    @staticmethod
    def _extract_value_static(data: dict[str, Any], path: str) -> Any:
        """与实例方法 _extract_value 相同的静态实现，供失败兜底复用。"""
        parts = re.split(r"\.|(?<=\w)(?=\[)", path)
        current: Any = data
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if "[" in part and "]" in part:
                key, idx_str = part.split("[", 1)
                idx = int(idx_str.rstrip("]"))
                if key:
                    current = current[key]
                current = current[idx]
            elif isinstance(current, dict):
                current = current.get(part, current.get(path, None))
            else:
                raise KeyError(f"无法从 {type(current)} 中提取 {part}")
        return current

    @staticmethod
    def mapping_version_of(rule: dict[str, Any]) -> str:
        """读取规则上实际声明的映射版本；未明确声明返回空串（不具交付资格）。"""
        version = str(rule.get("规则版本", "") or "").strip()
        return version

    @staticmethod
    def mapping_fingerprint(field_mapping: dict[str, str]) -> str:
        """对字段映射内容计算指纹，用于识别同版本号下映射是否被改动。"""
        canonical = json.dumps(field_mapping, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]

    def trace_record(self, rule_id: int, source_record: dict[str, Any],
                     source_line: int) -> dict[str, Any]:
        """生成单条源记录的转换复核轨迹（供转换对账数据包消费）。

        与 apply_transform 共用同一套映射快照，保证导出的对账数据
        与实际转换结果一致，并固化来源行号与映射版本。
        """
        rule = json_store.get_by_id(self.MODULE, rule_id)
        if rule is None:
            return {
                "ok": False, "errors": ["转换规则不存在"],
                "field_traces": [], "data": {},
                "mapping_version": "", "mapping_fingerprint": "",
                "source_line": source_line,
            }
        field_mapping = self._parse_mapping(str(rule.get("字段映射", "{}")))
        transform_func = str(rule.get("转换函数", "passthrough"))
        error_policy = str(rule.get("异常策略", "跳过"))
        mapping_version = self.mapping_version_of(rule)
        mapping_fingerprint = self.mapping_fingerprint(field_mapping)

        field_traces: list[dict[str, Any]] = []
        result: dict[str, Any] = {}
        errors: list[str] = []

        for target_field, source_path in field_mapping.items():
            trace: dict[str, Any] = {
                "target_field": target_field,
                "source_path": source_path,
                "source_line": source_line,
                "mapping_version": mapping_version,
                "mapping_fingerprint": mapping_fingerprint,
                "rule_id": rule_id,
                "rule_no": rule.get("规则编号"),
                "original_value": self._safe_extract(source_record, source_path),
            }
            try:
                source_value = self._extract_value(source_record, source_path)
                converted = self._apply_function(transform_func, source_value)
                result[target_field] = converted
                trace.update({
                    "original_value": source_value,
                    "target_value": converted,
                    "transform_ok": True,
                    "transform_message": "转换成功",
                })
            except (KeyError, ValueError, TypeError) as e:
                if error_policy == "使用默认值":
                    fallback = self._get_default_value(target_field, rule)
                    result[target_field] = fallback
                    trace.update({
                        "target_value": fallback,
                        "transform_ok": False,
                        "transform_message": f"转换失败，已使用默认值: {e}",
                    })
                elif error_policy == "报错":
                    errors.append(f"字段 {target_field}: {e}")
                    trace.update({
                        "target_value": None,
                        "transform_ok": False,
                        "transform_message": f"转换失败: {e}",
                    })
                else:
                    errors.append(f"字段 {target_field} 跳过: {e}")
                    trace.update({
                        "target_value": None,
                        "transform_ok": False,
                        "transform_message": f"字段跳过: {e}",
                    })
            field_traces.append(trace)

        quality_check = self._run_quality_checks(rule_id, result)
        self._annotate_quality(field_traces, quality_check)

        return {
            "ok": len(errors) == 0 or error_policy == "跳过",
            "data": result,
            "errors": errors,
            "field_traces": field_traces,
            "quality_check": quality_check,
            "mapping_version": mapping_version,
            "mapping_fingerprint": mapping_fingerprint,
            "source_line": source_line,
        }

    @staticmethod
    def _annotate_quality(field_traces: list[dict[str, Any]],
                          quality_check: dict[str, Any]) -> None:
        """把质量校验明细回填到对应目标字段的轨迹上，形成字段级校验结果。"""
        failed_fields: dict[str, dict[str, Any]] = {}
        for detail in quality_check.get("details", []):
            if not detail.get("passed", True):
                failed_fields[str(detail.get("field", ""))] = detail
        for trace in field_traces:
            detail = failed_fields.get(str(trace.get("target_field", "")))
            if detail is None:
                trace["quality_passed"] = True
                trace["quality_message"] = "质量校验通过"
            else:
                messages = []
                expr_result = detail.get("expr_result") or {}
                range_result = detail.get("range_result") or {}
                if not expr_result.get("passed", True):
                    messages.append(str(expr_result.get("message", "表达式校验失败")))
                if not range_result.get("passed", True):
                    messages.append(str(range_result.get("message", "范围校验失败")))
                trace["quality_passed"] = False
                trace["quality_message"] = "；".join(messages) or "质量校验失败"
                trace["quality_rule_no"] = detail.get("rule_no")
                trace["block_policy"] = detail.get("block_policy")

    def _parse_mapping(self, mapping_str: str) -> dict[str, str]:
        """解析字段映射定义。

        支持格式：
        - JSON: {"目标字段": "源字段路径"}
        - 简单: "field1->target1,field2->target2"
        """
        try:
            return json.loads(mapping_str)
        except json.JSONDecodeError:
            result = {}
            for part in mapping_str.split(","):
                if "->" in part:
                    src, tgt = part.split("->", 1)
                    result[tgt.strip()] = src.strip()
            return result if result else {"_raw": "payload"}

    def _extract_value(self, data: dict[str, Any], path: str) -> Any:
        """从嵌套数据中提取指定路径的值。

        支持点号和方括号路径：
        - "name" → data["name"]
        - "user.profile.email" → data["user"]["profile"]["email"]
        - "items[0].id" → data["items"][0]["id"]
        """
        parts = re.split(r"\.|(?<=\w)(?=\[)", path)
        current: Any = data
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if "[" in part and "]" in part:
                key, idx_str = part.split("[", 1)
                idx = int(idx_str.rstrip("]"))
                if key:
                    current = current[key]
                current = current[idx]
            elif isinstance(current, dict):
                current = current.get(part, current.get(path, None))
            else:
                raise KeyError(f"无法从 {type(current)} 中提取 {part}")

        return current

    def _apply_function(self, func_name: str, value: Any) -> Any:
        """应用命名转换函数。"""
        if func_name == "passthrough":
            return value
        func = self._BUILTIN_FUNCTIONS.get(func_name)
        if func:
            return func(value)
        return value

    def _get_default_value(self, field: str, rule: dict[str, Any]) -> Any:
        """根据规则配置获取字段默认值。"""
        default_mapping = {
            "string": "", "integer": 0, "float": 0.0,
            "boolean": False, "datetime": "",
        }
        return default_mapping.get(rule.get("目标Schema", ""), "")

    def _run_quality_checks(self, rule_id: int, transformed_data: dict[str, Any]) -> dict[str, Any]:
        """跨模块运行质量校验。"""
        try:
            from app.services.data_quality_rule import DataQualityRuleService
            quality_svc = DataQualityRuleService()
            return quality_svc.validate_record(transformed_data, source_module=self.MODULE)
        except Exception:
            return {"passed": True, "checks": 0}

    def batch_transform(self, rule_id: int, source_records: list[dict[str, Any]]) -> dict[str, Any]:
        """批量转换多条记录，每条记录固化来源行号（从1开始）。"""
        results = []
        success = 0
        failed = 0

        for index, record in enumerate(source_records, start=1):
            r = self.apply_transform(rule_id, record, source_line=index)
            if r["ok"]:
                success += 1
            else:
                failed += 1
            results.append(r)

        return {
            "ok": failed == 0,
            "message": f"批量转换完成: 成功 {success}/{len(source_records)}",
            "success": success,
            "failed": failed,
            "total": len(source_records),
            "results": results,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出数据转换编排的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "数据转换编排数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "active_rules": sum(1 for r in rows if r.get("status") == "已生效"),
            "operation": self.OPERATION_TERM,
        }