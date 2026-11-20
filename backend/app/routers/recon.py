"""转换对账 API：对账数据包导出、版本化文件下载与质检台账串联。

路由设计遵循项目约定：路由层只做参数解析与 HTTP 适配，
所有业务判断（交付闸门、版本核对、台账写入）都在领域服务内完成。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.services.transform_recon import TransformReconService

router = APIRouter(prefix="/api/transform-recon", tags=["转换对账"])


class CreatePackagePayload(BaseModel):
    rule_id: int
    batch_no: str
    source_records: list[dict[str, Any]] = Field(default_factory=list)


class ExportPayload(BaseModel):
    limit: int | None = None


class ReviewPayload(BaseModel):
    source_line: int
    conclusion: str
    reviewer: str = "质检同事"
    remark: str = ""


def _service() -> TransformReconService:
    return TransformReconService()


@router.post("/packages")
def create_package(payload: CreatePackagePayload) -> dict[str, Any]:
    """建立转换对账数据包（同批次幂等，并发请求拿到同一个包）。"""
    result = _service().create_package(
        rule_id=payload.rule_id,
        batch_no=payload.batch_no,
        source_records=payload.source_records,
    )
    if not result["ok"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.get("/packages")
def list_packages(batch_no: str | None = None) -> dict[str, Any]:
    """查询转换对账数据包列表及其交付状态。"""
    items = _service().list_packages(batch_no=batch_no)
    return {"ok": True, "total": len(items), "items": items}


@router.get("/packages/{package_id}")
def get_package(package_id: int) -> dict[str, Any]:
    package = _service().get_package(package_id)
    if package is None:
        raise HTTPException(status_code=404, detail=f"对账数据包 {package_id} 不存在")
    return {"ok": True, "package": package}


@router.post("/packages/{package_id}/export")
def export_rows(package_id: int, payload: ExportPayload) -> dict[str, Any]:
    """导出（或断点续跑）转换复核数据，自动识别后来取得行采用的映射。"""
    result = _service().export_rows(package_id, limit=payload.limit)
    if not result["ok"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.get("/packages/{package_id}/rows")
def list_rows(package_id: int) -> dict[str, Any]:
    """查看包内逐行转换复核数据（含原始值、目标值、校验结果）。"""
    result = _service().list_rows(package_id)
    if not result["ok"]:
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@router.get("/packages/{package_id}/failed-fields")
def failed_fields(package_id: int) -> dict[str, Any]:
    """导出失败字段清单，失败字段关联到收到的源记录。"""
    result = _service().get_failed_fields(package_id)
    if not result["ok"]:
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@router.get("/packages/{package_id}/download")
def download_package(package_id: int) -> StreamingResponse:
    """下载版本绑定的转换对账数据文件；非明确版本的文件不可下载。"""
    result = _service().download_package(package_id)
    if not result["ok"]:
        raise HTTPException(status_code=409, detail=result["message"])

    def iter_content():
        yield result["content"]

    return StreamingResponse(
        iter_content(),
        media_type=result["media_type"],
        headers={
            "Content-Disposition": f'attachment; filename="{result["filename"]}"',
            "X-Recon-File-Id": result["file_id"],
            "X-Mapping-Versions": ",".join(result["mapping_versions"]),
        },
    )


@router.post("/packages/{package_id}/reviews")
def review_row(package_id: int, payload: ReviewPayload) -> dict[str, Any]:
    """登记质检复查结论，同步到质检台账并绑定已下载文件版本。"""
    result = _service().review_row(
        package_id=package_id,
        source_line=payload.source_line,
        conclusion=payload.conclusion,
        reviewer=payload.reviewer,
        remark=payload.remark,
    )
    if not result["ok"]:
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.get("/ledger")
def list_ledger(package_id: int | None = None) -> dict[str, Any]:
    """查询质检台账，结论可与下载记录、映射版本相互核对。"""
    entries = _service().list_ledger(package_id=package_id)
    return {"ok": True, "total": len(entries), "items": entries}
