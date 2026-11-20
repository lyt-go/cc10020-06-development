"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import recon, resource, pipeline

# recon 含固定路径（如 /api/transform-recon/ledger），必须先于
# resource 的 /{module}/{entry_id} 通配路由注册，否则会被动态路径抢先匹配。
ROUTERS = [recon, resource, pipeline]
