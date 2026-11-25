"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import query, resource

# 查询服务路由含固定前缀 /api/query/...，必须排在通配资源路由之前，
# 避免 /api/{module} 动态路径抢先匹配。
ROUTERS = [query, resource]
