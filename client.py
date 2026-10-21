"""Read-only HTTP clients with bounded pagination and explicit rate limits."""

from __future__ import annotations

import asyncio
import time
from email.utils import parsedate_to_datetime

import aiohttp

from .domain import ObserverError, stamp


class APIError(ObserverError):
    def __init__(self, message, status=0):
        super().__init__(message)
        self.status = status


class Client:
    def __init__(self, session, settings, clock=time.time):
        self.session = session
        self.settings = settings
        self.clock = clock
        self.blocked_until = 0

    async def get(self, path, params=None, *, market=False):
        headers = {"User-Agent": "astrbot-plugin-github-observer/1.0"}
        if market:
            url = self.settings.market_url
        else:
            if self.clock() < self.blocked_until:
                raise APIError("GitHub 请求限流，预计恢复：" + stamp(self.blocked_until), 429)
            url = "https://api.github.com" + path
            headers.update(
                {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
            )
            if self.settings.github_token:
                headers["Authorization"] = "Bearer " + self.settings.github_token
        try:
            async with self.session.get(
                url, params=params, headers=headers, allow_redirects=False
            ) as response:
                if not market:
                    reset = response.headers.get("X-RateLimit-Reset", "")
                    if response.headers.get("X-RateLimit-Remaining") == "0" and reset.isdigit():
                        self.blocked_until = max(self.blocked_until, float(reset) + 1)
                    if response.status in {403, 429}:
                        retry = response.headers.get("Retry-After", "")
                        if retry.isdigit():
                            until = self.clock() + max(1, int(retry))
                        elif retry:
                            try:
                                until = parsedate_to_datetime(retry).timestamp()
                            except (TypeError, ValueError, OverflowError):
                                until = self.clock() + 60
                        else:
                            until = self.clock() + 60
                        self.blocked_until = max(self.blocked_until, until)
                if response.status != 200:
                    name = "插件市场" if market else "GitHub"
                    detail = {
                        301: "资源已迁移，请更新配置中的地址或仓库名",
                        302: "接口发生重定向，请检查配置",
                        401: "Token 无效或已过期",
                        403: "权限不足或触发限流",
                        404: "资源不存在，或 Token 无权访问",
                        409: "仓库为空或分支历史不可比较",
                        422: "请求参数无效，请检查仓库与分支",
                        429: "请求限流",
                    }.get(response.status, "接口请求失败")
                    raise APIError(f"{name} HTTP {response.status}：{detail}。", response.status)
                try:
                    return await response.json(content_type=None), response.headers
                except (ValueError, UnicodeError) as exc:
                    raise APIError("接口未返回有效 JSON。") from exc
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise APIError("接口网络连接失败或请求超时。") from exc

    async def listing(self, path, params=None, field=None):
        """Fetch every page, or fail without presenting a truncated result."""
        records, first = [], None
        for page in range(1, self.settings.max_pages + 1):
            data, headers = await self.get(path, {**(params or {}), "per_page": 100, "page": page})
            if first is None:
                first = data
            items = data.get(field) if field and isinstance(data, dict) else data
            if not isinstance(items, list) or any(not isinstance(x, dict) for x in items):
                raise APIError("GitHub 分页接口返回结构不正确。")
            records.extend(items)
            if 'rel="next"' not in headers.get("Link", ""):
                return records, first
        raise APIError("超过 max_pages 分页上限，本次统计未完成；请提高上限后重试。")
