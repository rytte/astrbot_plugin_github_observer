"""AstrBot command and lifecycle adapter for GitHub Observer."""

from __future__ import annotations

import asyncio
import shlex
from contextlib import suppress

import aiohttp
from astrbot.api import AstrBotConfig
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.star.filter.command import GreedyStr

from .client import Client
from .domain import ObserverError, Settings
from .service import Service, chunks
from .storage import Storage


class GithubObserver(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.settings = Settings.parse(dict(config))
        self.storage = None
        self.session = None
        self.service = None
        self.task = None

    async def initialize(self):
        self.storage = Storage(
            StarTools.get_data_dir("astrbot_plugin_github_observer") / "observer.sqlite3"
        )
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.settings.request_timeout), trust_env=True
        )
        self.service = Service(
            self.settings, self.storage, Client(self.session, self.settings), self.send
        )
        self.task = asyncio.create_task(self.run(), name="github-observer")

    async def send(self, session, text):
        return await self.context.send_message(session, MessageChain().message(text))

    async def run(self):
        while True:
            try:
                await self.service.tick()
            except Exception:
                self.logger.exception("GitHub Observer background tick failed.")
            await asyncio.sleep(30)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command(
        "观测站",
        desc="GitHub 观测站：市场下载及排名、Star、Fork、提交、Release、Issue/PR 与日周月报。",
    )
    async def observer(self, event: AstrMessageEvent, arguments: GreedyStr):
        event.should_call_llm(False)
        if not event.is_admin() or event.get_extra("_api_key_allow_admin_role") is False:
            yield (
                event.plain_result("GitHub 观测站仅限 AstrBot 管理员使用。")
                .use_t2i(False)
                .use_markdown(False)
            )
            return
        if self.service is None:
            yield event.plain_result("GitHub 观测站尚未初始化。").use_t2i(False).use_markdown(False)
            return
        try:
            try:
                tokens = shlex.split(arguments)
            except ValueError as exc:
                raise ObserverError("命令引号未正确闭合，请使用 /观测站 帮助 查看格式。") from exc
            if tokens == ["刷新"]:
                await self.service.collect()
                text = self.service.status(event.unified_msg_origin)
            else:
                text = self.service.command(event.unified_msg_origin, tokens)
        except (ObserverError, ValueError) as exc:
            text = str(exc)
        for part in chunks(text):
            yield event.plain_result(part).use_t2i(False).use_markdown(False)

    async def terminate(self):
        if self.task:
            self.task.cancel()
            with suppress(asyncio.CancelledError):
                await self.task
            self.task = None
        if self.session:
            await self.session.close()
            self.session = None
        if self.storage:
            self.storage.close()
            self.storage = None
        self.service = None
