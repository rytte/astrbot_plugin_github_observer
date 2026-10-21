import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from astrbot.api.event import AstrMessageEvent
from astrbot.api.message_components import Plain
from astrbot.core.platform.astrbot_message import AstrBotMessage, MessageMember
from astrbot.core.platform.message_type import MessageType
from astrbot.core.platform.platform_metadata import PlatformMetadata
from astrbot.core.star.filter.command import CommandFilter, GreedyStr
from astrbot.core.star.filter.permission import PermissionTypeFilter
from astrbot.core.star.star_handler import star_handlers_registry
from astrbot_plugin_github_observer.main import GithubObserver
from conftest import config, github


def message_event(role="admin", session="group", api_admin=None):
    message = AstrBotMessage()
    message.type = MessageType.GROUP_MESSAGE
    message.message = [Plain("/观测站 状态")]
    message.message_str = "/观测站 状态"
    message.sender = MessageMember(user_id="user", nickname="Tester")
    message.self_id = "bot"
    event = AstrMessageEvent(
        message.message_str, message, PlatformMetadata("aiocqhttp", "test", "qq"), session
    )
    event.role = role
    event.is_at_or_wake_command = True
    if api_admin is not None:
        event.set_extra("_api_key_allow_admin_role", api_admin)
    return event


@pytest.mark.asyncio
async def test_real_astrbot_command_registration_and_greedy_arguments():
    handler = next(
        item
        for item in star_handlers_registry
        if item.handler_module_path == "astrbot_plugin_github_observer.main"
    )
    command = next(f for f in handler.event_filters if isinstance(f, CommandFilter))
    assert command.command_name == "观测站"
    assert command.handler_params["arguments"] is GreedyStr
    permission = next(f for f in handler.event_filters if isinstance(f, PermissionTypeFilter))
    assert permission.filter(message_event("member"), {}) is False
    assert permission.filter(message_event("admin"), {}) is True


@pytest.mark.asyncio
@pytest.mark.parametrize(("role", "api_admin"), [("member", None), ("admin", False)])
async def test_command_checks_permissions_again_before_access(role, api_admin):
    plugin = GithubObserver(SimpleNamespace(), config([github()]))
    plugin.service = SimpleNamespace(
        command=lambda *_: pytest.fail("Unauthorized command reached service")
    )
    event = message_event(role, api_admin=api_admin)
    results = [result async for result in plugin.observer(event, "列表")]
    assert "仅限" in results[0].get_plain_text()
    assert event.call_llm is False


@pytest.mark.asyncio
async def test_real_message_results_and_session_scoped_subscription(env):
    service, storage, _, _, _ = env
    plugin = GithubObserver(SimpleNamespace(), config([github()]))
    plugin.service = service
    event = message_event(session="group-1")
    results = [result async for result in plugin.observer(event, "订阅 bot 日报")]
    assert "已订阅" in results[0].get_plain_text()
    assert results[0].use_t2i_ is False
    assert results[0].use_markdown_ is False
    assert storage.subscriptions()[0]["session"] == event.unified_msg_origin


@pytest.mark.asyncio
async def test_actual_lifecycle_creates_and_cleans_session_and_worker(tmp_path, monkeypatch):
    from astrbot_plugin_github_observer import main

    monkeypatch.setattr(main.StarTools, "get_data_dir", lambda *_: tmp_path)
    monkeypatch.setattr(main.Service, "tick", AsyncMock())
    plugin = GithubObserver(SimpleNamespace(send_message=AsyncMock(return_value=True)), config())
    await plugin.initialize()
    session, task = plugin.session, plugin.task
    try:
        await asyncio.sleep(0)
        assert (tmp_path / "observer.sqlite3").exists()
        assert not task.done()
        await plugin.send("qq:GroupMessage:group", "hello")
        sent = plugin.context.send_message.call_args[0]
        assert sent[0] == "qq:GroupMessage:group"
        assert sent[1].get_plain_text() == "hello"
    finally:
        await plugin.terminate()
    assert session.closed
    assert task.cancelled()
    assert plugin.service is None
    await plugin.terminate()


@pytest.mark.asyncio
async def test_unbalanced_quotes_return_readable_error(env):
    plugin = GithubObserver(SimpleNamespace(), config())
    plugin.service = env[0]
    results = [r async for r in plugin.observer(message_event(), '订阅 "unterminated')]
    assert results
