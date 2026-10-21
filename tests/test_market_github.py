from datetime import date

import pytest
from astrbot_plugin_github_observer.client import APIError
from astrbot_plugin_github_observer.domain import Settings, period, utc_string
from astrbot_plugin_github_observer.service import Service
from conftest import config, github, market
from test_collector import market_data

REPO = {"stargazers_count": 999, "forks_count": 3, "default_branch": "main", "size": 1}


def observer(env, *, enabled=True, metrics=(), market_metrics=(), targets=()):
    _, storage, api, clock, sender = env
    settings = Settings.parse(
        config(
            [
                market(
                    github_enabled=enabled,
                    github_metrics=list(metrics),
                    metrics=list(market_metrics),
                ),
                *targets,
            ]
        )
    )
    return Service(settings, storage, api, sender, clock)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "command"), [("day", "日报"), ("week", "周报"), ("month", "月报")]
)
async def test_linked_reports_and_pushes_keep_market_star_and_include_fork(env, kind, command):
    service = observer(env)
    _, storage, api, clock, sender = env
    anchor = date(2026, 10, 19)
    start, end = period(kind, anchor)
    clock.now = start
    service.subscribe("group", "plugin", (kind,))
    api.put("market", "", market_data(downloads=100, stars=5))
    api.put("get", "/repos/owner/repo", REPO)
    await service.collect()
    clock.now = end
    api.put("market", "", market_data(downloads=120, stars=6))
    api.put("get", "/repos/owner/repo", {**REPO, "forks_count": 4})
    await service.collect()
    assert api.calls == [("market", "", None), ("get", "/repos/owner/repo", None)] * 2
    assert storage.samples("gh:owner/repo", "stars", 0, clock()) == []
    storage.snapshot("gh:owner/repo", end + 1, {"forks": 999})
    clock.now = end + 9 * 3600
    report = service.command("group", [command, "plugin", anchor.isoformat()])
    assert "GitHub 仓库：" not in report
    assert "下载：120｜⬆️20" in report
    assert "Star：6｜⬆️1" in report
    assert "Fork：4｜⬆️1" in report
    assert "999" not in report and "缓存" not in report
    assert report.count("Star：") == 1 and report.count("采样时间：") == 1
    service.schedule()
    await service.deliver()
    sender.assert_awaited_once()
    assert sender.call_args.args[1] == report


@pytest.mark.asyncio
async def test_all_linked_activity_uses_default_branch_and_supports_alerts_and_dynamic(env):
    service = observer(
        env, metrics=("commits", "release", "issues", "prs"), market_metrics=("version",)
    )
    _, storage, api, clock, sender = env
    service.subscribe(
        "group", "plugin", ("day",), ("version", "release", "issues", "prs", "commits")
    )
    api.put("market", "", market_data())
    api.put("get", "/repos/owner/repo", REPO)
    api.put("list", "/repos/owner/repo/issues", [])
    api.put("get", "/repos/owner/repo/branches/main", {"commit": {"sha": "head1"}})
    await service.collect()
    clock.advance(3600)
    api.put("market", "", market_data(downloads=120, stars=6, version="1.1"))
    api.put("get", "/repos/owner/repo", {**REPO, "forks_count": 4})
    api.put("get", "/repos/owner/repo/branches/main", {"commit": {"sha": "head2"}})
    api.put(
        "list",
        "/repos/owner/repo/compare/head1...head2",
        {
            "status": "ahead",
            "commits": [
                {
                    "sha": "head2",
                    "commit": {"message": "new commit"},
                    "html_url": "https://github.com/owner/repo/commit/head2",
                }
            ],
        },
    )
    at = utc_string(clock() - 10)
    api.put(
        "list",
        "/repos/owner/repo/releases",
        [
            {
                "id": 1,
                "published_at": at,
                "tag_name": "v2",
                "html_url": "https://github.com/owner/repo/releases/tag/v2",
            }
        ],
    )
    api.put(
        "list",
        "/repos/owner/repo/issues",
        [
            {
                "number": 1,
                "title": "new issue",
                "created_at": at,
                "html_url": "https://github.com/owner/repo/issues/1",
            },
            {
                "number": 2,
                "title": "new PR",
                "created_at": at,
                "pull_request": {},
                "html_url": "https://github.com/owner/repo/pull/2",
            },
        ],
    )
    api.put("list", "/repos/owner/repo/issues/1/timeline", [])
    api.put("list", "/repos/owner/repo/issues/2/timeline", [])
    await service.collect()
    clock.advance(1)
    report = service.command("group", ["日报", "plugin"])
    for text in (
        "📥 下载：120｜⬆️20",
        "🏆 下载排名：第 1 名｜➖",
        "⭐ Star：6｜⬆️1",
        "🍴 Fork：4｜⬆️1",
        "🏷️ 市场版本：1.1",
        "📦 Release：1 条",
        "🐛 Issue：未关闭 1",
        "🔀 PR：未关闭 1",
        "📝 提交（main）：1 条",
    ):
        assert text in report
    assert "999" not in report
    activity = service.command("group", ["动态", "plugin"])
    for text in ("1.0 → 1.1", "v2", "new issue", "new PR", "new commit"):
        assert text in activity
    assert service.location(service.target("plugin"), "commits") == (
        "gh:owner/repo",
        "commits@main",
    )
    service.schedule()
    await service.deliver()
    assert sender.await_count == 5
    service.schedule()
    await service.deliver()
    assert sender.await_count == 5
    assert storage.samples("gh:owner/repo", "stars", 0, clock()) == []


@pytest.mark.asyncio
async def test_linked_and_standalone_targets_share_market_repo_and_branch_requests(env):
    service = observer(
        env,
        metrics=("commits",),
        targets=(
            market(alias="off"),
            github(metrics=["commits"], branch="main"),
        ),
    )
    _, storage, api, clock, _ = env
    api.put("market", "", market_data())
    api.put("get", "/repos/owner/repo", REPO)
    api.put("get", "/repos/owner/repo/branches/main", {"commit": {"sha": "head"}})
    await service.collect()
    assert api.calls == [
        ("market", "", None),
        ("get", "/repos/owner/repo", None),
        ("get", "/repos/owner/repo/branches/main", None),
    ]
    assert storage.samples("gh:owner/repo", "stars", 0, clock())[-1][1] == 999
    assert "Star：5" in service.command("group", ["日报", "plugin"])
    assert "Star：999" in service.command("group", ["日报", "bot"])
    assert "Fork：3" in service.command("group", ["日报", "plugin"])
    assert "Fork：" not in service.command("group", ["日报", "off"])


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize(
    "repo", [None, "", "https://gitee.com/owner/repo", "https://github.com/owner/repo/issues"]
)
async def test_invalid_repository_does_not_block_market_sampling(env, enabled, repo):
    service = observer(env, enabled=enabled, market_metrics=("version",))
    _, storage, api, clock, _ = env
    data = market_data()
    data["author/plugin"]["repo"] = repo
    api.put("market", "", data)
    await service.collect()
    assert api.calls == [("market", "", None)]
    assert storage.samples("market:author/plugin", "downloads", 0, clock())[-1][1] == 100
    assert storage.samples("market:author/plugin", "stars", 0, clock())[-1][1] == 5
    assert storage.stream("market:author/plugin|version") is not None
    assert bool(service.errors(service.target("plugin"))) is enabled
    data["author/plugin"]["repo"] = "https://github.com/owner/repo"
    api.put("get", "/repos/owner/repo", REPO)
    clock.advance(60)
    await service.collect()
    assert service.errors(service.target("plugin")) == []
    assert sum(call[1] == "/repos/owner/repo" for call in api.calls) == int(enabled)


@pytest.mark.asyncio
async def test_repository_error_does_not_pollute_market_star_and_disappears_when_disabled(env):
    service = observer(env)
    _, storage, api, clock, _ = env
    api.put("market", "", market_data())
    api.put("get", "/repos/owner/repo", APIError("GitHub HTTP 403：权限不足或触发限流。", 403))
    await service.collect()
    report = service.command("group", ["日报", "plugin"])
    assert "Star：5" in report and "下载：100" in report and "403" in report
    assert storage.samples("market:author/plugin", "stars", 0, clock())[-1][1] == 5
    service = observer(env, enabled=False)
    before = len(api.calls)
    clock.advance(60)
    await service.collect()
    assert api.calls[before:] == [("market", "", None)]
    report = service.command("group", ["日报", "plugin"])
    assert "403" not in report and "Fork：" not in report


@pytest.mark.asyncio
async def test_failed_market_refresh_does_not_reuse_stale_repo_for_collection(env):
    service = observer(env, targets=(github(repo="other/repo"),))
    _, _, api, clock, _ = env
    api.put("market", "", market_data())
    api.put("get", "/repos/owner/repo", REPO)
    api.put("get", "/repos/other/repo", REPO)
    await service.collect()
    before = len(api.calls)
    clock.advance(3600)
    api.put("market", "", APIError("市场连接失败"))
    await service.collect()
    assert api.calls[before:] == [("market", "", None), ("get", "/repos/other/repo", None)]
    assert "市场连接失败" in service.command("group", ["日报", "plugin"])


@pytest.mark.asyncio
async def test_repository_change_uses_new_source_and_invalid_link_clears_old_mapping(env):
    service = observer(env)
    _, storage, api, clock, _ = env
    data = market_data()
    api.put("market", "", data)
    api.put("get", "/repos/owner/repo", REPO)
    await service.collect()
    clock.advance(3600)
    data["author/plugin"]["repo"] = "https://github.com/other/new"
    api.put("get", "/repos/other/new", {**REPO, "forks_count": 10})
    await service.collect()
    report = service.command("group", ["日报", "plugin"])
    assert service.location(service.target("plugin"), "forks") == ("gh:other/new", "forks")
    assert "Fork：10｜⏳ 待积累，只有一次采样" in report
    assert "⬆️7" not in report
    assert storage.samples("gh:owner/repo", "forks", 0, clock())[-1][1] == 3
    before = len(api.calls)
    clock.advance(60)
    data["author/plugin"]["repo"] = None
    await service.collect()
    assert api.calls[before:] == [("market", "", None)]
    assert service.location(service.target("plugin"), "forks") == (None, "forks")
    assert "Fork：10" not in service.command("group", ["日报", "plugin"])


@pytest.mark.asyncio
async def test_switch_off_pauses_queued_alerts_but_reports_continue_and_can_resume(env):
    service = observer(env, metrics=("release",))
    _, _, api, clock, sender = env
    created = clock()
    service.subscribe("group", "plugin", ("day",), ("release",))
    api.put("market", "", market_data())
    api.put("get", "/repos/owner/repo", REPO)
    await service.collect()
    clock.advance(3600)
    api.put(
        "list",
        "/repos/owner/repo/releases",
        [{"id": 1, "published_at": utc_string(clock()), "tag_name": "v2"}],
    )
    await service.collect()
    service.schedule()
    service = observer(env, enabled=False, metrics=("release",))
    before = len(api.calls)
    clock.now = created + 86400 + 9 * 3600
    await service.collect()
    assert api.calls[before:] == [("market", "", None)]
    assert "事件提醒已暂停" in service.command("group", ["订阅列表"])
    service.schedule()
    await service.deliver()
    sender.assert_awaited_once()
    assert "· 日报" in sender.call_args.args[1] and "Release：" not in sender.call_args.args[1]
    service = observer(env, metrics=("release",))
    await service.collect()
    service.schedule()
    await service.deliver()
    assert sender.await_count == 2
    assert "Release · 发布" in sender.call_args.args[1]
