from datetime import date, datetime
from unittest.mock import AsyncMock

import pytest
from astrbot_plugin_github_observer.domain import (
    BEIJING,
    ObserverError,
    Settings,
    instant,
    period,
    stamp,
)
from astrbot_plugin_github_observer.service import Service, chunks
from astrbot_plugin_github_observer.storage import Storage
from conftest import Clock, FakeAPI, config, github, market
from test_collector import market_data


def event(uid, at, action="published", title="Release"):
    return {
        "uid": uid,
        "at": at,
        "action": action,
        "title": title,
        "url": "https://github.com/owner/repo",
    }


def add_events(storage, metric, start, checked, events):
    storage.commit_stream(
        "gh:owner/repo|" + metric, "gh:owner/repo", metric, start, checked, None, events
    )


def test_subscription_rejects_alert_for_unwatched_metric(env):
    _, storage, api, clock, sender = env
    service = Service(Settings.parse(config([github()])), storage, api, sender, clock)
    with pytest.raises(ObserverError, match="提醒只能"):
        service.command("group-1", ["订阅", "bot", "日报", "--提醒", "release"])
    assert storage.subscriptions() == []


def test_conversation_isolation_and_overwrite(env):
    service, storage, _, _, _ = env
    service.command("group-1", ["订阅", "bot", "日报,月报"])
    service.command("group-2", ["订阅", "bot", "周报", "--提醒", "release"])
    first = storage.subscriptions("group-1")[0]
    second = storage.subscriptions("group-2")[0]
    assert first["alerts"] == []
    assert second["alerts"] == ["release"]
    assert "metrics" not in first and "metrics" not in second
    service.command("group-1", ["订阅", "bot", "周报"])
    assert len(storage.subscriptions()) == 2
    assert storage.subscriptions("group-1")[0]["id"] != first["id"]
    service.command("group-1", ["退订", "bot"])
    assert len(storage.subscriptions("group-2")) == 1


def test_invalid_subscription_option_does_not_overwrite_existing_subscription(env):
    service, storage, _, _, _ = env
    service.command("group", ["订阅", "bot", "周报"])
    before = storage.subscriptions()
    with pytest.raises(ObserverError, match="订阅选项仅支持 --提醒"):
        service.command("group", ["订阅", "bot", "日报", "--未知", "stars"])
    assert storage.subscriptions() == before


def test_report_only_subscription_has_no_idle_database_writes(env):
    service, storage, _, clock, _ = env
    service.subscribe("group", "bot", ("day", "week", "month"))
    changes = storage.db.total_changes
    for _ in range(120):
        clock.advance(30)
        service.schedule()
    assert storage.db.total_changes == changes


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "command"), [("day", "日报"), ("week", "周报"), ("month", "月报")]
)
@pytest.mark.parametrize(
    ("factory", "alias", "source", "extras", "metric", "before", "after", "detail"),
    [
        (
            github,
            "bot",
            "gh:owner/repo",
            ["release"],
            "release",
            {"stars": 10, "forks": 3},
            {"stars": 15, "forks": 6},
            "Release：1 条",
        ),
        (
            market,
            "plugin",
            "market:author/plugin",
            ["version"],
            "version",
            {"downloads": 100, "download_rank": 10, "stars": 3, "version": "1.0"},
            {"downloads": 120, "download_rank": 8, "stars": 6, "version": "1.1"},
            "市场版本：1.1｜观测到变更 1 次",
        ),
    ],
    ids=["github", "market"],
)
async def test_configuration_changes_flow_to_collection_reports_and_activity(
    env, kind, command, factory, alias, source, extras, metric, before, after, detail
):
    _, storage, api, clock, sender = env
    anchor = date(2026, 10, 19)
    start, end = period(kind, anchor)
    clock.now = start
    service = Service(Settings.parse(config([factory()])), storage, api, sender, clock)
    service.subscribe("group", alias, (kind,))
    subscription_id = storage.subscriptions()[0]["id"]
    storage.snapshot(source, start, before)
    storage.snapshot(source, end, after)
    storage.commit_stream(
        source + "|" + metric,
        source,
        metric,
        start,
        end,
        None,
        [event("configured-event", start + 3600, title="configured event")],
    )
    clock.now = end + 9 * 3600

    for enabled in (extras, []):
        service = Service(
            Settings.parse(config([factory(metrics=enabled)])), storage, api, sender, clock
        )
        target = service.target(alias)
        service.collector.market = AsyncMock()
        service.collector.repository = AsyncMock()
        await service.collect()
        if factory is market:
            service.collector.market.assert_awaited_once_with(
                [target], {source: set(target.metrics)}
            )
            service.collector.repository.assert_not_awaited()
        else:
            service.collector.repository.assert_awaited_once_with(
                target.target, set(target.metrics), set()
            )
            service.collector.market.assert_not_awaited()
        report = service.command("group", [command, alias, anchor.isoformat()])
        assert report == service.command("unsubscribed-group", [command, alias, anchor.isoformat()])
        assert (detail in report) is bool(enabled)
        if factory is market:
            assert "Star：6｜⬆️3" in report
        else:
            assert "Fork：6｜⬆️3" in report
        activity = service.command("group", ["动态", alias])
        assert ("configured event" in activity) is bool(enabled)
        listing = service.command("group", ["订阅列表"])
        assert "指标：" + ",".join(target.metrics) in listing
        if not enabled:
            _, end = period(kind, datetime.fromtimestamp(end, BEIJING).date())
            clock.now = end + 9 * 3600
        service.schedule()
        pending = storage.pending(clock())
        assert len(pending) == 1
        assert (detail.split("｜")[0] in pending[0]["text"]) is bool(enabled)
        if factory is github:
            assert "Fork：6" in pending[0]["text"]
        await service.deliver()
        assert storage.subscriptions()[0]["id"] == subscription_id
        assert "metrics" not in storage.subscriptions()[0]


@pytest.mark.asyncio
async def test_disabled_alert_pauses_events_without_blocking_reports_then_resumes(env):
    service, storage, api, clock, sender = env
    service.subscribe("group", "bot", ("day",), ("release", "issues"))
    sub = storage.subscriptions()[0]
    created = clock()
    clock.advance(200)
    add_events(
        storage,
        "release",
        created,
        clock(),
        [event(str(index), created + index + 1) for index in range(25)],
    )
    service.schedule()
    assert len(storage.pending(clock())) == 20  # Delivery batches are limited to 20.
    progress = storage.state(f"alert-progress:{sub['id']}")

    service = Service(
        Settings.parse(config([github(metrics=["issues"])])), storage, api, sender, clock
    )
    clock.now = created + 86400 + 9 * 3600
    add_events(storage, "issues", created, clock(), [event("new-issue", created + 300, "opened")])
    service.schedule()
    assert storage.state(f"alert-progress:{sub['id']}") == progress
    for action in ("状态", "订阅列表"):
        text = service.command("group", [action])
        assert "事件提醒已暂停" in text and "Release" in text and "周期报告继续" in text
    await service.deliver()
    sender.assert_awaited_once()
    assert "· 日报" in sender.call_args.args[1]
    assert "Release：" not in sender.call_args.args[1]
    assert storage.db.execute("SELECT count(*) FROM outbox WHERE sent IS NULL").fetchone()[0] == 25

    service = Service(
        Settings.parse(config([github(metrics=["release", "issues"])])), storage, api, sender, clock
    )
    assert "事件提醒已暂停" not in service.command("group", ["订阅列表"])
    service.schedule()
    await service.deliver()
    await service.deliver()
    service.schedule()
    await service.deliver()
    assert sender.await_count == 27
    assert storage.pending(clock()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("item", "reference", "source", "metric"),
    [
        (github(alias="", repo="Owner/Repo"), "OWNER/Repo", "gh:owner/repo", "stars"),
        (
            market(alias="", plugin_id="Author/Plugin"),
            "AUTHOR/Plugin",
            "market:author/plugin",
            "downloads",
        ),
    ],
    ids=["github", "market"],
)
async def test_unaliased_target_query_subscription_delivery_and_unsubscribe(
    env, item, reference, source, metric
):
    _, storage, api, clock, sender = env
    service = Service(Settings.parse(config([item])), storage, api, sender, clock)
    storage.snapshot(source, clock(), {metric: 10})
    clock.advance(3600)
    storage.snapshot(source, clock(), {metric: 15})
    assert "⬆️5" in service.command("group", ["日报", reference])
    service.command("group", ["订阅", reference, "日报"])
    sub = storage.subscriptions("group")[0]
    assert sub["alias"] == reference.lower()
    assert "metrics" not in sub
    assert "指标：" + ",".join(service.target(reference).required) in service.command(
        "group", ["订阅列表"]
    )
    clock.advance(86400 + 8 * 3600)
    service.schedule()
    assert len(storage.pending(clock())) == 1
    await service.deliver()
    sender.assert_awaited_once()
    assert reference.lower() in sender.call_args.args[1]
    clock.advance(86400)
    service.schedule()
    assert len(storage.pending(clock())) == 1
    assert "已取消" in service.command("group", ["退订", reference])
    assert storage.subscriptions() == []
    assert storage.pending(clock()) == []


def test_unaliased_removed_target_can_still_be_unsubscribed(env):
    _, storage, api, clock, sender = env
    service = Service(Settings.parse(config([github(alias="")])), storage, api, sender, clock)
    service.command("group", ["订阅", "OWNER/Repo", "日报"])
    service = Service(Settings.parse(config()), storage, api, sender, clock)
    assert "目标已移除" in service.command("group", ["订阅列表"])
    assert "已取消" in service.command("group", ["退订", "Owner/REPO"])
    assert storage.subscriptions() == []


@pytest.mark.parametrize(
    "tokens",
    [
        ["订阅", "bot", "每日"],
        ["订阅", "bot", "日报", "--未知", "stars"],
        ["订阅", "bot", "日报", "--提醒", "release", "--提醒", "issues"],
        ["日报", "bot", "2026-99-01"],
        ["日报", "missing"],
        ["状态", "extra"],
        ["动态", "bot", "100"],
        ["订阅", "plugin", "日报", "--其他", "prs"],
    ],
)
def test_invalid_command_never_falls_back(env, tokens):
    with pytest.raises(ObserverError):
        env[0].command("group", tokens)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["day", "week", "month"])
async def test_star_and_fork_net_changes_and_boundary_exclusion(env, kind):
    _, storage, api, clock, sender = env
    service = Service(Settings.parse(config([github()])), storage, api, sender, clock)
    anchor = date(2026, 10, 19)
    start, end = period(kind, anchor)
    clock.now = start
    service.subscribe("group", "bot", (kind,))
    storage.snapshot("gh:owner/repo", start, {"stars": 100, "forks": 20})
    storage.snapshot("gh:owner/repo", end, {"stars": 98, "forks": 23})
    storage.snapshot("gh:owner/repo", end + 1, {"stars": 999, "forks": 999})
    clock.now = end + 9 * 3600
    text = service.report((service.target("bot"),), kind, anchor)
    assert "Star：98｜⬇️2" in text
    assert "Fork：23｜⬆️3" in text
    assert "999" not in text
    assert "本周期记录不完整" not in text
    assert text.count("采样时间：") == 1
    service.schedule()
    await service.deliver()
    sender.assert_awaited_once()
    assert sender.call_args.args[1] == text


@pytest.mark.parametrize(
    ("kind", "command"), [("day", "日报"), ("week", "周报"), ("month", "月报")]
)
@pytest.mark.parametrize("sample_offset", [0, 2])
def test_combined_reports_share_context_and_keep_each_targets_metrics(
    env, kind, command, sample_offset
):
    _, storage, api, clock, sender = env
    service = Service(
        Settings.parse(
            config(
                [github("one", "owner/one", ["commits"]), github("two", "owner/two", ["commits"])]
            )
        ),
        storage,
        api,
        sender,
        clock,
    )
    start, end = period(kind, date(2026, 10, 19))
    for alias, amount, offset in (("one", 10, 0), ("two", 100, sample_offset)):
        source = "gh:owner/" + alias
        storage.put_state(source, {"default_branch": "main"})
        storage.snapshot(source, start + 600, {"stars": amount, "forks": amount})
        storage.snapshot(source, start + 3600 + offset, {"stars": amount + 5, "forks": amount + 1})
        storage.commit_stream(
            source + "|commits@main",
            source,
            "commits@main",
            start + 600,
            start + 3600 + offset,
            "head",
            [],
        )
    clock.now = start + 3601 + sample_offset
    text = service.command("group", [command])
    header, first, second = text.split("\n\n")
    assert text.count(f"GitHub 观测站 · {command}") == 1
    assert text.count("｜北京时间｜统计中") == 1
    assert text.count("采样时间：") == 1
    assert f"{stamp(start)[:10]} ～ {stamp(end - 1)[:10]}｜北京时间｜统计中" in header
    expected_time = stamp(start + 3600)
    if sample_offset:
        expected_time += " ～ " + stamp(start + 3600 + sample_offset)
    assert "🕒 采样时间：" + expected_time in header
    assert text.count("本周期记录不完整，统计仅覆盖已记录时段") == 1
    assert "⚠️ 注意：本周期记录不完整，统计仅覆盖已记录时段" in header
    assert text.count("提交按首次观测到进入所选分支的时间归入周期") == 1
    assert first.startswith("one｜owner/one") and second.startswith("two｜owner/two")
    assert "Star：15｜⬆️5" in first and "Fork：11｜⬆️1" in first
    assert "Star：105｜⬆️5" in second and "Fork：101｜⬆️1" in second
    assert "提交（main）：0 条" in first and "提交（main）：0 条" in second
    assert api.calls == []


def test_combined_report_keeps_missing_data_and_specific_warnings_with_their_target(env):
    _, storage, api, clock, sender = env
    service = Service(
        Settings.parse(config([github("ready", "owner/ready"), github("empty", "owner/empty")])),
        storage,
        api,
        sender,
        clock,
    )
    clock.advance(3600)
    storage.snapshot("gh:owner/ready", clock(), {"stars": 10, "forks": 1})
    storage.health("gh:owner/empty", clock(), "GitHub HTTP 403：权限不足或触发限流。")
    text = service.command("group", ["日报"])
    header, first, second = text.split("\n\n")
    assert "采样时间：" + stamp(clock()) in header
    assert "注意：" not in header and "403" not in header
    assert "本周期记录不完整" in first and "403" not in first
    assert "本周期记录不完整" not in second and "403" in second
    assert "Star：本周期无可用采样。" in second
    assert "Fork：本周期无可用采样。" in second


def test_partial_first_day_explains_incomplete_statistics(env):
    service, storage, _, clock, _ = env
    clock.advance(11 * 3600)
    storage.snapshot("market:author/plugin", clock(), {"downloads": 212})
    clock.advance(3600)
    storage.snapshot("market:author/plugin", clock(), {"downloads": 213})
    text = service.report((service.target("plugin"),), "day", date(2026, 10, 19))
    assert "下载：213｜⬆️1" in text
    assert "采样时间：2026-10-19 12:00:00" in text
    assert "本周期记录不完整，统计仅覆盖已记录时段" in text


@pytest.mark.parametrize(
    ("kind", "command", "anchor"),
    [
        ("day", "日报", date(2026, 10, 19)),
        ("week", "周报", date(2026, 10, 23)),
        ("month", "月报", date(2026, 10, 20)),
    ],
)
def test_all_calendar_reports_include_rank_and_exclude_later_samples(env, kind, command, anchor):
    service, storage, _, clock, _ = env
    start, end = period(kind, anchor)
    storage.snapshot(
        "market:author/plugin", start, {"downloads": 100, "download_rank": 10, "stars": 3}
    )
    storage.snapshot(
        "market:author/plugin", end, {"downloads": 110, "download_rank": 7, "stars": 5}
    )
    storage.snapshot(
        "market:author/plugin", end + 1, {"downloads": 120, "download_rank": 1, "stars": 999}
    )
    clock.now = end + 9 * 3600
    text = service.command("group", [command, "plugin", anchor.isoformat()])
    assert "下载排名：第 7 名｜⬆️3 名" in text
    assert "下载：110｜⬆️10" in text
    assert "Star：5｜⬆️2" in text
    assert "999" not in text
    assert "第 1 名" not in text
    assert "本周期记录不完整" not in text
    assert text.count("采样时间：") == 1
    assert "采样时间：" + datetime.fromtimestamp(end, BEIJING).strftime("%Y-%m-%d %H:%M:%S") in text


@pytest.mark.parametrize(
    ("ranks", "expected"),
    [
        ((10, 12), "下载排名：第 12 名｜⬇️2 名"),
        ((10, 10), "下载排名：第 10 名｜➖"),
        ((10,), "下载排名：第 10 名｜⏳ 待积累，只有一次采样"),
        ((), "下载排名：本周期无可用采样。"),
    ],
)
def test_rank_change_missing_samples_and_single_sample(env, ranks, expected):
    service, storage, _, clock, _ = env
    start = clock()
    for index, rank in enumerate(ranks):
        storage.snapshot(
            "market:author/plugin", start + (index + 1) * 3600, {"download_rank": rank}
        )
    clock.advance(3 * 3600)
    text = service.report((service.target("plugin"),), "day", date(2026, 10, 19))
    assert expected in text
    if ranks:
        assert "本周期记录不完整，统计仅覆盖已记录时段" in text


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["day", "week", "month"])
async def test_stored_subscriptions_include_required_rank_in_each_push(env, kind):
    service, storage, _, clock, sender = env
    start, end = period(kind, date(2026, 10, 19))
    clock.now = start
    storage.subscribe("group", "plugin", (kind,), (), clock())
    storage.snapshot(
        "market:author/plugin", start, {"downloads": 100, "download_rank": 10, "stars": 3}
    )
    storage.snapshot(
        "market:author/plugin", end, {"downloads": 120, "download_rank": 8, "stars": 5}
    )
    clock.now = end + 9 * 3600
    service.schedule()
    assert len(storage.pending(clock())) == 1
    await service.deliver()
    sender.assert_awaited_once()
    assert "下载排名：第 8 名｜⬆️2 名" in sender.call_args.args[1]
    assert "Star：5｜⬆️2" in sender.call_args.args[1]


def test_download_counter_reset_and_single_sample_are_not_false_zeroes(env):
    service, storage, _, clock, _ = env
    start = clock()
    storage.snapshot("market:author/plugin", start, {"downloads": 100})
    text = service.report((service.target("plugin"),), "day", date(2026, 10, 19))
    assert "待积累" in text
    storage.snapshot("market:author/plugin", start + 3600, {"downloads": 90})
    clock.advance(7200)
    text = service.report((service.target("plugin"),), "day", date(2026, 10, 19))
    assert "⚠️ 计数下降，增量不可用" in text
    assert "⬇️10" not in text


def test_stale_baseline_and_missing_end_are_visible(env):
    service, storage, _, clock, _ = env
    storage.snapshot("gh:owner/repo", clock() - 7200, {"stars": 10})
    storage.snapshot("gh:owner/repo", clock() + 3600, {"stars": 20})
    clock.advance(86400)
    text = service.report((service.target("bot"),), "day", date(2026, 10, 19))
    assert "期初基线过旧" in text
    assert "期末采样缺失或过旧" in text
    assert "采样存在间断" in text


def test_missing_activity_baseline_and_future_dates(env):
    service, _, _, _, _ = env
    text = service.report((service.target("bot"),), "day", date(2026, 10, 19))
    assert "事件尚未建立采集基线" in text
    assert "新增 0" not in text
    with pytest.raises(ObserverError, match="尚未开始"):
        service.report((service.target("bot"),), "month", date(2026, 11, 1))


def test_event_period_is_half_open(env):
    service, storage, _, clock, _ = env
    start = clock()
    add_events(
        storage,
        "release",
        start,
        start + 90000,
        [event("one", start + 100), event("two", start + 86400)],
    )
    clock.advance(90000)
    text = service.report((service.target("bot"),), "day", date(2026, 10, 19))
    assert "Release：1 条" in text


@pytest.mark.asyncio
async def test_shared_repository_is_collected_once(env):
    service, storage, api, _, _ = env
    api.put("market", "", market_data())
    api.put(
        "get",
        "/repos/owner/repo",
        {"stargazers_count": 10, "forks_count": 2, "default_branch": "main", "size": 1},
    )
    api.put("list", "/repos/owner/repo/issues", [])
    api.put("get", "/repos/owner/repo/branches/main", {"commit": {"sha": "sha"}})
    await service.collect()
    assert sum(call[1] == "/repos/owner/repo" for call in api.calls) == 1
    assert storage.state("market:author/plugin")["repo"] == "owner/repo"


@pytest.mark.asyncio
async def test_market_star_never_uses_github_snapshots_or_errors(env):
    from astrbot_plugin_github_observer.client import APIError

    _, storage, api, clock, sender = env
    settings = Settings.parse(config([market(metrics=["version"]), github()]))
    service = Service(settings, storage, api, sender, clock)
    clock.advance(11 * 3600)
    storage.snapshot("gh:owner/repo", clock(), {"stars": 999})
    api.put("market", "", market_data(stars=4))
    api.put("get", "/repos/owner/repo", APIError("GitHub HTTP 403：权限不足或触发限流。", 403))
    await service.collect()
    clock.advance(3600)
    api.put("market", "", market_data(stars=5))
    await service.collect()
    text = service.command("group", ["日报", "plugin"])
    assert "Star：5｜⬆️1" in text
    assert "999" not in text
    assert "403" not in text
    assert "缓存" not in text and "Star 来源" not in text
    assert text.count("采样时间：") == 1
    assert text.count("本周期记录不完整") == 1
    assert "事件采集截至：" not in text
    assert "开始监看：" not in text
    assert service.errors(service.target("plugin")) == []
    assert "403" in service.errors(service.target("bot"))[0]


@pytest.mark.asyncio
async def test_market_subscription_and_duplicate_target_merge_metrics_without_github(env):
    _, storage, api, clock, sender = env
    settings = Settings.parse(config([market(), market(alias="other", metrics=["version"])]))
    service = Service(settings, storage, api, sender, clock)
    service.subscribe("group", "plugin", ("day",))
    api.put("market", "", market_data(stars=7))
    await service.collect()
    assert api.calls == [("market", "", None)]
    assert storage.samples("market:author/plugin", "stars", 0, clock())[-1][1] == 7
    assert storage.stream("market:author/plugin|version") is not None
    assert "Star：7" in service.command("group", ["日报", "plugin"])


@pytest.mark.asyncio
async def test_individual_repo_failure_does_not_block_others(env):
    from astrbot_plugin_github_observer.client import APIError

    _, storage, api, clock, sender = env
    settings = Settings.parse(config([github("bad", "owner/bad"), github("good", "owner/good")]))
    service = Service(settings, storage, api, sender, clock)
    api.put("get", "/repos/owner/bad", APIError("not found", 404))
    api.put("get", "/repos/owner/good", {"stargazers_count": 8, "forks_count": 2})
    await service.collect()
    assert storage.samples("gh:owner/good", "stars", 0, clock())[-1][1] == 8
    assert storage.samples("gh:owner/good", "forks", 0, clock())[-1][1] == 2
    assert sum(call[1] == "/repos/owner/good" for call in api.calls) == 1
    assert service.errors(service.target("bad")) == ["not found"]


@pytest.mark.asyncio
async def test_midnight_collection_even_with_daily_interval(env):
    _, storage, api, clock, sender = env
    service = Service(Settings.parse(config(poll_minutes=1440)), storage, api, sender, clock)
    service.collect = AsyncMock()
    await service.tick()
    assert service.collect.await_count == 1
    clock.advance(23 * 3600)
    service.last_day = date(2026, 10, 19)
    service.next_poll = clock() + 7200
    await service.tick()
    assert service.collect.await_count == 1
    clock.advance(3600)
    await service.tick()
    assert service.collect.await_count == 2


@pytest.mark.asyncio
async def test_new_subscription_requests_collection(env):
    service, _, _, clock, _ = env
    service.collect = AsyncMock()
    await service.tick()
    service.subscribe("group", "bot", ("day",))
    clock.advance(30)
    await service.tick()
    assert service.collect.await_count == 2


def test_report_only_after_push_time_and_no_duplicates(env):
    service, storage, _, clock, _ = env
    service.subscribe("group", "bot", ("day",))
    clock.advance(86400 + 9 * 3600 - 1)
    service.schedule()
    assert storage.pending(clock()) == []
    clock.advance(1)
    service.schedule()
    assert len(storage.pending(clock())) == 1
    service.schedule()
    assert len(storage.pending(clock())) == 1


def test_week_and_month_schedules_and_missed_period_catchup(env):
    service, storage, _, clock, _ = env
    clock.now = instant("2026-09-29T12:00:00+08:00")
    service.subscribe("group", "bot", ("day", "week", "month"))
    clock.now = instant("2026-10-06T09:00:00+08:00")
    service.schedule()
    rows = storage.pending(clock())
    assert sum("· 日报" in row["text"] for row in rows) == 7
    assert sum("· 周报" in row["text"] for row in rows) == 1
    assert sum("· 月报" in row["text"] for row in rows) == 1
    assert any("本订阅于周期中途建立" in row["text"] for row in rows)


@pytest.mark.asyncio
async def test_send_false_retries_then_success_does_not_duplicate(env):
    service, storage, _, clock, sender = env
    service.subscribe("group", "bot", ("day",))
    clock.advance(86400 + 9 * 3600)
    service.schedule()
    sender.return_value = False
    await service.deliver()
    assert sender.await_count == 1
    await service.deliver()
    assert sender.await_count == 1
    clock.advance(60)
    sender.return_value = True
    await service.deliver()
    assert sender.await_count == 2
    service.schedule()
    await service.deliver()
    assert sender.await_count == 2
    assert storage.db.execute("SELECT sent FROM outbox").fetchone()[0] == clock()


@pytest.mark.asyncio
async def test_late_discovered_event_is_alerted_once(env):
    service, storage, _, clock, sender = env
    service.subscribe("group", "bot", ("day",), ("release",))
    created = clock()
    clock.advance(200)
    service.schedule()
    clock.advance(100)
    add_events(storage, "release", created, clock(), [event("late", created + 100)])
    service.schedule()
    await service.deliver()
    assert sender.await_count == 1
    clock.advance(30)
    service.schedule()
    await service.deliver()
    assert sender.await_count == 1


def test_unsubscribe_removes_pending_messages(env):
    service, storage, _, clock, _ = env
    service.subscribe("group", "bot", ("day",))
    clock.advance(86400 + 9 * 3600)
    service.schedule()
    assert storage.pending(clock())
    service.command("group", ["退订", "bot"])
    assert storage.pending(clock()) == []


@pytest.mark.asyncio
async def test_removed_target_is_paused_including_already_queued_messages(env):
    service, storage, api, clock, sender = env
    service.subscribe("group", "bot", ("day",))
    clock.advance(86400 + 9 * 3600)
    service.schedule()
    service = Service(Settings.parse(config()), storage, api, sender, clock)
    await service.deliver()
    assert sender.await_count == 0
    assert "目标已移除" in service.command("group", ["订阅列表"])


@pytest.mark.asyncio
async def test_database_reopen_preserves_outbox_and_report_progress(tmp_path):
    clock, api, sender = Clock(), FakeAPI(), AsyncMock(return_value=True)
    path = tmp_path / "restart.sqlite3"
    settings = Settings.parse(config([github()]))
    storage = Storage(path)
    service = Service(settings, storage, api, sender, clock)
    service.subscribe("group", "bot", ("day",))
    clock.advance(86400 + 9 * 3600)
    service.schedule()
    storage.close()
    storage = Storage(path)
    try:
        service = Service(settings, storage, api, sender, clock)
        service.schedule()
        assert len(storage.pending(clock())) == 1
        await service.deliver()
        service.schedule()
        assert storage.pending(clock()) == []
        assert sender.await_count == 1
    finally:
        storage.close()


def test_message_chunks_preserve_content():
    text = "测试" * 5000 + "\n" + "https://github.com/a/b\n"
    parts = list(chunks(text))
    assert "".join(parts) == text
    assert max(map(len, parts)) <= 2800


@pytest.mark.asyncio
async def test_busy_manual_refresh_does_not_consume_scheduled_collection(env):
    service, _, _, _, _ = env
    service.collect = AsyncMock()
    async with service.lock:
        await service.tick()
    assert service.needs_collection is True
    assert service.collect.await_count == 0
    await service.tick()
    assert service.collect.await_count == 1


def test_outbox_and_progress_rollback_together(env):
    import sqlite3

    service, storage, _, clock, _ = env
    service.subscribe("group", "bot", ("day",))
    subscription_id = storage.subscriptions()[0]["id"]
    storage.db.execute("""CREATE TRIGGER fail_test_message BEFORE INSERT ON outbox
        WHEN NEW.text='bad' BEGIN SELECT RAISE(ABORT,'test failure'); END""")
    with pytest.raises(sqlite3.IntegrityError):
        storage.enqueue("atomic", subscription_id, ["good", "bad"], clock(), ("progress-test", 10))
    assert storage.db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 0
    assert storage.state("progress-test") is None


def test_compact_date_is_rejected_even_on_python_312(env):
    with pytest.raises(ObserverError, match="YYYY-MM-DD"):
        env[0].command("group", ["日报", "bot", "20261019"])
