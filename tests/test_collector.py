import pytest
from astrbot_plugin_github_observer.client import APIError
from astrbot_plugin_github_observer.domain import utc_string


def market_data(downloads=100, version="1.0", stars=5):
    return {
        "$meta": {"schema_version": 1},
        "author/plugin": {
            "author": "author",
            "name": "plugin",
            "version": version,
            "repo": "https://github.com/owner/repo",
            "download_count": downloads,
            "stars": stars,
        },
    }


@pytest.mark.asyncio
async def test_market_baseline_downloads_and_version_changes(env):
    service, storage, api, clock, _ = env
    api.put("market", "", market_data())
    target = service.target("plugin")
    watched = {target.source: set(target.metrics)}
    await service.collector.market([target], watched)
    assert not storage.events(target.source, "version", 0, clock() + 1)
    clock.advance(3600)
    api.put("market", "", market_data(120, "1.1"))
    await service.collector.market([target], watched)
    await service.collector.market([target], watched)
    events = storage.events(target.source, "version", 0, clock() + 1)
    assert len(events) == 1
    assert events[0]["title"] == "1.0 → 1.1"
    assert storage.samples(target.source, "downloads", 0, clock())[-1][1] == 120


@pytest.mark.asyncio
async def test_missing_download_count_is_not_zero(env):
    service, storage, api, clock, _ = env
    data = market_data()
    del data["author/plugin"]["download_count"]
    api.put("market", "", data)
    target = service.target("plugin")
    await service.collector.market([target], {target.source: set(target.metrics)})
    assert storage.samples("market:author/plugin", "downloads", 0, clock()) == []
    assert any("未提供 download_count" in error for error in service.errors(target))


@pytest.mark.asyncio
@pytest.mark.parametrize(("downloads", "expected"), [(200, 1), (100, 3), (50, 4), (0, 5)])
async def test_download_rank_uses_entire_market_and_competition_ties(env, downloads, expected):
    service, storage, api, clock, _ = env
    data = market_data(downloads=downloads)
    for name, count in (("top-a", 200), ("top-b", 200), ("mid", 100), ("low", 50), ("zero", 0)):
        data["other/" + name] = {
            **data["author/plugin"],
            "author": "other",
            "name": name,
            "download_count": count,
        }
    api.put("market", "", data)
    target = service.target("plugin")
    await service.collector.market([target], {target.source: set(target.metrics)})
    assert storage.samples(target.source, "download_rank", 0, clock()) == [(clock(), expected)]
    assert storage.db.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 1
    assert api.calls == [("market", "", None)]


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["missing", None, True, -1, "5"])
async def test_missing_other_plugin_downloads_blocks_only_rank_and_recovers(env, invalid):
    service, storage, api, clock, _ = env
    data = market_data()
    data["other/plugin"] = {"download_count": invalid}
    if invalid == "missing":
        del data["other/plugin"]["download_count"]
    api.put("market", "", data)
    target = service.target("plugin")
    watched = {target.source: set(target.metrics)}
    await service.collector.market([target], watched)
    assert storage.samples(target.source, "download_rank", 0, clock()) == []
    assert storage.samples(target.source, "downloads", 0, clock())[-1][1] == 100
    assert storage.samples(target.source, "stars", 0, clock())[-1][1] == 5
    assert storage.stream(target.source + "|version") is not None
    assert any("下载排名不可用" in error for error in service.errors(target))
    clock.advance(3600)
    data["other/plugin"]["download_count"] = 200
    await service.collector.market([target], watched)
    assert storage.samples(target.source, "download_rank", 0, clock())[-1][1] == 2
    assert service.errors(target) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["missing", None, True, -1, "5"])
async def test_invalid_market_star_does_not_block_downloads_or_versions_or_use_github(env, invalid):
    from astrbot_plugin_github_observer.domain import Settings
    from astrbot_plugin_github_observer.service import Service
    from conftest import config, market

    _, storage, api, clock, sender = env
    service = Service(
        Settings.parse(config([market(metrics=["version"])])), storage, api, sender, clock
    )
    data = market_data(stars=invalid)
    if invalid == "missing":
        del data["author/plugin"]["stars"]
    api.put("market", "", data)
    await service.collect()
    assert api.calls == [("market", "", None)]
    assert storage.samples("market:author/plugin", "stars", 0, clock()) == []
    assert storage.samples("market:author/plugin", "downloads", 0, clock())[-1][1] == 100
    assert storage.stream("market:author/plugin|version") is not None
    assert "stars" in service.errors(service.target("plugin"))[0]
    clock.advance(3600)
    api.put("market", "", market_data(stars=0))
    await service.collect()
    assert storage.samples("market:author/plugin", "stars", 0, clock())[-1][1] == 0
    assert service.errors(service.target("plugin")) == []


@pytest.mark.asyncio
async def test_old_market_schema_is_rejected(env):
    service, storage, api, clock, _ = env
    data = market_data()
    del data["$meta"]
    api.put("market", "", data)
    target = service.target("plugin")
    await service.collector.market([target], {target.source: set(target.metrics)})
    assert storage.state("market:author/plugin") is None
    assert "schema_version" in service.errors(service.target("plugin"))[0]


@pytest.mark.asyncio
async def test_open_issue_counts_exclude_pull_requests(env):
    service, storage, api, clock, _ = env
    api.put(
        "get",
        "/repos/owner/repo",
        {"stargazers_count": 3, "forks_count": 2, "default_branch": "main", "size": 1},
    )
    api.put("list", "/repos/owner/repo/issues", [{"number": 1}, {"number": 2, "pull_request": {}}])
    await service.collector.repository("owner/repo", {"stars", "forks", "issues", "prs"}, set())
    assert storage.samples("gh:owner/repo", "issues", 0, clock())[-1][1] == 1
    assert storage.samples("gh:owner/repo", "prs", 0, clock())[-1][1] == 1


@pytest.mark.asyncio
async def test_issue_pr_events_reopen_merge_and_deduplication(env):
    service, storage, api, clock, _ = env
    collector = service.collector
    await collector.issue_events("owner/repo", {"issues", "prs"})
    clock.advance(3600)
    at = utc_string(clock() - 60)
    api.put(
        "list",
        "/repos/owner/repo/issues",
        [
            {
                "number": 1,
                "created_at": at,
                "title": "Bug",
                "html_url": "https://github.com/owner/repo/issues/1",
            },
            {
                "number": 2,
                "created_at": at,
                "title": "Fix",
                "pull_request": {},
                "html_url": "https://github.com/owner/repo/pull/2",
            },
        ],
    )
    api.put(
        "list",
        "/repos/owner/repo/issues/1/timeline",
        [
            {"id": 10, "event": "closed", "created_at": at},
            {"id": 11, "event": "reopened", "created_at": at},
        ],
    )
    api.put(
        "list",
        "/repos/owner/repo/issues/2/timeline",
        [
            {"id": 12, "event": "merged", "created_at": at},
            {"id": 13, "event": "closed", "created_at": at},
        ],
    )
    await collector.issue_events("owner/repo", {"issues", "prs"})
    await collector.issue_events("owner/repo", {"issues", "prs"})
    assert sorted(
        e["action"] for e in storage.events("gh:owner/repo", "issues", 0, clock() + 1)
    ) == ["closed", "opened", "reopened"]
    assert sorted(e["action"] for e in storage.events("gh:owner/repo", "prs", 0, clock() + 1)) == [
        "merged",
        "opened",
    ]


@pytest.mark.asyncio
async def test_failed_timeline_does_not_advance_cursor_or_write_partial_events(env):
    service, storage, api, clock, _ = env
    await service.collector.issue_events("owner/repo", {"issues"})
    old = storage.stream("gh:owner/repo|issues")["checked"]
    clock.advance(600)
    api.put(
        "list",
        "/repos/owner/repo/issues",
        [{"number": 1, "created_at": utc_string(clock()), "title": "Bug"}],
    )
    api.put("list", "/repos/owner/repo/issues/1/timeline", APIError("限流"))
    with pytest.raises(APIError):
        await service.collector.issue_events("owner/repo", {"issues"})
    assert storage.stream("gh:owner/repo|issues")["checked"] == old
    assert storage.events("gh:owner/repo", "issues", 0, clock() + 1) == []


@pytest.mark.asyncio
async def test_commit_baseline_comparison_and_sha_deduplication(env):
    service, storage, api, clock, _ = env
    api.put("get", "/repos/owner/repo/branches/main", {"commit": {"sha": "old"}})
    await service.collector.commits("owner/repo", "main", False)
    assert storage.events("gh:owner/repo", "commits@main", 0, clock() + 1) == []
    clock.advance(3600)
    api.put("get", "/repos/owner/repo/branches/main", {"commit": {"sha": "new"}})
    api.put(
        "list",
        "/repos/owner/repo/compare/old...new",
        {
            "status": "ahead",
            "commits": [
                {
                    "sha": "new",
                    "commit": {"message": "Fix bug\nDetails"},
                    "html_url": "https://github.com/owner/repo/commit/new",
                }
            ],
        },
    )
    await service.collector.commits("owner/repo", "main", False)
    await service.collector.commits("owner/repo", "main", False)
    events = storage.events("gh:owner/repo", "commits@main", 0, clock() + 1)
    assert len(events) == 1
    assert events[0]["at"] == clock()
    assert events[0]["title"] == "new Fix bug"


@pytest.mark.asyncio
async def test_commit_force_push_is_visible(env):
    service, storage, api, clock, _ = env
    api.put("get", "/repos/owner/repo/branches/main", {"commit": {"sha": "old"}})
    await service.collector.commits("owner/repo", "main", False)
    clock.advance(60)
    api.put("get", "/repos/owner/repo/branches/main", {"commit": {"sha": "new"}})
    api.put("list", "/repos/owner/repo/compare/old...new", {"status": "diverged", "commits": []})
    await service.collector.commits("owner/repo", "main", False)
    assert (
        storage.events("gh:owner/repo", "commits@main", 0, clock() + 1)[0]["action"]
        == "history_changed"
    )


@pytest.mark.asyncio
async def test_empty_repo_first_real_commit(env):
    service, storage, api, clock, _ = env
    api.put("get", "/repos/owner/repo/branches/main", APIError("empty", 404))
    await service.collector.commits("owner/repo", "main", True)
    clock.advance(100)
    api.put("get", "/repos/owner/repo/branches/main", {"commit": {"sha": "first"}})
    api.put(
        "list", "/repos/owner/repo/commits", [{"sha": "first", "commit": {"message": "Initial"}}]
    )
    await service.collector.commits("owner/repo", "main", False)
    assert len(storage.events("gh:owner/repo", "commits@main", 0, clock() + 1)) == 1


@pytest.mark.asyncio
async def test_release_baseline_pre_release_and_draft_handling(env):
    service, storage, api, clock, _ = env
    await service.collector.releases("owner/repo")
    clock.advance(60)
    api.put(
        "list",
        "/repos/owner/repo/releases",
        [
            {"id": 1, "published_at": utc_string(clock() - 1000), "tag_name": "old"},
            {"id": 2, "published_at": utc_string(clock()), "tag_name": "v2", "prerelease": True},
            {"id": 3, "published_at": utc_string(clock()), "tag_name": "draft", "draft": True},
        ],
    )
    await service.collector.releases("owner/repo")
    await service.collector.releases("owner/repo")
    events = storage.events("gh:owner/repo", "release", 0, clock() + 1)
    assert len(events) == 1
    assert "预发布" in events[0]["title"]


@pytest.mark.asyncio
async def test_invalid_count_is_not_silently_coerced(env):
    service, _, api, _, _ = env
    api.put("market", "", market_data(downloads=True))
    target = service.target("plugin")
    await service.collector.market([target], {target.source: set(target.metrics)})
    assert "非负整数" in service.errors(service.target("plugin"))[0]


@pytest.mark.asyncio
async def test_merge_close_with_different_timestamp_preserves_earlier_unmerged_close(env):
    service, storage, api, clock, _ = env
    await service.collector.issue_events("owner/repo", {"prs"})
    start = clock()
    clock.advance(3600)
    api.put(
        "list",
        "/repos/owner/repo/issues",
        [{"number": 3, "created_at": utc_string(start + 1), "pull_request": {}}],
    )
    api.put(
        "list",
        "/repos/owner/repo/issues/3/timeline",
        [
            {"id": 1, "event": "closed", "created_at": utc_string(start + 100)},
            {"id": 2, "event": "reopened", "created_at": utc_string(start + 200)},
            {"id": 3, "event": "closed", "created_at": utc_string(start + 298)},
            {"id": 4, "event": "merged", "created_at": utc_string(start + 300)},
        ],
    )
    await service.collector.issue_events("owner/repo", {"prs"})
    events = storage.events("gh:owner/repo", "prs", 0, clock() + 1)
    assert [e["action"] for e in events].count("closed") == 1
    assert [e["action"] for e in events].count("merged") == 1


@pytest.mark.asyncio
async def test_empty_repository_can_remain_empty_across_polls(env):
    service, storage, api, clock, _ = env
    api.put("get", "/repos/owner/repo/branches/main", APIError("empty", 404))
    await service.collector.commits("owner/repo", "main", True)
    clock.advance(3600)
    await service.collector.commits("owner/repo", "main", True)
    assert storage.stream("gh:owner/repo|commits@main")["checked"] == clock()


@pytest.mark.asyncio
async def test_market_version_monitoring_is_optional(env):
    service, storage, api, _, _ = env
    api.put("market", "", market_data())
    target = service.target("plugin")
    await service.collector.market([target], {target.source: set(target.required)})
    assert storage.stream("market:author/plugin|version") is None
    assert storage.samples(target.source, "stars", 0, service.clock())[-1][1] == 5


@pytest.mark.asyncio
async def test_default_and_named_same_branch_share_one_collection(env):
    service, _, api, _, _ = env
    api.put(
        "get", "/repos/owner/repo", {"stargazers_count": 10, "default_branch": "main", "size": 1}
    )
    api.put("get", "/repos/owner/repo/branches/main", {"commit": {"sha": "head"}})
    await service.collector.repository("owner/repo", {"stars", "commits"}, {"", "main"})
    assert sum(call[1] == "/repos/owner/repo/branches/main" for call in api.calls) == 1
