import json
from datetime import date
from pathlib import Path

import pytest
from astrbot_plugin_github_observer.domain import (
    ObserverError,
    Settings,
    instant,
    market_repository,
    period,
    repository,
)
from conftest import config, github, market


@pytest.mark.parametrize(
    ("kind", "anchor", "start", "end"),
    [
        ("day", date(2026, 10, 19), "2026-10-19T00:00:00+08:00", "2026-10-20T00:00:00+08:00"),
        ("week", date(2026, 10, 25), "2026-10-19T00:00:00+08:00", "2026-10-26T00:00:00+08:00"),
        ("month", date(2024, 2, 20), "2024-02-01T00:00:00+08:00", "2024-03-01T00:00:00+08:00"),
        ("month", date(2026, 12, 31), "2026-12-01T00:00:00+08:00", "2027-01-01T00:00:00+08:00"),
        ("week", date(2027, 1, 1), "2026-12-28T00:00:00+08:00", "2027-01-04T00:00:00+08:00"),
    ],
)
def test_calendar(kind, anchor, start, end):
    assert period(kind, anchor) == (instant(start), instant(end))


def test_required_metrics_and_case_normalization():
    settings = Settings.parse(
        config([github(repo="Owner/Repo"), market(plugin_id="Author/Plugin")])
    )
    assert settings.targets[0].metrics == ("stars", "forks")
    assert settings.targets[0].target == "owner/repo"
    assert settings.targets[1].metrics == ("downloads", "download_rank", "stars")
    assert settings.targets[1].target == "author/plugin"


@pytest.mark.parametrize("factory", [github, market], ids=["github", "market"])
@pytest.mark.parametrize("omit_alias", [False, True], ids=["empty", "omitted"])
def test_alias_is_optional(factory, omit_alias):
    item = factory(alias="")
    if omit_alias:
        del item["alias"]
    target = Settings.parse(config([item])).targets[0]
    assert target.alias == target.target


@pytest.mark.parametrize("alias", [None, 123, " ", "a/b", "a,b", "a|b", "a" * 41])
def test_nonempty_alias_must_be_valid(alias):
    with pytest.raises(ObserverError, match="目标别名"):
        Settings.parse(config([github(alias=alias)]))


def test_default_identity_collision_requires_explicit_alias():
    items = [github(alias="", repo="Owner/Repo"), market(alias="", plugin_id="owner/repo")]
    with pytest.raises(ObserverError, match="目标标识重复：owner/repo"):
        Settings.parse(config(items))
    items[1]["alias"] = "市场插件"
    assert [t.alias for t in Settings.parse(config(items)).targets] == ["owner/repo", "市场插件"]


@pytest.mark.parametrize("kind", ["market", "github"])
def test_template_defaults_and_checkbox_choices_load(kind):
    schema = json.loads(
        (Path(__file__).resolve().parents[1] / "_conf_schema.json").read_text(encoding="utf-8")
    )
    items = schema["targets"]["templates"][kind]["items"]
    entry = {"__template_key": kind, **{key: value["default"] for key, value in items.items()}}
    entry["plugin_id" if kind == "market" else "repo"] = "Owner/Repo"
    target = Settings.parse(config([entry])).targets[0]
    assert target.alias == "owner/repo"
    assert target.metrics == target.required
    metric = items["metrics"]
    assert metric["type"] == "list" and metric["render_type"] == "checkbox"
    assert len(metric["labels"]) == len(metric["options"])
    assert set(metric["options"]) == target.allowed - set(target.required)
    entry["metrics"] = metric["options"]
    assert Settings.parse(config([entry])).targets[0].metrics == (
        *target.required,
        *metric["options"],
    )


@pytest.mark.parametrize("enabled", [False, True])
def test_market_github_switch_controls_fork_and_repository_metrics(enabled):
    target = Settings.parse(
        config(
            [
                market(
                    github_enabled=enabled,
                    metrics=["version"],
                    github_metrics=["release", "commits"],
                )
            ]
        )
    ).targets[0]
    assert target.github_enabled is enabled
    assert target.metrics == (
        ("downloads", "download_rank", "stars", "forks", "version", "release", "commits")
        if enabled
        else ("downloads", "download_rank", "stars", "version")
    )


def test_market_github_options_match_repository_options_and_are_conditional():
    schema = json.loads(
        (Path(__file__).resolve().parents[1] / "_conf_schema.json").read_text(encoding="utf-8")
    )
    templates = schema["targets"]["templates"]
    items = templates["market"]["items"]
    assert items["github_enabled"]["type"] == "bool" and items["github_enabled"]["default"] is False
    assert items["github"]["condition"] == {"github_enabled": True}
    linked = items["github"]["items"]["metrics"]
    repository = templates["github"]["items"]["metrics"]
    for key in ("type", "options", "labels", "render_type", "default"):
        assert linked[key] == repository[key]


@pytest.mark.parametrize(
    "update",
    [
        {"github_enabled": 1},
        {"github_enabled": "true"},
        {"github": None},
        {"github": {}},
        {"github": {"metrics": [], "repo": "owner/repo"}},
        {"github": {"metrics": "release"}},
        {"github": {"metrics": ["version"]}},
        {"github": {"metrics": ["stars"]}},
        {"github": {"metrics": ["forks"]}},
        {"github": {"metrics": ["release", "release"]}},
        {"metrics": ["release"]},
    ],
)
def test_invalid_market_github_configuration_fails_even_when_switch_is_off(update):
    with pytest.raises(ObserverError):
        Settings.parse(config([{**market(), **update}]))


@pytest.mark.parametrize("missing", ["github_enabled", "github"])
def test_market_requires_current_configuration_fields(missing):
    item = market()
    del item[missing]
    with pytest.raises(ObserverError, match="配置项缺失或未知"):
        Settings.parse(config([item]))


@pytest.mark.parametrize(
    "update",
    [
        {"poll_minutes": True},
        {"poll_minutes": 0},
        {"request_timeout": 1000},
        {"max_pages": 0},
        {"push_time": "9:00"},
        {"push_time": "24:00"},
        {"market_url": "http://example.com"},
        {"market_url": "https://user:password@example.com"},
        {"github_token": "abc\n123"},
        {"legacy_token": "old"},
    ],
)
def test_bad_config_fails_fast(update):
    with pytest.raises(ObserverError):
        Settings.parse(config(**update))


def test_missing_configuration_not_silently_defaulted():
    with pytest.raises(ObserverError):
        Settings.parse({})


@pytest.mark.parametrize(
    "targets",
    [
        [github(), github()],
        [github(metrics=["downloads"])],
        [github(metrics=["download_rank"])],
        [github(metrics=["stars"])],
        [github(metrics=["forks"])],
        [github(metrics=["unknown"])],
        [market(metrics=["forks"])],
        [market(metrics=["downloads"])],
        [market(metrics=["download_rank"])],
        [market(metrics=["stars"])],
        [market(metrics=["unknown"])],
        [github(alias="has space")],
        [github(metrics=["forks", "forks"])],
        [{**github(), "old_branch": "main"}],
    ],
)
def test_invalid_target_configuration(targets):
    with pytest.raises(ObserverError):
        Settings.parse(config(targets))


@pytest.mark.parametrize("slug", ["https://github.com/a/b", "a/b.git", "a/..", "../b", "a/b/c"])
def test_slug_rejects_alternate_formats(slug):
    with pytest.raises(ObserverError):
        repository(slug)


def test_market_repository_url():
    assert market_repository("https://github.com/A/B.git") == "a/b"
    assert market_repository("https://github.com/A/B/tree/main") == "a/b"
    for url in [
        "https://github.com.evil/a/b",
        "https://github.com/a/b/issues",
        "http://github.com/a/b",
    ]:
        with pytest.raises(ObserverError):
            market_repository(url)
