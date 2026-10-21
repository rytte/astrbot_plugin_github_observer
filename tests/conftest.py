"""Isolate imports from the repository's real AstrBot runtime."""

import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT.parent))
sys.path.insert(0, str(ROOT.parent / "AstrBot"))
RUNTIME = tempfile.TemporaryDirectory(prefix="github-observer-tests-")
os.environ["ASTRBOT_ROOT"] = RUNTIME.name

from astrbot_plugin_github_observer.domain import Settings, instant
from astrbot_plugin_github_observer.service import Service
from astrbot_plugin_github_observer.storage import Storage


def config(targets=None, **updates):
    data = {
        key: value["default"]
        for key, value in json.loads(
            (ROOT / "_conf_schema.json").read_text(encoding="utf-8")
        ).items()
    }
    data["targets"] = targets or []
    return {**data, **updates}


def github(alias="bot", repo="owner/repo", metrics=None, branch=""):
    return {
        "__template_key": "github",
        "alias": alias,
        "repo": repo,
        "branch": branch,
        "metrics": metrics or [],
    }


def market(
    alias="plugin",
    plugin_id="author/plugin",
    metrics=None,
    github_enabled=False,
    github_metrics=None,
):
    return {
        "__template_key": "market",
        "alias": alias,
        "plugin_id": plugin_id,
        "metrics": metrics or [],
        "github_enabled": github_enabled,
        "github": {"metrics": github_metrics or []},
    }


class Clock:
    def __init__(self, value="2026-10-19T00:00:00+08:00"):
        self.now = instant(value)

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class FakeAPI:
    def __init__(self):
        self.routes = {}
        self.calls = []

    def put(self, method, path, value):
        self.routes[method, path] = value

    def response(self, method, path, params):
        self.calls.append((method, path, params))
        if (method, path) not in self.routes:
            raise AssertionError(f"Unexpected API call: {method} {path} {params}")
        value = self.routes[method, path]
        if isinstance(value, Exception):
            raise value
        return value(params) if callable(value) else value

    async def get(self, path, params=None, *, market=False):
        return self.response("market" if market else "get", path, params), {}

    async def listing(self, path, params=None, field=None):
        result = self.response("list", path, params)
        return (result[field], result) if field else (result, result)


@pytest.fixture
def env(tmp_path):
    settings = Settings.parse(
        config(
            [
                github(metrics=["commits", "release", "issues", "prs"]),
                market(metrics=["version"]),
            ]
        )
    )
    storage, client, clock = Storage(tmp_path / "observer.sqlite3"), FakeAPI(), Clock()
    sender = AsyncMock(return_value=True)
    service = Service(settings, storage, client, sender, clock)
    yield service, storage, client, clock, sender
    storage.close()
