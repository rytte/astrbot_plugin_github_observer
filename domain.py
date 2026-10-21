"""Configuration, identities and calendar periods."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from urllib.parse import urlsplit

BEIJING = timezone(timedelta(hours=8), "Asia/Shanghai")
LABELS = {
    "downloads": "下载",
    "download_rank": "下载排名",
    "stars": "Star",
    "forks": "Fork",
    "commits": "提交",
    "release": "Release",
    "version": "市场版本",
    "issues": "Issue",
    "prs": "PR",
}
PERIODS = {"日报": "day", "周报": "week", "月报": "month"}
ALERTS = {"version", "release", "issues", "prs", "commits"}
MARKET_METRICS = {"downloads", "download_rank", "stars", "version"}
GITHUB_EXTRA_METRICS = {"commits", "release", "issues", "prs"}
REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")


class ObserverError(Exception):
    """An actionable error safe to display to an administrator."""


def repository(value: str) -> str:
    """Validate a repository slug, without accepting alternate URL formats."""
    if not isinstance(value, str) or not REPOSITORY.fullmatch(value):
        raise ObserverError("GitHub 仓库必须使用 owner/repo 格式。")
    if any(part in {".", ".."} or part.endswith(".git") for part in value.split("/")):
        raise ObserverError("仓库名不能是 .、.. 或带 .git 后缀。")
    return value.lower()


def market_repository(value: str) -> str:
    """Parse the repository URL carried by the current marketplace schema."""
    if not isinstance(value, str):
        raise ObserverError("市场 repo 字段必须是 HTTPS GitHub 仓库地址。")
    url = urlsplit(value)
    if url.scheme != "https" or url.netloc.lower() != "github.com" or url.query or url.fragment:
        raise ObserverError("市场 repo 字段必须是 HTTPS GitHub 仓库地址。")
    parts = url.path.strip("/").split("/")
    if len(parts) != 2 and not (len(parts) == 4 and parts[2] == "tree" and parts[3]):
        raise ObserverError("市场 repo 不是仓库地址或 /tree/分支 地址。")
    return repository(parts[0] + "/" + parts[1].removesuffix(".git"))


def instant(value: str) -> float:
    """Parse a timezone-aware API timestamp to UTC seconds."""
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError, AttributeError) as exc:
        raise ObserverError("接口返回了无效时间戳。") from exc
    if result.tzinfo is None:
        raise ObserverError("接口时间戳缺少时区。")
    return result.timestamp()


def stamp(value: float) -> str:
    return datetime.fromtimestamp(value, BEIJING).strftime("%Y-%m-%d %H:%M:%S")


def utc_string(value: float) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def period(kind: str, anchor: date) -> tuple[float, float]:
    """Return a half-open Beijing calendar interval containing the date."""
    if kind == "day":
        start, end = anchor, anchor + timedelta(days=1)
    elif kind == "week":
        start = anchor - timedelta(days=anchor.weekday())
        end = start + timedelta(days=7)
    elif kind == "month":
        start = anchor.replace(day=1)
        end = date(start.year + (start.month == 12), start.month % 12 + 1, 1)
    else:
        raise ObserverError("统计周期必须是日报、周报或月报。")
    return (
        datetime.combine(start, time(), BEIJING).timestamp(),
        datetime.combine(end, time(), BEIJING).timestamp(),
    )


def selections(value: str, allowed: set[str]) -> tuple[str, ...]:
    items = value.split(",")
    if not items or any(item not in allowed for item in items):
        raise ObserverError("可选值：" + ",".join(sorted(allowed)))
    if len(set(items)) != len(items):
        raise ObserverError("选项不能重复。")
    return tuple(items)


@dataclass(frozen=True)
class Target:
    alias: str
    kind: str
    target: str
    metrics: tuple[str, ...]
    branch: str = ""
    github_enabled: bool = False

    @property
    def allowed(self) -> set[str]:
        if self.kind == "market":
            return MARKET_METRICS | (
                {"forks"} | GITHUB_EXTRA_METRICS if self.github_enabled else set()
            )
        return {"stars", "forks"} | GITHUB_EXTRA_METRICS

    @property
    def required(self) -> tuple[str, ...]:
        if self.kind == "market":
            metrics = ("downloads", "download_rank", "stars")
            return (*metrics, "forks") if self.github_enabled else metrics
        return ("stars", "forks")

    @property
    def source(self) -> str:
        return ("market:" if self.kind == "market" else "gh:") + self.target

    def choose(self, metrics: tuple[str, ...]) -> tuple[str, ...]:
        if set(metrics) - self.allowed:
            raise ObserverError(f"{self.alias} 支持的指标：" + ",".join(sorted(self.allowed)))
        return tuple(dict.fromkeys((*self.required, *metrics)))


@dataclass(frozen=True)
class Settings:
    targets: tuple[Target, ...]
    github_token: str
    market_url: str
    poll_minutes: int
    request_timeout: int
    max_pages: int
    push_time: str

    @classmethod
    def parse(cls, data: dict) -> Settings:
        fields = {
            "targets",
            "github_token",
            "market_url",
            "poll_minutes",
            "request_timeout",
            "max_pages",
            "push_time",
        }
        if set(data) != fields:
            raise ObserverError("配置项缺失或未知，请按当前 _conf_schema.json 配置。")
        for key, low, high in (
            ("poll_minutes", 5, 1440),
            ("request_timeout", 5, 120),
            ("max_pages", 1, 500),
        ):
            if type(data[key]) is not int or not low <= data[key] <= high:
                raise ObserverError(f"{key} 必须是 {low}～{high} 的整数。")
        if not isinstance(data["github_token"], str) or any(
            c.isspace() for c in data["github_token"]
        ):
            raise ObserverError("github_token 必须是不含空白的字符串，可以留空。")
        if not isinstance(data["market_url"], str):
            raise ObserverError("market_url 必须是 HTTPS URL。")
        url = urlsplit(data["market_url"])
        if url.scheme != "https" or not url.netloc or url.username or url.password or url.fragment:
            raise ObserverError("market_url 必须是无账号密码和片段的 HTTPS URL。")
        if not isinstance(data["push_time"], str) or not re.fullmatch(
            r"(?:[01]\d|2[0-3]):[0-5]\d", data["push_time"]
        ):
            raise ObserverError("push_time 必须是北京时间 HH:MM。")
        if not isinstance(data["targets"], list):
            raise ObserverError("targets 必须是列表。")
        targets = []
        aliases = set()
        for item in data["targets"]:
            if not isinstance(item, dict) or item.get("__template_key") not in {"market", "github"}:
                raise ObserverError("每个目标必须选择 market 或 github 模板。")
            kind = item["__template_key"]
            expected = {"__template_key", "metrics"} | (
                {"plugin_id", "github_enabled", "github"}
                if kind == "market"
                else {"repo", "branch"}
            )
            if not expected <= set(item) or set(item) - expected - {"alias"}:
                raise ObserverError("目标配置项缺失或未知，请重新检查目标模板。")
            alias = item.get("alias", "")
            if not isinstance(alias, str) or (alias and not re.fullmatch(r"[^\s,/|]{1,40}", alias)):
                raise ObserverError(
                    "目标别名可留空；填写时须为 1～40 字符，不能含空白、逗号、/ 或 |。"
                )
            if kind == "market":
                identity = item["plugin_id"]
                if (
                    not isinstance(identity, str)
                    or identity.count("/") != 1
                    or any(not p or p.strip() != p for p in identity.split("/"))
                    or any(ord(c) < 32 or ord(c) == 127 for c in identity)
                ):
                    raise ObserverError("plugin_id 必须是市场 作者/插件名。")
                identity, branch = identity.lower(), ""
            else:
                identity, branch = repository(item["repo"]), item["branch"]
                if (
                    not isinstance(branch, str)
                    or branch.strip() != branch
                    or any(ord(c) < 32 or ord(c) == 127 for c in branch)
                ):
                    raise ObserverError("branch 必须是分支名字符串，留空表示默认分支。")
            alias = alias or identity
            if alias in aliases:
                raise ObserverError(f"目标标识重复：{alias}。请为其中一个目标设置不同别名。")
            aliases.add(alias)
            github_enabled, github_metrics = False, []
            if kind == "market":
                github_enabled = item["github_enabled"]
                if type(github_enabled) is not bool:
                    raise ObserverError("github_enabled 必须是布尔值。")
                if not isinstance(item["github"], dict) or set(item["github"]) != {"metrics"}:
                    raise ObserverError("github 必须包含且仅包含 metrics 配置。")
                github_metrics = item["github"]["metrics"]
            groups = [
                (
                    "metrics",
                    item["metrics"],
                    {"version"} if kind == "market" else GITHUB_EXTRA_METRICS,
                )
            ]
            if kind == "market":
                groups.append(("github.metrics", github_metrics, GITHUB_EXTRA_METRICS))
            for name, metrics, allowed in groups:
                if (
                    not isinstance(metrics, list)
                    or any(not isinstance(x, str) for x in metrics)
                    or len(set(metrics)) != len(metrics)
                ):
                    raise ObserverError(f"{name} 必须是不重复的指标字符串列表。")
                if set(metrics) - allowed:
                    raise ObserverError(
                        f"{alias} 的 {name} 附加指标只能选择：" + ",".join(sorted(allowed))
                    )
            target = Target(alias, kind, identity, (), branch, github_enabled)
            metrics = tuple(item["metrics"]) + (tuple(github_metrics) if github_enabled else ())
            targets.append(
                Target(alias, kind, identity, target.choose(metrics), branch, github_enabled)
            )
        return cls(tuple(targets), **{k: data[k] for k in fields - {"targets"}})
