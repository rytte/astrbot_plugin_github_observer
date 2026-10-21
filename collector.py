"""Repository snapshots and durable incremental activity streams."""

from __future__ import annotations

import time
from urllib.parse import quote

from .client import APIError
from .domain import LABELS, ObserverError, instant, market_repository, utc_string


def count(value):
    if type(value) is not int or value < 0:
        raise ObserverError("接口计数字段缺失或不是非负整数。")
    return value


class Collector:
    def __init__(self, client, storage, clock=time.time):
        self.client, self.storage, self.clock = client, storage, clock

    async def market(self, targets, watched_metrics):
        try:
            data, _ = await self.client.get("", market=True)
            if (
                not isinstance(data, dict)
                or not isinstance(data.get("$meta"), dict)
                or type(data["$meta"].get("schema_version")) is not int
                or data["$meta"]["schema_version"] != 1
            ):
                raise ObserverError("市场源必须使用含 $meta.schema_version=1 的当前格式。")
            index = {}
            for key, record in data.items():
                if key == "$meta":
                    continue
                if not isinstance(record, dict):
                    raise ObserverError("市场记录必须是对象。")
                # The observer deliberately requires canonical author/name keys.
                if key.lower() in index:
                    raise ObserverError("市场中存在大小写冲突的插件 ID。")
                index[key.lower()] = record
        except ObserverError as exc:
            for target in targets:
                self.storage.health(target.source, self.clock(), str(exc))
            return {}
        ranks, rank_error = {}, ""
        try:
            downloads = sorted(
                (count(record.get("download_count")) for record in index.values()), reverse=True
            )
            for position, downloads_count in enumerate(downloads, 1):
                ranks.setdefault(downloads_count, position)
        except ObserverError:
            rank_error = "市场部分插件的 download_count 缺失或不是非负整数，下载排名不可用。"
        repositories = {}
        for target in targets:
            try:
                record = index.get(target.target)
                if record is None:
                    raise ObserverError("市场中找不到配置的插件 ID，请填写市场作者/插件名。")
                author, name = record.get("author"), record.get("name")
                if (
                    not isinstance(author, str)
                    or not isinstance(name, str)
                    or f"{author}/{name}".lower() != target.target
                ):
                    raise ObserverError("市场记录的 author/name 与插件 ID 不一致。")
                version = record.get("version")
                if not isinstance(version, str) or not version.strip():
                    raise ObserverError("市场记录缺少有效 version。")
                repo, repo_error = None, ""
                try:
                    repo = market_repository(record.get("repo"))
                except ObserverError as exc:
                    repo_error = str(exc)
                now = self.clock()
                self.storage.put_state(
                    target.source,
                    {
                        "repo": repo,
                        "version": version,
                        "market_updated_at": data["$meta"].get("updated_at"),
                    },
                )
                repositories[target.source] = (repo, repo_error)
                snapshot = {"version": version}
                for metric, field in (("downloads", "download_count"), ("stars", "stars")):
                    if metric not in watched_metrics[target.source]:
                        continue
                    key = target.source + "|" + metric
                    if field not in record:
                        self.storage.health(
                            key, now, f"市场未提供 {field}，{LABELS[metric]}不可用。"
                        )
                        continue
                    try:
                        snapshot[metric] = count(record[field])
                    except ObserverError:
                        self.storage.health(
                            key, now, f"市场 {field} 必须是非负整数，{LABELS[metric]}不可用。"
                        )
                    else:
                        self.storage.health(key, now)
                if "download_rank" in watched_metrics[target.source]:
                    key = target.source + "|download_rank"
                    if rank_error:
                        self.storage.health(key, now, rank_error)
                    else:
                        snapshot["download_rank"] = ranks[record["download_count"]]
                        self.storage.health(key, now)
                self.storage.snapshot(target.source, now, snapshot)
                if "version" not in watched_metrics[target.source]:
                    self.storage.health(target.source, now)
                    continue
                key = target.source + "|version"
                old = self.storage.stream(key)
                events = []
                if old and old["cursor"] != version:
                    events.append(
                        {
                            "uid": f"{now}:{version}",
                            "at": now,
                            "action": "changed",
                            "title": f"{old['cursor']} → {version}",
                            "url": "https://github.com/" + repo if repo else "",
                        }
                    )
                self.storage.commit_stream(
                    key,
                    target.source,
                    "version",
                    old["started"] if old else now,
                    now,
                    version,
                    events,
                )
                self.storage.health(target.source, now)
            except ObserverError as exc:
                self.storage.health(target.source, self.clock(), str(exc))
        return repositories

    async def repository(self, repo, metrics, branches):
        source, path = "gh:" + repo, "/repos/" + repo
        try:
            data, _ = await self.client.get(path)
            if not isinstance(data, dict):
                raise ObserverError("GitHub 仓库接口返回结构不正确。")
            self.storage.put_state(
                source,
                {"default_branch": data.get("default_branch"), "empty": data.get("size") == 0},
            )
            snapshot = {}
            for metric, field in (("stars", "stargazers_count"), ("forks", "forks_count")):
                if metric in metrics:
                    snapshot[metric] = count(data.get(field))
            if metrics & {"issues", "prs"}:
                try:
                    opened, _ = await self.client.listing(path + "/issues", {"state": "open"})
                    for metric, is_pr in (("issues", False), ("prs", True)):
                        if metric in metrics:
                            snapshot[metric] = sum(
                                ("pull_request" in issue) == is_pr for issue in opened
                            )
                    self.storage.health(source + "|open", self.clock())
                except ObserverError as exc:
                    self.storage.health(source + "|open", self.clock(), str(exc))
            self.storage.snapshot(source, self.clock(), snapshot)
            self.storage.health(source, self.clock())
        except ObserverError as exc:
            self.storage.health(source, self.clock(), str(exc))
            return
        if metrics & {"issues", "prs"}:
            try:
                await self.issue_events(repo, metrics & {"issues", "prs"})
                self.storage.health(source + "|activity", self.clock())
            except ObserverError as exc:
                self.storage.health(source + "|activity", self.clock(), str(exc))
        if "release" in metrics:
            try:
                await self.releases(repo)
                self.storage.health(source + "|release", self.clock())
            except ObserverError as exc:
                self.storage.health(source + "|release", self.clock(), str(exc))
        if "commits" in metrics:
            for branch in {requested or data.get("default_branch") for requested in branches}:
                if not isinstance(branch, str) or not branch:
                    self.storage.health(source + "|commits", self.clock(), "仓库缺少默认分支。")
                    continue
                try:
                    await self.commits(repo, branch, data.get("size") == 0)
                    self.storage.health(source + "|commits@" + branch, self.clock())
                except ObserverError as exc:
                    self.storage.health(source + "|commits@" + branch, self.clock(), str(exc))

    async def issue_events(self, repo, metrics):
        source, cutoff = "gh:" + repo, self.clock()
        states = {metric: self.storage.stream(source + "|" + metric) for metric in metrics}
        active = {m: s for m, s in states.items() if s}
        found = {metric: [] for metric in metrics}
        if active:
            since = min(state["checked"] for state in active.values()) - 300
            issues, _ = await self.client.listing(
                "/repos/" + repo + "/issues",
                {
                    "state": "all",
                    "sort": "updated",
                    "direction": "desc",
                    "since": utc_string(since),
                },
            )
            for issue in issues:
                metric = "prs" if "pull_request" in issue else "issues"
                state = active.get(metric)
                if state is None:
                    continue
                number = count(issue.get("number"))
                title, url = str(issue.get("title", "")), str(issue.get("html_url", ""))
                opened = instant(issue.get("created_at"))
                if state["started"] <= opened <= cutoff:
                    found[metric].append(
                        {
                            "uid": f"opened:{number}",
                            "at": opened,
                            "action": "opened",
                            "title": f"#{number} {title}",
                            "url": url,
                        }
                    )
                timeline, _ = await self.client.listing(f"/repos/{repo}/issues/{number}/timeline")
                merged_times = [
                    instant(e.get("created_at")) for e in timeline if e.get("event") == "merged"
                ]
                reopened_times = [
                    instant(e.get("created_at")) for e in timeline if e.get("event") == "reopened"
                ]
                for event in timeline:
                    action = event.get("event")
                    if action not in {"closed", "reopened", "merged"}:
                        continue
                    occurred = instant(event.get("created_at"))
                    if metric == "prs" and action == "closed" and merged_times:
                        merged = min(merged_times)
                        # Automatic merge closure can have a different timestamp.
                        # Earlier unmerged closes are retained if later reopened.
                        if occurred >= merged or not any(
                            occurred <= t <= merged for t in reopened_times
                        ):
                            continue
                    if state["started"] <= occurred <= cutoff:
                        if "id" not in event:
                            raise ObserverError("GitHub 事件缺少 ID，无法可靠去重。")
                        found[metric].append(
                            {
                                "uid": str(event["id"]),
                                "at": occurred,
                                "action": action,
                                "title": f"#{number} {title}",
                                "url": url,
                            }
                        )
        # No cursor moves until every required timeline has been fetched.
        for metric, old in states.items():
            self.storage.commit_stream(
                source + "|" + metric,
                source,
                metric,
                old["started"] if old else cutoff,
                cutoff,
                None,
                found[metric],
            )

    async def releases(self, repo):
        source, now = "gh:" + repo, self.clock()
        key = source + "|release"
        old = self.storage.stream(key)
        events = []
        if old:
            releases, _ = await self.client.listing("/repos/" + repo + "/releases")
            for release in releases:
                if release.get("draft") or not release.get("published_at"):
                    continue
                at = instant(release["published_at"])
                if old["started"] <= at <= now:
                    events.append(
                        {
                            "uid": str(release["id"]),
                            "at": at,
                            "action": "published",
                            "title": ("[预发布] " if release.get("prerelease") else "")
                            + str(release.get("tag_name", ""))
                            + " "
                            + str(release.get("name") or ""),
                            "url": str(release.get("html_url", "")),
                        }
                    )
        self.storage.commit_stream(
            key, source, "release", old["started"] if old else now, now, None, events
        )

    async def commits(self, repo, branch, empty):
        source, now = "gh:" + repo, self.clock()
        metric = "commits@" + branch
        key = source + "|" + metric
        old = self.storage.stream(key)
        try:
            data, _ = await self.client.get(f"/repos/{repo}/branches/{quote(branch, safe='')}")
            head = data["commit"]["sha"]
            if not isinstance(head, str) or not head:
                raise ObserverError("GitHub 未返回有效的分支提交 SHA。")
        except APIError as exc:
            if exc.status == 404 and empty and (not old or old["cursor"] is None):
                self.storage.commit_stream(
                    key, source, metric, old["started"] if old else now, now, None, []
                )
                return
            raise
        except (KeyError, TypeError) as exc:
            raise ObserverError("GitHub 分支接口返回结构不正确。") from exc
        events = []
        if old and old["cursor"] != head:
            if old["cursor"]:
                commits, comparison = await self.client.listing(
                    f"/repos/{repo}/compare/{quote(old['cursor'], safe='')}...{quote(head, safe='')}",
                    field="commits",
                )
                status = comparison.get("status")
                if status not in {"ahead", "identical", "behind", "diverged"}:
                    raise ObserverError("GitHub 提交比较状态无效。")
                if status in {"behind", "diverged"}:
                    events.append(
                        {
                            "uid": f"history:{head}:{now}",
                            "at": now,
                            "action": "history_changed",
                            "title": "分支历史发生回退或改写，新增提交按比较结果统计。",
                            "url": f"https://github.com/{repo}/tree/{quote(branch, safe='')}",
                        }
                    )
            else:
                commits, _ = await self.client.listing(f"/repos/{repo}/commits", {"sha": branch})
            for commit in commits:
                sha, detail = commit.get("sha"), commit.get("commit")
                if not isinstance(sha, str) or not isinstance(detail, dict):
                    raise ObserverError("GitHub 提交记录缺少 SHA 或提交详情。")
                events.append(
                    {
                        "uid": sha,
                        "at": now,
                        "action": "added",
                        "title": sha[:7] + " " + str(detail.get("message", "")).split("\n")[0],
                        "url": str(commit.get("html_url", "")),
                    }
                )
        self.storage.commit_stream(
            key, source, metric, old["started"] if old else now, now, head, events
        )
