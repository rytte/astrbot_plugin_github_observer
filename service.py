"""Calendar reports, per-conversation subscriptions and a durable outbox."""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from datetime import date, datetime

from .collector import Collector
from .domain import (
    ALERTS,
    BEIJING,
    LABELS,
    MARKET_METRICS,
    PERIODS,
    ObserverError,
    period,
    selections,
    stamp,
)

ACTION_NAMES = {
    "opened": "新增",
    "closed": "关闭未合并",
    "reopened": "重新打开",
    "merged": "合并",
    "published": "发布",
    "added": "新增提交",
    "changed": "版本变更",
    "history_changed": "分支历史变更",
}
METRIC_ICONS = {
    "downloads": "📥",
    "download_rank": "🏆",
    "stars": "⭐",
    "forks": "🍴",
    "commits": "📝",
    "release": "📦",
    "version": "🏷️",
    "issues": "🐛",
    "prs": "🔀",
}


def chunks(text, limit=2800):
    """Split messages deterministically so each outbox item is one send."""
    pending = ""
    for line in text.splitlines(keepends=True):
        while line:
            remaining = limit - len(pending)
            pending += line[:remaining]
            line = line[remaining:]
            if len(pending) == limit:
                yield pending
                pending = ""
    if pending:
        yield pending


def line(value):
    return " ".join(str(value).split())[:500]


class Service:
    def __init__(self, settings, storage, client, sender, clock=time.time):
        self.settings, self.storage, self.client = settings, storage, client
        self.sender, self.clock = sender, clock
        self.collector = Collector(client, storage, clock)
        self.targets = {target.alias: target for target in settings.targets}
        self.lock = asyncio.Lock()
        self.next_poll = 0
        self.last_day = None
        self.needs_collection = True

    def target(self, alias):
        alias = alias.lower() if "/" in alias else alias
        if alias not in self.targets:
            raise ObserverError("目标未配置，请先在插件配置页面添加目标。")
        return self.targets[alias]

    def location(self, target, metric):
        if target.kind == "market":
            if metric in MARKET_METRICS:
                return target.source, metric
            repo = (
                self.storage.state(target.source, {}).get("repo") if target.github_enabled else None
            )
            if not repo:
                return None, metric
        else:
            repo = target.target
        source = "gh:" + repo
        if metric == "commits":
            branch = target.branch or self.storage.state(source, {}).get("default_branch")
            if not branch:
                return None, metric
            return source, "commits@" + branch
        return source, metric

    async def collect(self):
        if self.lock.locked():
            raise ObserverError("采集任务正在进行，请稍后查看状态。")
        async with self.lock:
            resolved = {}
            markets = {
                target.source: target for target in self.targets.values() if target.kind == "market"
            }
            if markets:
                market_metrics = {source: set() for source in markets}
                for target in self.targets.values():
                    if target.kind == "market":
                        market_metrics[target.source].update(set(target.metrics) & MARKET_METRICS)
                resolved = await self.collector.market(list(markets.values()), market_metrics)
            repos = {}
            for target in self.targets.values():
                if target.kind == "market":
                    if not target.github_enabled or target.source not in resolved:
                        continue
                    repo, error = resolved[target.source]
                    self.storage.health(target.source + "|github", self.clock(), error)
                    metrics = set(target.metrics) - MARKET_METRICS
                else:
                    repo, metrics = target.target, set(target.metrics)
                if repo and metrics:
                    item = repos.setdefault(repo, {"metrics": set(), "branches": set()})
                    item["metrics"].update(metrics)
                    if "commits" in metrics:
                        item["branches"].add(target.branch)
            for repo, item in repos.items():
                try:
                    await self.collector.repository(repo, item["metrics"], item["branches"])
                except (KeyError, TypeError, ValueError):
                    # Malformed remote records must not stop other repositories.
                    self.storage.health(
                        "gh:" + repo, self.clock(), "GitHub 返回数据结构异常，本次采集未完成。"
                    )

    def report(self, targets, kind, anchor, now=None):
        """Render one calendar report with shared context and separate target sections."""
        targets = tuple(targets)
        if not targets:
            return "尚无监看目标。"
        now = self.clock() if now is None else now
        start, boundary = period(kind, anchor)
        if start > now:
            raise ObserverError("不能查询尚未开始的周期。")
        end = min(boundary, now)
        title = {"day": "日报", "week": "周报", "month": "月报"}[kind]
        sections = [self._report_target(target, start, end) for target in targets]
        sampled = sorted({stamp(at) for _, at, _ in sections if at is not None})
        sample_time = "本周期无可用采样"
        if sampled:
            sample_time = sampled[0] if len(sampled) == 1 else f"{sampled[0]} ～ {sampled[-1]}"
        header = [
            f"GitHub 观测站 · {title}",
            f"{stamp(start)[:10]} ～ {stamp(boundary - 1)[:10]}｜北京时间"
            + ("｜统计中" if boundary > now else ""),
            "🕒 采样时间：" + sample_time,
        ]
        shared_notices = [
            notice
            for notice in sections[0][2]
            if all(notice in notices for _, _, notices in sections)
        ]
        if shared_notices:
            header.append("⚠️ 注意：" + "；".join(shared_notices))
        if any("commits" in target.metrics for target in targets):
            header.append("提交按首次观测到进入所选分支的时间归入周期，首次采集仅建立基线。")
        parts = ["\n".join(header)]
        for lines, _, notices in sections:
            specific = [notice for notice in notices if notice not in shared_notices]
            if specific:
                lines.append("⚠️ 注意：" + "；".join(specific))
            parts.append("\n".join(lines))
        return "\n\n".join(parts)

    def _report_target(self, target, start, end):
        """Collect a target's metric lines, latest sample and completeness notices."""
        metrics = target.metrics
        lines = [f"{target.alias}｜{target.target}"]
        sample_times, notices = [], []
        for metric in metrics:
            label = f"{METRIC_ICONS[metric]} {LABELS[metric]}"
            source, stored_metric = self.location(target, metric)
            if source is None:
                lines.append(f"{label}：尚未解析仓库或分支。")
                continue
            if metric in {"downloads", "download_rank", "stars", "forks"}:
                samples = self.storage.samples(source, stored_metric, start, end)
                if not samples or samples[-1][0] < start:
                    lines.append(f"{label}：本周期无可用采样。")
                    continue
                first, last = samples[0], samples[-1]
                sample_times.append(last[0])
                if len(samples) < 2:
                    delta = "⏳ 待积累，只有一次采样"
                elif metric == "download_rank":
                    change = first[1] - last[1]
                    delta = (
                        f"⬆️{change} 名" if change > 0 else f"⬇️{-change} 名" if change < 0 else "➖"
                    )
                elif metric == "downloads" and any(
                    b[1] < a[1] for a, b in zip(samples, samples[1:])
                ):
                    delta = "⚠️ 计数下降，增量不可用"
                else:
                    change = last[1] - first[1]
                    delta = f"⬆️{change}" if change > 0 else f"⬇️{-change}" if change < 0 else "➖"
                current = f"第 {last[1]:,} 名" if metric == "download_rank" else f"{last[1]:,}"
                lines.append(f"{label}：{current}｜{delta}")
                interval = self.settings.poll_minutes * 60
                if first[0] > start:
                    notices.append("本周期记录不完整，统计仅覆盖已记录时段")
                elif start - first[0] > interval * 1.5:
                    notices.append("期初基线过旧")
                if end - last[0] > interval * 1.5:
                    notices.append("期末采样缺失或过旧")
                if any(b[0] - a[0] > interval * 1.5 for a, b in zip(samples, samples[1:])):
                    notices.append("采样存在间断")
                continue
            events = self.storage.events(source, stored_metric, start, end)
            stream = self.storage.stream(source + "|" + stored_metric)
            if metric in {"issues", "prs"}:
                samples = self.storage.samples(source, stored_metric, start, end)
                if samples and samples[-1][0] >= start:
                    opened = f"未关闭 {samples[-1][1]}"
                    sample_times.append(samples[-1][0])
                else:
                    opened = "未关闭数量不可用"
                counts = Counter(event["action"] for event in events)
                if stream is None:
                    lines.append(f"{label}：{opened}｜事件尚未建立采集基线")
                    continue
                detail = f"新增 {counts['opened']}｜关闭 {counts['closed']}｜重新打开 {counts['reopened']}"
                if metric == "prs":
                    detail = detail.replace("关闭 ", "关闭未合并 ") + f"｜合并 {counts['merged']}"
                lines.append(f"{label}：{opened}")
                lines.append("  " + detail)
            elif metric == "version":
                samples = self.storage.samples(source, "version", start, end)
                current = samples[-1][1] if samples else "不可用"
                if samples and samples[-1][0] >= start:
                    sample_times.append(samples[-1][0])
                changes = f"观测到变更 {len(events)} 次" if stream else "尚未建立采集基线"
                lines.append(f"{label}：{line(current)}｜{changes}")
            else:
                if metric == "commits":
                    label += "（" + stored_metric.removeprefix("commits@") + "）"
                amount = sum(
                    event["action"] == ("added" if metric == "commits" else "published")
                    for event in events
                )
                lines.append(f"{label}：" + (f"{amount} 条" if stream else "尚未建立采集基线"))
            if stream:
                if stream["started"] > start:
                    notices.append("本周期记录不完整，统计仅覆盖已记录时段")
                if start <= stream["checked"] <= end:
                    sample_times.append(stream["checked"])
                if stream["checked"] < end - self.settings.poll_minutes * 90:
                    notices.append("事件采集进度落后，结果可能不完整")
            for event in events[-3:]:
                action = (
                    "关闭"
                    if metric == "issues" and event["action"] == "closed"
                    else ACTION_NAMES.get(event["action"], event["action"])
                )
                lines.append(f"  {action}：{line(event['title'])}")
                if event["url"]:
                    lines.append("  " + event["url"])
        errors = self.errors(target)
        if errors:
            lines.append("⚠️ 当前采集告警：" + "；".join(errors))
        return lines, max(sample_times) if sample_times else None, list(dict.fromkeys(notices))

    def errors(self, target):
        sources = {target.source}
        for metric in target.metrics:
            source, _ = self.location(target, metric)
            if source:
                sources.add(source)
        errors = []
        for row in self.storage.db.execute(
            "SELECT key,error FROM health WHERE error<>'' ORDER BY key"
        ):
            if row["key"] == target.source + "|github" and not target.github_enabled:
                continue
            if row["key"].split("|", 1)[0] in sources and row["error"] not in errors:
                errors.append(row["error"])
        return errors

    def alert_notice(self, target, alerts):
        disabled = set(alerts) - (set(target.metrics) & ALERTS)
        if not disabled:
            return ""
        names = "、".join(LABELS.get(metric, metric) for metric in sorted(disabled))
        return (
            f"事件提醒已暂停：目标配置未启用提醒指标 {names}；周期报告继续，请修改配置或重新订阅。"
        )

    def subscribe(self, session, alias, periods, alerts=()):
        target = self.target(alias)
        if not periods or set(periods) - set(PERIODS.values()):
            raise ObserverError("订阅周期必须是日报、周报、月报。")
        if set(alerts) - (set(target.metrics) & ALERTS):
            raise ObserverError(
                "提醒只能选择目标配置中已启用的 version、release、issues、prs、commits；"
                "请先在插件配置页面勾选对应指标。"
            )
        self.storage.subscribe(session, target.alias, periods, alerts, self.clock())
        self.needs_collection = True

    def queue(self, key, subscription, text, ready, progress=None):
        self.storage.enqueue(key, subscription["id"], list(chunks(text)), ready, progress)

    def schedule(self):
        now = self.clock()
        hour, minute = map(int, self.settings.push_time.split(":"))
        offset = hour * 3600 + minute * 60
        for sub in self.storage.subscriptions():
            target = self.targets.get(sub["alias"])
            if target is None:
                continue
            for kind in sub["periods"]:
                key = f"report-progress:{sub['id']}:{kind}"
                cursor = self.storage.state(key)
                if cursor is None:
                    cursor = period(kind, datetime.fromtimestamp(sub["created"], BEIJING).date())[0]
                # Bound work per tick; later ticks continue from this durable cursor.
                for _ in range(20):
                    anchor = datetime.fromtimestamp(cursor, BEIJING).date()
                    start, end = period(kind, anchor)
                    if end + offset > now:
                        break
                    report = self.report((target,), kind, anchor, now)
                    if sub["created"] > start:
                        report += "\n本订阅于周期中途建立：" + stamp(sub["created"])
                    self.queue(
                        f"report:{sub['id']}:{kind}:{int(start)}",
                        sub,
                        report,
                        end + offset,
                        progress=(key, end),
                    )
                    cursor = end
            if not sub["alerts"] or self.alert_notice(target, sub["alerts"]):
                continue
            alert_key = f"alert-progress:{sub['id']}"
            since = self.storage.state(alert_key, sub["created"])
            for metric in sub["alerts"]:
                source, stored_metric = self.location(target, metric)
                if not source:
                    continue
                for event in self.storage.alert_events(
                    source, stored_metric, since, now, sub["created"]
                ):
                    action = (
                        "关闭"
                        if metric == "issues" and event["action"] == "closed"
                        else ACTION_NAMES.get(event["action"], event["action"])
                    )
                    text = (
                        f"GitHub 观测站 · {target.alias}\n{LABELS[metric]} · {action}\n"
                        f"{line(event['title'])}\n{event['url']}\n{stamp(event['at'])}（北京时间）"
                    )
                    self.queue(
                        f"event:{sub['id']}:{source}:{stored_metric}:{event['uid']}", sub, text, now
                    )
            self.storage.put_state(alert_key, now)

    async def deliver(self):
        active, alerts = [], []
        for sub in self.storage.subscriptions():
            target = self.targets.get(sub["alias"])
            if target is None:
                continue
            active.append(sub["id"])
            if sub["alerts"] and not self.alert_notice(target, sub["alerts"]):
                alerts.append(sub["id"])
        for message in self.storage.pending(self.clock(), active, alerts):
            try:
                result = await self.sender(message["session"], message["text"])
                if result is False:
                    raise ObserverError("找不到可发送消息的平台，或平台不支持主动消息。")
            except Exception:
                self.storage.delivery(
                    message["key"],
                    self.clock(),
                    "推送失败，将自动重试；请检查平台状态和主动消息权限。",
                )
            else:
                self.storage.delivery(message["key"], self.clock())

    async def tick(self):
        now = self.clock()
        day = datetime.fromtimestamp(now, BEIJING).date()
        if self.needs_collection or now >= self.next_poll or day != self.last_day:
            if self.lock.locked():
                self.needs_collection = True
            else:
                self.needs_collection = False
                interval = self.settings.poll_minutes * 60
                self.next_poll = (int(now) // interval + 1) * interval
                self.last_day = day
                await self.collect()
        self.schedule()
        await self.deliver()

    def status(self, session):
        lines = [
            "GitHub 观测站",
            "采集中" if self.lock.locked() else "等待下次采集",
            f"采集间隔：{self.settings.poll_minutes} 分钟；周期推送：北京时间 {self.settings.push_time}",
        ]
        if not self.targets:
            lines.append("尚无目标，请在插件配置页面添加市场插件或 GitHub 仓库。")
        for target in self.targets.values():
            health = self.storage.db.execute(
                "SELECT ok_at FROM health WHERE key=?", (target.source,)
            ).fetchone()
            last = stamp(health["ok_at"]) if health and health["ok_at"] else "尚未成功采集"
            lines.append(f"{target.alias}｜{target.target}｜最近成功：{last}")
            if target.kind == "market" and target.github_enabled:
                repo = self.storage.state(target.source, {}).get("repo")
                lines.append("  关联 GitHub 仓库：" + (repo or "尚未解析"))
            for error in self.errors(target):
                lines.append("  告警：" + error)
        for sub in self.storage.subscriptions(session):
            target = self.targets.get(sub["alias"])
            if target and (notice := self.alert_notice(target, sub["alerts"])):
                lines.append(f"{target.alias}｜{notice}")
        pending = self.storage.db.execute(
            """SELECT COUNT(*) FROM outbox o JOIN subscriptions s
            ON s.id=o.subscription_id WHERE s.session=? AND o.sent IS NULL""",
            (session,),
        ).fetchone()[0]
        lines.append(f"当前会话待推送消息：{pending}")
        for row in self.storage.db.execute(
            """SELECT DISTINCT error FROM outbox o JOIN subscriptions s
            ON s.id=o.subscription_id WHERE s.session=? AND o.sent IS NULL AND o.error<>''""",
            (session,),
        ):
            lines.append(row["error"])
        return "\n".join(lines)

    def command(self, session, tokens):
        """Dispatch local commands; refresh is handled by the async adapter."""
        if not tokens:
            return HELP
        action, args = tokens[0], tokens[1:]
        if action == "帮助" and not args:
            return HELP
        if action == "列表" and not args:
            return "\n".join(
                ["GitHub 观测站 · 目标列表"]
                + [
                    f"{t.alias}｜{'市场插件' if t.kind == 'market' else 'GitHub'}｜{t.target}\n  监看指标：{','.join(t.metrics)}"
                    for t in self.targets.values()
                ]
            )
        if action == "状态" and not args:
            return self.status(session)
        if action == "订阅列表" and not args:
            lines = ["GitHub 观测站 · 当前会话订阅"]
            for sub in self.storage.subscriptions(session):
                target = self.targets.get(sub["alias"])
                periods = ",".join({v: k for k, v in PERIODS.items()}[p] for p in sub["periods"])
                text = f"{sub['alias']}｜周期：{periods}｜提醒：{','.join(sub['alerts']) or '无'}"
                if target is None:
                    text += "｜目标已移除，暂停推送"
                else:
                    text += "｜指标：" + ",".join(target.metrics)
                    if notice := self.alert_notice(target, sub["alerts"]):
                        text += "｜" + notice
                lines.append(text)
            return "\n".join(lines)
        if action == "退订" and len(args) == 1:
            reference = args[0].lower() if "/" in args[0] else args[0]
            removed = self.storage.unsubscribe(session, reference)
            return "已取消当前会话对该目标的订阅。" if removed else "当前会话没有该目标的订阅。"
        if action == "订阅" and len(args) >= 2:
            alias, periods_text, *options = args
            periods = tuple(PERIODS[p] for p in selections(periods_text, set(PERIODS)))
            alerts = ()
            if options:
                if len(options) != 2 or options[0] != "--提醒":
                    raise ObserverError("订阅选项仅支持 --提醒 提醒指标列表，且只能出现一次。")
                alerts = selections(options[1], ALERTS)
            self.subscribe(session, alias, periods, alerts)
            return f"已订阅 {alias}：{periods_text}。周期报告在北京时间 {self.settings.push_time} 推送；再次订阅将覆盖当前会话的该目标设置，从现在生效。"
        if action in PERIODS and 0 <= len(args) <= 2:
            anchor = datetime.fromtimestamp(self.clock(), BEIJING).date()
            selected = list(self.targets.values())
            if args:
                selected = [self.target(args[0])]
            if len(args) == 2:
                try:
                    anchor = date.fromisoformat(args[1])
                    if anchor.isoformat() != args[1]:
                        raise ValueError("Non-canonical date")
                except ValueError as exc:
                    raise ObserverError(
                        "日期必须使用 YYYY-MM-DD；周报取该日期所在周，月报取所在月。"
                    ) from exc
            return self.report(selected, PERIODS[action], anchor)
        if action == "动态" and len(args) in {1, 2}:
            target = self.target(args[0])
            limit = 10
            if len(args) == 2:
                if not args[1].isdigit() or not 1 <= int(args[1]) <= 50:
                    raise ObserverError("动态条数须为 1～50。")
                limit = int(args[1])
            events = []
            for metric in target.metrics:
                if metric not in ALERTS:
                    continue
                source, stored = self.location(target, metric)
                if source:
                    events.extend(
                        self.storage.db.execute(
                            """SELECT * FROM events WHERE source=? AND metric=?
                        ORDER BY at DESC LIMIT ?""",
                            (source, stored, limit),
                        ).fetchall()
                    )
            events.sort(key=lambda e: e["at"], reverse=True)
            return "\n".join(
                [f"GitHub 观测站 · {target.alias} · 最近动态"]
                + [
                    f"{stamp(e['at'])}｜{'关闭' if e['metric'] == 'issues' and e['action'] == 'closed' else ACTION_NAMES.get(e['action'], e['action'])}\n{line(e['title'])}\n{e['url']}"
                    for e in events[:limit]
                ]
            )
        raise ObserverError("命令格式不正确，请使用 /观测站 帮助 查看用法。")


HELP = """GitHub 观测站（仅 AstrBot 管理员）
/观测站 列表
/观测站 状态
/观测站 刷新
/观测站 日报 [目标标识] [YYYY-MM-DD]
/观测站 周报 [目标标识] [YYYY-MM-DD]
/观测站 月报 [目标标识] [YYYY-MM-DD]
/观测站 动态 <目标标识> [1～50条]
/观测站 订阅 <目标标识> <日报,周报,月报> [--提醒 提醒指标列表]
/观测站 订阅列表
/观测站 退订 <目标标识>
目标标识：填写了 alias 则使用别名，未填写则使用市场 作者/插件名 或 GitHub owner/repo。
市场基础指标：downloads、download_rank、stars；附加指标：version
下载排名按全市场累计下载量排序，同下载量并列（1、1、3）；周期报告显示期末排名及采样期间升降。
仓库基础指标：stars、forks；附加指标：commits、release、issues、prs
市场目标开启关联 GitHub 仓库监看后，自动关联市场中的仓库地址，补充 Fork 和勾选的仓库附加指标；Star 继续使用市场数据。
提醒指标：version,release,issues,prs,commits；必须已在目标配置中启用，不填 --提醒 则不提醒。
例：/观测站 订阅 AstrBot 周报,月报 --提醒 release
采集、报表和动态统一使用目标配置中启用的全部指标，配置调整后自动跟随。
目标在插件配置页添加；首次采集建立基线，历史不会计为当天新增。
周一至周日、自然月，全部按北京时间统计。"""
