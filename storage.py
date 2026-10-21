"""Small synchronous SQLite transactions with no network waits inside them."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .domain import ObserverError


class Storage:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        version = self.db.execute("PRAGMA user_version").fetchone()[0]
        if version not in {0, 1, 2}:
            self.db.close()
            raise ObserverError(f"不支持的观测站数据库版本：{version}。")
        if version == 1:
            try:
                self.db.execute("PRAGMA foreign_keys=OFF")
                with self.db:
                    self.db.execute("BEGIN IMMEDIATE")
                    sequence = self.db.execute(
                        "SELECT seq FROM sqlite_sequence WHERE name='subscriptions'"
                    ).fetchone()
                    self.db.execute("""CREATE TABLE subscriptions_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT, session TEXT NOT NULL,
                        alias TEXT NOT NULL, periods TEXT NOT NULL, alerts TEXT NOT NULL,
                        created REAL NOT NULL, UNIQUE(session, alias))""")
                    self.db.execute("""INSERT INTO subscriptions_new
                        (id,session,alias,periods,alerts,created)
                        SELECT id,session,alias,periods,alerts,created FROM subscriptions""")
                    self.db.execute("DROP TABLE subscriptions")
                    self.db.execute("ALTER TABLE subscriptions_new RENAME TO subscriptions")
                    if sequence is not None:
                        self.db.execute("DELETE FROM sqlite_sequence WHERE name='subscriptions'")
                        self.db.execute(
                            "INSERT INTO sqlite_sequence(name,seq) VALUES ('subscriptions',?)",
                            (sequence[0],),
                        )
                    if self.db.execute("PRAGMA foreign_key_check").fetchone() is not None:
                        raise ObserverError("观测站数据库升级失败：订阅与推送队列关联异常。")
                    self.db.execute("PRAGMA user_version=2")
            except Exception:
                self.db.close()
                raise
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS snapshots (
                source TEXT NOT NULL, at REAL NOT NULL, data TEXT NOT NULL,
                PRIMARY KEY(source, at));
            CREATE TABLE IF NOT EXISTS streams (
                key TEXT PRIMARY KEY, started REAL NOT NULL, checked REAL NOT NULL,
                cursor TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS events (
                source TEXT NOT NULL, metric TEXT NOT NULL, uid TEXT NOT NULL,
                at REAL NOT NULL, observed REAL NOT NULL, action TEXT NOT NULL,
                title TEXT NOT NULL, url TEXT NOT NULL,
                PRIMARY KEY(source, metric, uid));
            CREATE INDEX IF NOT EXISTS event_time ON events(source, metric, at);
            CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS health (
                key TEXT PRIMARY KEY, ok_at REAL, error_at REAL, error TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT, session TEXT NOT NULL,
                alias TEXT NOT NULL, periods TEXT NOT NULL, alerts TEXT NOT NULL,
                created REAL NOT NULL, UNIQUE(session, alias));
            CREATE TABLE IF NOT EXISTS outbox (
                key TEXT PRIMARY KEY, subscription_id INTEGER NOT NULL,
                text TEXT NOT NULL, ready REAL NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                sent REAL, error TEXT NOT NULL DEFAULT '',
                FOREIGN KEY(subscription_id) REFERENCES subscriptions(id) ON DELETE CASCADE);
            PRAGMA user_version=2;
        """)

    def close(self):
        self.db.close()

    def state(self, key, default=None):
        row = self.db.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def put_state(self, key, value):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO state VALUES (?,?)", (key, json.dumps(value)))

    def health(self, key, at, error=""):
        with self.db:
            if error:
                self.db.execute(
                    """INSERT INTO health VALUES (?,NULL,?,?)
                    ON CONFLICT(key) DO UPDATE SET error_at=excluded.error_at,error=excluded.error""",
                    (key, at, error),
                )
            else:
                self.db.execute(
                    """INSERT INTO health VALUES (?,?,NULL,'')
                    ON CONFLICT(key) DO UPDATE SET ok_at=excluded.ok_at,error=''""",
                    (key, at),
                )

    def snapshot(self, source, at, data):
        with self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO snapshots VALUES (?,?,?)", (source, at, json.dumps(data))
            )

    def samples(self, source, metric, start, end):
        before = self.db.execute(
            """SELECT at,json_extract(data,?) AS value FROM snapshots
            WHERE source=? AND at<=? AND json_type(data,?) IS NOT NULL ORDER BY at DESC LIMIT 1""",
            ("$." + metric, source, start, "$." + metric),
        ).fetchone()
        rows = self.db.execute(
            """SELECT at,json_extract(data,?) AS value FROM snapshots
            WHERE source=? AND at>? AND at<=? AND json_type(data,?) IS NOT NULL ORDER BY at""",
            ("$." + metric, source, start, end, "$." + metric),
        )
        return ([(before["at"], before["value"])] if before else []) + [
            (row["at"], row["value"]) for row in rows
        ]

    def stream(self, key):
        row = self.db.execute("SELECT * FROM streams WHERE key=?", (key,)).fetchone()
        return {**dict(row), "cursor": json.loads(row["cursor"])} if row else None

    def commit_stream(self, key, source, metric, started, checked, cursor, events):
        """Commit events and the high-water mark together, including on retry."""
        with self.db:
            for event in events:
                self.db.execute(
                    "INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?,?,?)",
                    (
                        source,
                        metric,
                        str(event["uid"]),
                        event["at"],
                        checked,
                        event["action"],
                        event["title"],
                        event["url"],
                    ),
                )
            self.db.execute(
                "INSERT OR REPLACE INTO streams VALUES (?,?,?,?)",
                (key, started, checked, json.dumps(cursor)),
            )

    def events(self, source, metric, start, end):
        return [
            dict(row)
            for row in self.db.execute(
                """SELECT * FROM events
            WHERE source=? AND metric=? AND at>=? AND at<? ORDER BY at,uid""",
                (source, metric, start, end),
            )
        ]

    def alert_events(self, source, metric, since, now, created):
        return [
            dict(row)
            for row in self.db.execute(
                """SELECT * FROM events
            WHERE source=? AND metric=? AND observed>=? AND observed<=? AND at>=?
            ORDER BY observed,uid""",
                (source, metric, since, now, created),
            )
        ]

    def subscriptions(self, session=None):
        rows = self.db.execute(
            "SELECT * FROM subscriptions"
            + (" WHERE session=?" if session is not None else "")
            + " ORDER BY id",
            (session,) if session is not None else (),
        )
        return [
            {**dict(row), **{key: json.loads(row[key]) for key in ("periods", "alerts")}}
            for row in rows
        ]

    def subscribe(self, session, alias, periods, alerts, now):
        with self.db:
            self.db.execute(
                "DELETE FROM subscriptions WHERE session=? AND alias=?", (session, alias)
            )
            self.db.execute(
                """INSERT INTO subscriptions
                (session,alias,periods,alerts,created) VALUES (?,?,?,?,?)""",
                (session, alias, json.dumps(periods), json.dumps(alerts), now),
            )

    def unsubscribe(self, session, alias):
        with self.db:
            return self.db.execute(
                "DELETE FROM subscriptions WHERE session=? AND alias=?", (session, alias)
            ).rowcount

    def enqueue(self, key, subscription_id, parts, ready, progress=None):
        with self.db:
            for index, part in enumerate(parts):
                self.db.execute(
                    "INSERT OR IGNORE INTO outbox(key,subscription_id,text,ready) VALUES (?,?,?,?)",
                    (key + f":part:{index}", subscription_id, part, ready),
                )
            if progress is not None:
                self.db.execute(
                    "INSERT OR REPLACE INTO state VALUES (?,?)",
                    (progress[0], json.dumps(progress[1])),
                )

    def pending(self, now, subscription_ids=None, alert_subscription_ids=None):
        condition, values = "", [now]
        if subscription_ids is not None:
            if not subscription_ids:
                return []
            condition = " AND s.id IN (" + ",".join("?" for _ in subscription_ids) + ")"
            values.extend(subscription_ids)
        if alert_subscription_ids is not None:
            if alert_subscription_ids:
                condition += (
                    " AND (o.key NOT LIKE 'event:%' OR s.id IN ("
                    + ",".join("?" for _ in alert_subscription_ids)
                    + "))"
                )
                values.extend(alert_subscription_ids)
            else:
                condition += " AND o.key NOT LIKE 'event:%'"
        return [
            dict(row)
            for row in self.db.execute(
                """SELECT o.*,s.session FROM outbox o
            JOIN subscriptions s ON s.id=o.subscription_id
            WHERE o.sent IS NULL AND o.ready<=?"""
                + condition
                + " ORDER BY o.ready,o.key LIMIT 20",
                values,
            )
        ]

    def delivery(self, key, now, error=""):
        with self.db:
            if error:
                row = self.db.execute("SELECT attempts FROM outbox WHERE key=?", (key,)).fetchone()
                delay = min(3600, 60 * 2 ** min(row[0], 6))
                self.db.execute(
                    "UPDATE outbox SET attempts=attempts+1,ready=?,error=? WHERE key=?",
                    (now + delay, error, key),
                )
            else:
                self.db.execute("UPDATE outbox SET sent=?,error='' WHERE key=?", (now, key))
