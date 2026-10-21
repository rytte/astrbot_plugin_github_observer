import sqlite3

import pytest
from astrbot_plugin_github_observer.domain import ObserverError
from astrbot_plugin_github_observer.storage import Storage


def version_one_database(path):
    with sqlite3.connect(path) as database:
        database.executescript("""
            CREATE TABLE subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT, session TEXT NOT NULL,
                alias TEXT NOT NULL, periods TEXT NOT NULL, metrics TEXT NOT NULL,
                alerts TEXT NOT NULL, created REAL NOT NULL, UNIQUE(session, alias));
            CREATE TABLE outbox (
                key TEXT PRIMARY KEY, subscription_id INTEGER NOT NULL,
                text TEXT NOT NULL, ready REAL NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                sent REAL, error TEXT NOT NULL DEFAULT '',
                FOREIGN KEY(subscription_id) REFERENCES subscriptions(id) ON DELETE CASCADE);
            CREATE TABLE snapshots (
                source TEXT NOT NULL, at REAL NOT NULL, data TEXT NOT NULL,
                PRIMARY KEY(source, at));
            CREATE TABLE state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO subscriptions VALUES
                (7,'group','bot','["day"]','["stars"]','["release"]',100);
            INSERT INTO subscriptions VALUES (99,'deleted','bot','["day"]','[]','[]',100);
            DELETE FROM subscriptions WHERE id=99;
            INSERT INTO outbox VALUES ('report:7:day:100:part:0',7,'queued report',200,2,NULL,'failure');
            INSERT INTO snapshots VALUES ('gh:owner/repo',100,'{"stars":10}');
            INSERT INTO state VALUES ('report-progress:7:day','200');
            PRAGMA user_version=1;
        """)


@pytest.mark.parametrize("empty", [False, True], ids=["subscribed", "empty"])
def test_schema_upgrade_preserves_history_queue_and_subscription_ids(tmp_path, empty):
    path = tmp_path / "observer.sqlite3"
    version_one_database(path)
    if empty:
        with sqlite3.connect(path) as database:
            database.execute("DELETE FROM outbox")
            database.execute("DELETE FROM subscriptions")
    storage = Storage(path)
    try:
        assert storage.db.execute("PRAGMA user_version").fetchone()[0] == 2
        assert "metrics" not in {
            row["name"] for row in storage.db.execute("PRAGMA table_info(subscriptions)")
        }
        assert storage.db.execute("PRAGMA foreign_key_check").fetchall() == []
        assert storage.db.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert storage.samples("gh:owner/repo", "stars", 0, 200) == [(100, 10)]
        assert storage.state("report-progress:7:day") == 200
        if not empty:
            assert storage.subscriptions() == [
                {
                    "id": 7,
                    "session": "group",
                    "alias": "bot",
                    "periods": ["day"],
                    "alerts": ["release"],
                    "created": 100,
                }
            ]
            message = storage.pending(200)[0]
            assert message["text"] == "queued report"
            assert message["attempts"] == 2 and message["error"] == "failure"
            storage.unsubscribe("group", "bot")
            assert storage.pending(200) == []
        storage.subscribe("new-group", "bot", ("week",), (), 300)
        assert storage.subscriptions()[0]["id"] == 100
    finally:
        storage.close()
    storage = Storage(path)
    try:
        assert storage.subscriptions()[0]["id"] == 100
        assert storage.subscriptions()[0]["periods"] == ["week"]
        assert storage.samples("gh:owner/repo", "stars", 0, 200) == [(100, 10)]
    finally:
        storage.close()


def test_schema_upgrade_rolls_back_if_queue_has_missing_subscription(tmp_path):
    path = tmp_path / "observer.sqlite3"
    version_one_database(path)
    with sqlite3.connect(path) as database:
        database.execute("UPDATE outbox SET subscription_id=999")
    with pytest.raises(ObserverError, match="关联异常"):
        Storage(path)
    with sqlite3.connect(path) as database:
        assert database.execute("PRAGMA user_version").fetchone()[0] == 1
        assert "metrics" in {row[1] for row in database.execute("PRAGMA table_info(subscriptions)")}
        assert database.execute("SELECT id FROM subscriptions").fetchone()[0] == 7
        assert database.execute("SELECT subscription_id FROM outbox").fetchone()[0] == 999


def test_unknown_database_version_fails_without_rewriting_it(tmp_path):
    path = tmp_path / "observer.sqlite3"
    with sqlite3.connect(path) as database:
        database.execute("PRAGMA user_version=3")
    with pytest.raises(ObserverError, match="不支持的观测站数据库版本：3"):
        Storage(path)
    with sqlite3.connect(path) as database:
        assert database.execute("PRAGMA user_version").fetchone()[0] == 3
