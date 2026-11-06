import sqlite3
import json
import time
import threading
from typing import Optional, Dict, Any, Tuple

db_lock = threading.RLock()


def get_connection(db_path: str = "app.db") -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False, timeout=20.0)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str = "app.db") -> None:
    schema = """
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT UNIQUE NOT NULL,
        type TEXT NOT NULL,
        payload TEXT NOT NULL,
        created_at REAL NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);

    CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('pending', 'success', 'failed', 'skipped')),
        attempts INTEGER NOT NULL DEFAULT 0,
        next_retry_at REAL NOT NULL DEFAULT 0.0,
        last_error TEXT,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        FOREIGN KEY (event_id) REFERENCES events(event_id)
    );
    CREATE INDEX IF NOT EXISTS idx_tasks_queue ON tasks(status, next_retry_at);
    """
    with db_lock, get_connection(db_path) as conn:
        conn.executescript(schema)


def save_event_and_task(db_path: str, event_id: str, event_type: str, payload: dict) -> Tuple[bool, str]:
    now = time.time()
    payload_str = json.dumps(payload, separators=(',', ':'), ensure_ascii=False)

    with db_lock:
        conn = get_connection(db_path)
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1 FROM events WHERE event_id = ?", (event_id,))
            if cur.fetchone():
                return True, "already_exists"

            cur.execute(
                "INSERT INTO events (event_id, type, payload, created_at) VALUES (?, ?, ?, ?)",
                (event_id, event_type, payload_str, now),
            )
            cur.execute(
                """
                INSERT INTO tasks (event_id, event_type, status, attempts, next_retry_at, created_at, updated_at)
                VALUES (?, ?, 'pending', 0, ?, ?, ?)
                """,
                (event_id, event_type, now, now, now),
            )
            conn.commit()
            return True, "created"
        except sqlite3.IntegrityError:
            conn.rollback()
            return True, "already_exists"
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def get_stats(db_path: str) -> Dict[str, Any]:
    with db_lock, get_connection(db_path) as conn:
        cur = conn.cursor()

        cur.execute("SELECT type, COUNT(1) AS c FROM events GROUP BY type")
        by_type = {r["type"]: r["c"] for r in cur.fetchall()}

        cur.execute("SELECT status, COUNT(1) AS c FROM tasks GROUP BY status")
        status_counts = {r["status"]: r["c"] for r in cur.fetchall()}

        return {
            "events_by_type": by_type,
            "tasks_by_status": {
                "failed": status_counts.get("failed", 0),
                "pending": status_counts.get("pending", 0),
                "skipped": status_counts.get("skipped", 0),
                "success": status_counts.get("success", 0),
            },
        }
