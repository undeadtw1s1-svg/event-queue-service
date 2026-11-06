import sqlite3
import json
import time
import threading
from typing import Optional, Dict, Any, Tuple

db_lock = threading.RLock()


def get_connection(db_path: str = "app.db") -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False, timeout=15.0)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str = "app.db") -> None:
    with db_lock:
        with get_connection(db_path) as conn:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT UNIQUE NOT NULL,
                    type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);")

            cur.execute("""
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
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_tasks_queue ON tasks(status, next_retry_at);")


def save_event_and_task(db_path: str, event_id: str, event_type: str, payload: dict) -> Tuple[bool, str]:
    now = time.time()
    serialized = json.dumps(payload, ensure_ascii=False)

    with db_lock:
        conn = get_connection(db_path)
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1 FROM events WHERE event_id = ?", (event_id,))
            if cur.fetchone():
                return True, "already_exists"

            cur.execute(
                "INSERT INTO events (event_id, type, payload, created_at) VALUES (?, ?, ?, ?)",
                (event_id, event_type, serialized, now),
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
    with db_lock:
        with get_connection(db_path) as conn:
            cur = conn.cursor()

            cur.execute("SELECT type, COUNT(*) as cnt FROM events GROUP BY type")
            events_by_type = {row["type"]: row["cnt"] for row in cur.fetchall()}

            cur.execute("SELECT status, COUNT(*) as cnt FROM tasks GROUP BY status")
            counts = {row["status"]: row["cnt"] for row in cur.fetchall()}

            tasks_by_status = {
                "pending": counts.get("pending", 0),
                "success": counts.get("success", 0),
                "failed": counts.get("failed", 0),
                "skipped": counts.get("skipped", 0),
            }

            return {
                "events_by_type": events_by_type,
                "tasks_by_status": tasks_by_status,
            }
