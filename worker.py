import time
import json
import random
import threading
from typing import Optional, Callable, Dict, Any
from database import get_connection, db_lock


def default_process_handler(event_type: str, payload: dict) -> None:
    if payload.get("simulate_fail") or event_type == "test_force_fail":
        raise RuntimeError(f"Processing failed for event: {event_type}")

    if event_type == "payment_succeeded":
        if not payload.get("disable_random_fail") and random.random() < 0.15:
            raise ConnectionError("Gateway timeout")
        time.sleep(0.01)
    elif event_type == "user_registered":
        time.sleep(0.01)
    elif event_type == "skip_test":
        return
    else:
        time.sleep(0.01)


def get_backoff_delay(attempts: int) -> float:
    return float(2 ** (attempts - 1))


def process_one_task(db_path: str = "app.db", handler: Optional[Callable[[str, dict], None]] = None) -> Optional[Dict[str, Any]]:
    if handler is None:
        handler = default_process_handler

    now = time.time()

    with db_lock:
        conn = get_connection(db_path)
        cur = conn.cursor()

        cur.execute(
            """
            SELECT t.id, t.event_id, t.event_type, t.attempts, t.status, e.payload
            FROM tasks t
            JOIN events e ON t.event_id = e.event_id
            WHERE t.status = 'pending' AND t.next_retry_at <= ?
            ORDER BY t.id ASC
            LIMIT 1
            """,
            (now,),
        )
        task = cur.fetchone()
        if not task:
            conn.close()
            return None

        task_id = task["id"]
        event_id = task["event_id"]
        event_type = task["event_type"]
        attempts = task["attempts"] + 1
        payload = json.loads(task["payload"])

        try:
            if event_type == "skip_test":
                new_status = "skipped"
                last_error = None
                next_retry = now
            else:
                handler(event_type, payload)
                new_status = "success"
                last_error = None
                next_retry = now
        except BaseException as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempts >= 3:
                new_status = "failed"
                next_retry = now
            else:
                delay = get_backoff_delay(attempts)
                new_status = "pending"
                next_retry = now + delay

        cur.execute(
            """
            UPDATE tasks
            SET status = ?, attempts = ?, next_retry_at = ?, last_error = ?, updated_at = ?
            WHERE id = ?
            """,
            (new_status, attempts, next_retry, last_error, time.time(), task_id),
        )
        conn.commit()
        conn.close()

        return {
            "id": task_id,
            "event_id": event_id,
            "event_type": event_type,
            "attempts": attempts,
            "status": new_status,
            "error": last_error,
        }


class BackgroundWorker:
    def __init__(self, db_path: str = "app.db", poll_interval: float = 0.2, handler: Optional[Callable[[str, dict], None]] = None):
        self.db_path = db_path
        self.poll_interval = poll_interval
        self.handler = handler
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="QueueWorkerThread")
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                task = process_one_task(db_path=self.db_path, handler=self.handler)
                if not task:
                    time.sleep(self.poll_interval)
            except Exception:
                time.sleep(self.poll_interval)
