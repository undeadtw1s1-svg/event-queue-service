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
    elif event_type == "skip_test":
        return

    time.sleep(0.01)


def get_backoff_delay(attempts: int) -> float:
    return float(1 << (attempts - 1))


def process_one_task(db_path: str = "app.db", handler: Optional[Callable[[str, dict], None]] = None) -> Optional[Dict[str, Any]]:
    fn = handler or default_process_handler
    now = time.time()

    with db_lock:
        conn = get_connection(db_path)
        cur = conn.cursor()

        cur.execute(
            """
            SELECT t.id, t.event_id, t.event_type, t.attempts, e.payload
            FROM tasks t
            JOIN events e ON t.event_id = e.event_id
            WHERE t.status = 'pending' AND t.next_retry_at <= ?
            ORDER BY t.id ASC
            LIMIT 1
            """,
            (now,),
        )
        row = cur.fetchone()
        if not row:
            conn.close()
            return None

        task_id = row["id"]
        eid = row["event_id"]
        etype = row["event_type"]
        attempts = row["attempts"] + 1
        payload = json.loads(row["payload"])

        err_msg = None
        if etype == "skip_test":
            state, retry_ts = "skipped", now
        else:
            try:
                fn(etype, payload)
                state, retry_ts = "success", now
            except BaseException as exc:
                err_msg = f"{exc.__class__.__name__}: {exc}"
                if attempts >= 3:
                    state, retry_ts = "failed", now
                else:
                    state = "pending"
                    retry_ts = now + get_backoff_delay(attempts)

        cur.execute(
            """
            UPDATE tasks
            SET status = ?, attempts = ?, next_retry_at = ?, last_error = ?, updated_at = ?
            WHERE id = ?
            """,
            (state, attempts, retry_ts, err_msg, time.time(), task_id),
        )
        conn.commit()
        conn.close()

        return {
            "id": task_id,
            "event_id": eid,
            "event_type": etype,
            "attempts": attempts,
            "status": state,
            "error": err_msg,
        }


class BackgroundWorker:
    def __init__(self, db_path: str = "app.db", poll_interval: float = 0.2, handler: Optional[Callable[[str, dict], None]] = None):
        self.db_path = db_path
        self.poll_interval = poll_interval
        self.handler = handler
        self._stopper = threading.Event()
        self._th: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._th and self._th.is_alive():
            return
        self._stopper.clear()
        self._th = threading.Thread(target=self._worker_loop, daemon=True, name="EventQueueWorker")
        self._th.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stopper.set()
        if self._th:
            self._th.join(timeout=timeout)

    def _worker_loop(self) -> None:
        while not self._stopper.is_set():
            try:
                res = process_one_task(self.db_path, self.handler)
                if not res:
                    time.sleep(self.poll_interval)
            except Exception:
                time.sleep(self.poll_interval)
