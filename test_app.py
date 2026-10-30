import pytest
from app import create_app
from database import get_connection
from worker import process_one_task


@pytest.fixture
def temp_db(tmp_path):
    return str(tmp_path / "test_app.db")


@pytest.fixture
def client(temp_db):
    # Для тестов отключаем фоновый поток, дергаем воркер вручную
    app = create_app(db_path=temp_db, start_worker=False)
    app.config["TESTING"] = True
    with app.test_client() as test_client:
        yield test_client, temp_db


def test_webhook_idempotency_and_creation(client):
    test_client, db_path = client

    event_data = {
        "event_id": "pay_test_001",
        "type": "payment_succeeded",
        "payload": {"amount": 500}
    }

    # Первый запрос — создается событие
    r1 = test_client.post("/webhook", json=event_data)
    assert r1.status_code == 200
    assert r1.get_json()["status"] == "ok"

    # Второй запрос с тем же event_id — тоже 200, но дубль не создается
    r2 = test_client.post("/webhook", json=event_data)
    assert r2.status_code == 200
    assert "уже было получено" in r2.get_json()["message"]

    # Проверяем, что в базе ровно по 1 записи
    conn = get_connection(db_path)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM events WHERE event_id = ?", ("pay_test_001",))
    assert cur.fetchone()[0] == 1

    cur.execute("SELECT COUNT(*) FROM tasks WHERE event_id = ?", ("pay_test_001",))
    assert cur.fetchone()[0] == 1
    conn.close()


def test_webhook_validation_errors(client):
    test_client, _ = client

    # Не json
    r = test_client.post("/webhook", data="hello", content_type="text/plain")
    assert r.status_code == 400

    # Нет event_id
    r = test_client.post("/webhook", json={"type": "pay", "payload": {}})
    assert r.status_code == 400
    assert "event_id" in r.get_json()["error"]

    # Пустой event_id
    r = test_client.post("/webhook", json={"event_id": "   ", "type": "pay", "payload": {}})
    assert r.status_code == 400

    # Нет type
    r = test_client.post("/webhook", json={"event_id": "1", "payload": {}})
    assert r.status_code == 400

    # Невалидный payload
    r = test_client.post("/webhook", json={"event_id": "1", "type": "pay", "payload": "not-dict"})
    assert r.status_code == 400


def test_worker_retries_and_failure(client):
    test_client, db_path = client

    # Засылаем задачу с флагом принудительной ошибки
    test_client.post("/webhook", json={
        "event_id": "fail_event",
        "type": "payment_succeeded",
        "payload": {"simulate_fail": True}
    })

    # Попытка 1
    t1 = process_one_task(db_path)
    assert t1["attempts"] == 1
    assert t1["status"] == "pending"

    # Сбрасываем паузу для следующей попытки
    conn = get_connection(db_path)
    conn.execute("UPDATE tasks SET next_retry_at = 0 WHERE event_id = 'fail_event'")
    conn.commit()

    # Попытка 2
    t2 = process_one_task(db_path)
    assert t2["attempts"] == 2
    assert t2["status"] == "pending"

    conn.execute("UPDATE tasks SET next_retry_at = 0 WHERE event_id = 'fail_event'")
    conn.commit()

    # Попытка 3 — после нее задача переходит в failed
    t3 = process_one_task(db_path)
    assert t3["attempts"] == 3
    assert t3["status"] == "failed"

    # Проверяем, что воркер больше не берет ее
    conn.execute("UPDATE tasks SET next_retry_at = 0 WHERE event_id = 'fail_event'")
    conn.commit()
    conn.close()

    assert process_one_task(db_path) is None


def test_stats_endpoint(client):
    test_client, db_path = client

    test_client.post("/webhook", json={
        "event_id": "e1",
        "type": "payment_succeeded",
        "payload": {"disable_random_fail": True}
    })
    test_client.post("/webhook", json={
        "event_id": "e2",
        "type": "payment_succeeded",
        "payload": {"disable_random_fail": True}
    })
    test_client.post("/webhook", json={
        "event_id": "e3",
        "type": "user_signup",
        "payload": {"disable_random_fail": True}
    })

    # Обрабатываем 2 задачи из 3
    process_one_task(db_path)
    process_one_task(db_path)

    res = test_client.get("/stats")
    assert res.status_code == 200
    stats = res.get_json()

    assert stats["events_by_type"]["payment_succeeded"] == 2
    assert stats["events_by_type"]["user_signup"] == 1
    assert stats["tasks_by_status"]["success"] == 2
    assert stats["tasks_by_status"]["pending"] == 1


def test_task_never_stuck_in_pending(client):
    test_client, db_path = client

    test_client.post("/webhook", json={
        "event_id": "crash_event",
        "type": "crash",
        "payload": {}
    })

    def buggy_handler(event_type, payload):
        raise KeyboardInterrupt("Непредвиденное падение")

    # 3 раза падаем
    for _ in range(3):
        conn = get_connection(db_path)
        conn.execute("UPDATE tasks SET next_retry_at = 0 WHERE event_id = 'crash_event'")
        conn.commit()
        conn.close()
        process_one_task(db_path, handler=buggy_handler)

    # Проверяем, что статус стал failed, а не завис в pending
    conn = get_connection(db_path)
    cur = conn.cursor()
    cur.execute("SELECT status, attempts FROM tasks WHERE event_id = 'crash_event'")
    row = cur.fetchone()
    assert row["status"] == "failed"
    assert row["attempts"] == 3
    conn.close()
