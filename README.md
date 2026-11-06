# Event Queue Service

Lightweight webhook ingestion service with an in-process persistent task queue and exponential backoff retries. Built with Flask and SQLite.

## Architecture

1. **Idempotent Ingestion (`POST /webhook`)**:
   - `events.event_id` is constrained with a `UNIQUE` index.
   - Event creation and initial task queuing run in a single transaction.
   - Duplicate `event_id` submissions immediately short-circuit with `200 OK` without creating duplicate jobs.

2. **Task Queue & Background Worker**:
   - Tasks are stored in SQLite (`tasks` table) and processed by an autonomous background worker thread.
   - Safe concurrent execution between Flask request handlers and worker threads is coordinated via reentrant thread synchronization (`threading.RLock`) and database transaction isolation.

3. **Retry Strategy & Backoff**:
   - Failing tasks are retried up to 3 times with exponential backoff:
     - 1st retry: 1s delay
     - 2nd retry: 2s delay
     - 3rd retry: 4s delay
   - Tasks failing after 3 attempts transition to `failed` with captured error details.

---

## Quickstart

### 1. Installation
```bash
pip install -r requirements.txt
```

### 2. Run the Service
```bash
python app.py
```
Starts the HTTP server on `http://127.0.0.1:5000` with the background worker automatically running.

---

## API Reference

### Ingest Event
```bash
curl -X POST http://127.0.0.1:5000/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "tx_98765",
    "type": "payment_succeeded",
    "payload": {"amount": 1000, "currency": "USD"}
  }'
```

Response (`200 OK`):
```json
{
  "status": "ok",
  "message": "Event queued successfully"
}
```

### Metrics & Status
```bash
curl http://127.0.0.1:5000/stats
```

Response (`200 OK`):
```json
{
  "events_by_type": {
    "payment_succeeded": 1
  },
  "tasks_by_status": {
    "failed": 0,
    "pending": 0,
    "skipped": 0,
    "success": 1
  }
}
```

---

## Running Tests

```bash
pytest test_app.py -v
```

Test coverage covers:
- Idempotency guarantees under identical `event_id` deliveries.
- Input validation (missing fields, bad types, empty strings).
- Exponential backoff timings and retry exhaustion (transitioning to `failed`).
- Accurate event and task counter aggregation on `/stats`.
