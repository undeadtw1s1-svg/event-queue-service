import os
import logging
from flask import Flask, request, jsonify
from database import init_db, save_event_and_task, get_stats
from worker import BackgroundWorker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _validate_payload(raw):
    if not isinstance(raw, dict):
        return None, "Malformed JSON payload"
    eid = raw.get("event_id")
    etype = raw.get("type")
    pdata = raw.get("payload")

    if not eid or not isinstance(eid, str) or not eid.strip():
        return None, "Field 'event_id' is required"
    if not etype or not isinstance(etype, str) or not etype.strip():
        return None, "Field 'type' is required"
    if not isinstance(pdata, dict):
        return None, "Field 'payload' must be an object"

    return (eid.strip(), etype.strip(), pdata), None


def create_app(db_path: str = "app.db", start_worker: bool = True) -> Flask:
    app = Flask(__name__)
    app.config["DB_PATH"] = db_path

    init_db(db_path)

    app.worker = None
    if start_worker:
        app.worker = BackgroundWorker(db_path=db_path)
        app.worker.start()

    @app.post("/webhook")
    def handle_webhook():
        if not request.is_json:
            return jsonify({"error": "Content-Type must be application/json"}), 400

        data, err = _validate_payload(request.get_json(silent=True))
        if err:
            return jsonify({"error": err}), 400

        eid, etype, payload = data
        _, res_state = save_event_and_task(app.config["DB_PATH"], eid, etype, payload)

        if res_state == "already_exists":
            logger.info("Duplicate event ignored: %s", eid)
            return jsonify({
                "status": "ok",
                "message": "Событие с таким event_id уже было получено ранее"
            }), 200

        return jsonify({
            "status": "ok",
            "message": "Событие принято и отправлено в очередь"
        }), 200

    @app.get("/stats")
    def handle_stats():
        return jsonify(get_stats(app.config["DB_PATH"])), 200

    return app


if __name__ == "__main__":
    db = os.getenv("DATABASE_URL", "app.db")
    port = int(os.getenv("PORT", 5000))
    app = create_app(db_path=db, start_worker=True)
    app.run(host="0.0.0.0", port=port)
