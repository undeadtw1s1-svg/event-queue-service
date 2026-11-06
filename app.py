import os
import logging
from flask import Flask, request, jsonify, Blueprint, current_app
from database import init_db, save_event_and_task, get_stats
from worker import BackgroundWorker

log = logging.getLogger("event_queue")

api_bp = Blueprint("api", __name__)


class WebhookValidator:
    REQUIRED_STRINGS = ("event_id", "type")

    @classmethod
    def check(cls, body: dict):
        if not isinstance(body, dict):
            return "Malformed JSON payload"
        for field in cls.REQUIRED_STRINGS:
            val = body.get(field)
            if not val or not isinstance(val, str) or not val.strip():
                return f"Field '{field}' is required"
        if not isinstance(body.get("payload"), dict):
            return "Field 'payload' must be an object"
        return None


@api_bp.post("/webhook")
def receive_webhook():
    if not request.is_json:
        return jsonify({"error": "Content-Type must be application/json"}), 400

    payload = request.get_json(silent=True)
    err = WebhookValidator.check(payload)
    if err:
        return jsonify({"error": err}), 400

    db_path = current_app.config["DB_PATH"]
    _, res = save_event_and_task(
        db_path,
        payload["event_id"].strip(),
        payload["type"].strip(),
        payload["payload"]
    )

    if res == "already_exists":
        log.debug("Event %s dropped (idempotent duplicate)", payload["event_id"])
        return jsonify({
            "status": "ok",
            "message": "Событие с таким event_id уже было получено ранее"
        }), 200

    return jsonify({
        "status": "ok",
        "message": "Событие принято и отправлено в очередь"
    }), 200


@api_bp.get("/stats")
def fetch_stats():
    db_path = current_app.config["DB_PATH"]
    return jsonify(get_stats(db_path)), 200


def create_app(db_path: str = "app.db", start_worker: bool = True) -> Flask:
    app = Flask("event_service")
    app.config["DB_PATH"] = db_path

    init_db(db_path)
    app.register_blueprint(api_bp)

    app.worker = None
    if start_worker:
        app.worker = BackgroundWorker(db_path=db_path)
        app.worker.start()

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app = create_app(os.getenv("DB_PATH", "app.db"))
    app.run(host="127.0.0.1", port=5000, debug=False)
