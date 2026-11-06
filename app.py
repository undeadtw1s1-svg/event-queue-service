import os
from flask import Flask, request, jsonify
from database import init_db, save_event_and_task, get_stats
from worker import BackgroundWorker


def create_app(db_path: str = "app.db", start_worker: bool = True) -> Flask:
    app = Flask(__name__)
    app.config["DB_PATH"] = db_path

    init_db(db_path)

    worker = None
    if start_worker:
        worker = BackgroundWorker(db_path=db_path)
        worker.start()
    app.worker = worker

    @app.route("/webhook", methods=["POST"])
    def webhook():
        if not request.is_json:
            return jsonify({"error": "Content-Type must be application/json"}), 400

        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify({"error": "Malformed JSON payload"}), 400

        event_id = payload.get("event_id")
        event_type = payload.get("type")
        event_data = payload.get("payload")

        if not isinstance(event_id, str) or not event_id.strip():
            return jsonify({"error": "Field 'event_id' is required"}), 400

        if not isinstance(event_type, str) or not event_type.strip():
            return jsonify({"error": "Field 'type' is required"}), 400

        if not isinstance(event_data, dict):
            return jsonify({"error": "Field 'payload' must be an object"}), 400

        _, status = save_event_and_task(
            db_path=app.config["DB_PATH"],
            event_id=event_id.strip(),
            event_type=event_type.strip(),
            payload=event_data,
        )

        if status == "already_exists":
            return jsonify({
                "status": "ok",
                "message": "Событие с таким event_id уже было получено ранее"
            }), 200

        return jsonify({
            "status": "ok",
            "message": "Событие принято и отправлено в очередь"
        }), 200

    @app.route("/stats", methods=["GET"])
    def stats():
        return jsonify(get_stats(app.config["DB_PATH"])), 200

    return app


if __name__ == "__main__":
    db_file = os.getenv("DB_PATH", "app.db")
    app = create_app(db_path=db_file, start_worker=True)
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
