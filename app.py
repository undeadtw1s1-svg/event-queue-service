from flask import Flask, request, jsonify
from database import init_db, save_event_and_task, get_stats
from worker import BackgroundWorker


def create_app(db_path: str = "app.db", start_worker: bool = True) -> Flask:
    app = Flask(__name__)
    app.config["DB_PATH"] = db_path

    # Создаем таблицы, если их еще нет
    init_db(db_path)

    worker = None
    if start_worker:
        worker = BackgroundWorker(db_path=db_path)
        worker.start()
    app.worker = worker

    @app.route("/webhook", methods=["POST"])
    def webhook():
        if not request.is_json:
            return jsonify({"error": "Ожидается JSON (Content-Type: application/json)"}), 400

        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify({"error": "Тело запроса должно быть JSON-объектом"}), 400

        event_id = data.get("event_id")
        event_type = data.get("type")
        payload = data.get("payload")

        # Базовая валидация входящих полей
        if not event_id or not isinstance(event_id, str) or not event_id.strip():
            return jsonify({"error": "Поле 'event_id' обязательно и не должно быть пустым"}), 400

        if not event_type or not isinstance(event_type, str) or not event_type.strip():
            return jsonify({"error": "Поле 'type' обязательно и не должно быть пустым"}), 400

        if payload is None or not isinstance(payload, dict):
            return jsonify({"error": "Поле 'payload' обязательно и должно быть объектом"}), 400

        current_db = app.config["DB_PATH"]
        _, state = save_event_and_task(
            db_path=current_db,
            event_id=event_id.strip(),
            event_type=event_type.strip(),
            payload=payload
        )

        # Если такой event_id уже был — отвечаем 200, дубль не создаем
        if state == "already_exists":
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
        current_db = app.config["DB_PATH"]
        data = get_stats(current_db)
        return jsonify(data), 200

    return app


if __name__ == "__main__":
    app = create_app(db_path="app.db", start_worker=True)
    app.run(host="127.0.0.1", port=5000)
