# app.py
import os
from flask import Flask
from dotenv import load_dotenv
from initial_data import check_and_import_data


def create_app():
    load_dotenv()
    app = Flask(__name__)
    app.secret_key = os.getenv("FLASK_SECRET", "default_fallback_secret")

    # Import + register blueprints
    from routes.auth import auth_bp
    from routes.main import main_bp
    from routes.participants import participants_bp
    from routes.events import events_bp
    from routes.imports import imports_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(main_bp)
    app.register_blueprint(participants_bp)
    app.register_blueprint(events_bp)
    app.register_blueprint(imports_bp)

    app.config["UPLOAD_FOLDER"] = os.path.join(os.path.dirname(__file__), "uploads")
    app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16 MB

    # Ensure the folder exists
    os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

    # Initial import (safe: your function already skips if events are present)
    with app.app_context():
        check_and_import_data()

    return app


if __name__ == "__main__":
    app = create_app()
    port = int(os.environ.get("PORT", 5000))  # Read from ENV or default to 5000
    app.run(host="0.0.0.0", port=port, debug=True)
