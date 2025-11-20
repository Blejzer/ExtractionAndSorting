import os
import importlib
import pkgutil

from flask import Blueprint, Flask
from repositories.participant_repository import ParticipantRepository
from utils.initial_data import check_and_import_data
from utils.participants import initialize_cache

def create_app() -> Flask:
    """Flask application factory."""
    app = Flask(__name__)
    app.secret_key = os.getenv("SECRET_KEY", "dev")

    """Registration of error handlers."""
    from middleware.handlers import register_error_handlers
    register_error_handlers(app)

    # Allow test suites to bypass authentication without modifying middleware.
    if os.getenv("PYTEST_CURRENT_TEST"):
        app.config.setdefault("LOGIN_DISABLED", True)


    # Configure uploads directory for temporary Excel files
    uploads_dir = os.getenv("UPLOADS_DIR", os.path.join(os.getcwd(), "uploads"))
    app.config["UPLOADS_DIR"] = uploads_dir
    os.makedirs(uploads_dir, exist_ok=True)

    # Initialize participant cache for cross-user reuse
    try:
        initialize_cache(ParticipantRepository())
    except Exception:
        # Continue startup even if the repository is unavailable (e.g., tests)
        initialize_cache(None)

    # Auto-register all blueprints defined in routes/*.py
    from routes import __path__ as routes_path

    for _, module_name, _ in pkgutil.iter_modules(routes_path):
        module = importlib.import_module(f"routes.{module_name}")
        for attr_name in dir(module):
            obj = getattr(module, attr_name)
            if isinstance(obj, Blueprint):
                app.register_blueprint(obj)

    # Initial import (safe: your function already skips if events are present)
    with app.app_context():
        check_and_import_data()
    return app


# at bottom of app.py
if __name__ == "__main__":
    from os import getenv

    app = create_app()
    app.run(
        host="0.0.0.0",
        port=int(getenv("PORT", 443)),
        debug=getenv("FLASK_DEBUG", "0") == "1",
        use_reloader=False,  # <- important
        ssl_context=(getenv("CERT_PATH"), getenv("KEY_PATH"))
    )
