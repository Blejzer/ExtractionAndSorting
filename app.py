import os
import importlib
import pkgutil

from flask import Blueprint, Flask
from utils.initial_data import check_and_import_data

def create_app() -> Flask:
    """Flask application factory."""
    app = Flask(__name__)
    app.secret_key = os.getenv("SECRET_KEY", "dev")

    # Configure uploads directory for temporary Excel files
    uploads_dir = os.getenv("UPLOADS_DIR", os.path.join(os.getcwd(), "uploads"))
    app.config["UPLOADS_DIR"] = uploads_dir
    os.makedirs(uploads_dir, exist_ok=True)

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
        port=int(getenv("PORT", 5000)),
        debug=getenv("FLASK_DEBUG", "0") == "1",
        use_reloader=False,  # <- important
    )
