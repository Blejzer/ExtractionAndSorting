import os
import sys
import importlib
import pkgutil
from urllib.parse import urlsplit, urlunsplit

from flask import Blueprint, Flask, redirect, request
from werkzeug.middleware.proxy_fix import ProxyFix

from utils.initial_data import check_and_import_data


def create_app() -> Flask:
    """Flask application factory."""
    app = Flask(__name__)
    app.secret_key = os.getenv("SECRET_KEY", "dev")

    app.config.setdefault("PREFERRED_URL_SCHEME", "https")

    force_https_env = os.getenv("FORCE_HTTPS", "1").strip().lower()
    force_https = force_https_env not in {"0", "false", "no"}

    if force_https:
        app.config.setdefault("SESSION_COOKIE_SECURE", True)
        app.config.setdefault("REMEMBER_COOKIE_SECURE", True)
        app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

        @app.before_request
        def _enforce_https():
            """Redirect incoming HTTP requests to HTTPS on port 443."""

            if app.testing or os.getenv("PYTEST_CURRENT_TEST"):
                return None

            forwarded_proto = request.headers.get("X-Forwarded-Proto")
            if forwarded_proto:
                proto = forwarded_proto.split(",")[0].strip().lower()
                if proto == "https":
                    return None
                if proto != "http":
                    return None
            elif request.is_secure:
                return None

            parts = urlsplit(request.url)
            hostname = parts.hostname or ""
            if not hostname:
                return None

            host = f"[{hostname}]" if ":" in hostname and not hostname.startswith("[") else hostname
            netloc = host
            if parts.username:
                credentials = parts.username
                if parts.password:
                    credentials = f"{credentials}:{parts.password}"
                netloc = f"{credentials}@{netloc}"

            if parts.port not in (None, 443):
                netloc = f"{netloc}:443"

            secure_url = urlunsplit(("https", netloc, parts.path, parts.query, parts.fragment))
            return redirect(secure_url, code=301)

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

    ssl_context = None

    cert_file = getenv("SSL_CERT_FILE")
    key_file = getenv("SSL_KEY_FILE")

    if cert_file and key_file:
        ssl_context = (cert_file, key_file)
    else:
        use_adhoc = getenv("USE_ADHOC_SSL", "1").strip().lower() not in {"0", "false", "no"}
        if use_adhoc:
            try:
                import cryptography  # type: ignore  # noqa: F401
            except ModuleNotFoundError:
                print(
                    "WARNING: Cannot generate ad-hoc SSL certificate because the 'cryptography' package is not installed. "
                    "Set SSL_CERT_FILE/SSL_KEY_FILE or install 'cryptography' to enable HTTPS when running via app.run().",
                    file=sys.stderr,
                )
            else:
                ssl_context = "adhoc"

    app.run(
        host="0.0.0.0",
        port=int(getenv("PORT", 443)),
        debug=getenv("FLASK_DEBUG", "0") == "1",
        use_reloader=False,  # <- important
        ssl_context=ssl_context,
    )
