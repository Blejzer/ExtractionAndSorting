
# middleware/auth.py
from functools import wraps
from flask import (
    Blueprint, redirect, url_for, session, flash, current_app
)

# ✅ ADD: use the auth service API you implemented in services/auth_service.py
from services.auth_service import ensure_default_users

auth_bp = Blueprint("auth", __name__)

@auth_bp.before_app_request
def _seed_default_users_once():
    # store a flag on the app object so it persists across requests
    if not current_app.config.get("_DEFAULT_USERS_SEEDED", False):
        try:
            ensure_default_users()
        except Exception as exc:
            current_app.logger.warning("ensure_default_users failed: %s", exc)
        current_app.config["_DEFAULT_USERS_SEEDED"] = True


def login_required(view_func):
    """Decorator that requires a logged-in user (session['username'])."""
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if "username" not in session:
            flash("Please log in to continue.", "warning")
            return redirect(url_for("auth.login"))
        return view_func(*args, **kwargs)
    return wrapper



