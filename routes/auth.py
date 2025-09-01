# routes/auth.py
from functools import wraps
from flask import (
    Blueprint, render_template, request, redirect,
    url_for, session, flash, current_app
)

# ✅ ADD: use the auth service API you implemented in services/auth_service.py
from services.auth_service import authenticate, ensure_default_users

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


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    """Render login form and handle authentication."""
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""

        # ✅ REPLACE: (find_user + verify_password) → authenticate()
        user = authenticate(username, password)
        if not user:
            flash("Invalid username or password.", "danger")
            return render_template("login.html", username=username)

        # Minimal session payload; add more fields if you need them later
        session["username"] = user.get("username")
        flash(f"Welcome, {session['username']}!", "success")
        # Redirect to a sensible default (update if you prefer another page)
        return redirect(url_for("main.show_home"))

    return render_template("login.html")


@auth_bp.route("/logout", methods=["POST", "GET"])
def logout():
    """Log out current user and redirect to login page."""
    # Support GET or POST for convenience; restrict to POST if you prefer CSRF-only
    session.clear()
    flash("You have been logged out.", "info")
    return redirect(url_for("auth.login"))
