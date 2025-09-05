# routes/auth.py
from flask import (
    render_template, request, redirect,
    url_for, session, flash
)

from services.auth_service import authenticate
from middleware.auth import auth_bp

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
        session["username"] = user["username"]
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

