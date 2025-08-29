# routes/auth.py
from functools import wraps
from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from urllib.parse import urlparse, urljoin

from services.auth_service import ensure_users_collection_and_seed, find_user, verify_password

auth_bp = Blueprint("auth", __name__)

def _is_safe_url(target: str) -> bool:
    # Only allow same-host redirects
    ref_url = urlparse(request.host_url)
    test_url = urlparse(urljoin(request.host_url, target))
    return (test_url.scheme in ("http", "https")) and (ref_url.netloc == test_url.netloc)

def login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not session.get("username"):
            # preserve where the user was going
            next_url = request.url
            return redirect(url_for("auth.login", next=next_url))
        return view_func(*args, **kwargs)
    return wrapper

@auth_bp.before_app_request
def seed_once():
    # Make sure users collection/index exists and seed runs once
    # (cheap idempotent call)
    ensure_users_collection_and_seed()

@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        user = find_user(username)
        if not user or not verify_password(user.get("password_hash", ""), password):
            flash("Invalid username or password", "danger")
            return render_template("login.html"), 401

        # Success
        session["username"] = username
        flash(f"Welcome, {username}!", "success")
        next_param = request.args.get("next") or request.form.get("next")
        if next_param and _is_safe_url(next_param):
            return redirect(next_param)
        return redirect(url_for("main.show_home"))  # or wherever your landing route is
    # GET
    return render_template("login.html")

@auth_bp.route("/logout", methods=["POST", "GET"])
def logout():
    session.pop("username", None)
    flash("You have been logged out.", "info")
    return redirect(url_for("auth.login"))
