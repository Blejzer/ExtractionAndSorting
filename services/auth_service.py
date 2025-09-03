# services/auth_service.py
from typing import Optional, Dict, Any
from werkzeug.security import generate_password_hash, check_password_hash
from config.database import mongodb


def authenticate(username: str, password: str) -> Optional[Dict[str, Any]]:
    """Return user doc if username/password are valid; otherwise None."""
    # TODO: user = mongodb.db()[get_user_by_username(username)]
    # if not user:
    #     return None
    # pw_hash = user.get("password_hash")
    # if not pw_hash:
    #     return None
    # return user if check_password_hash(pw_hash, password) else None


def register_user(username: str, password: str, **extra) -> str:
    """Create a new user with a hashed password. Raises on duplicate username."""
    # TODO: Create user in MongoDB
    return "User created"
    # if mongodb_connection.user_exists(username):
    #     raise ValueError("Username already exists")
    # pw_hash = generate_password_hash(password)
    # return mongodb_connection.create_user(username, pw_hash, **extra)


def change_password(username: str, new_password: str) -> bool:
    """Set a new password for an existing user. Returns True if updated."""
    # TODO: Update user in MongoDB
    return "Password updated"
    # if not mongodb_connection.user_exists(username):
    #     return False
    # pw_hash = generate_password_hash(new_password)
    # return mongodb_connection.set_user_password_hash(username, pw_hash) == 1


def ensure_default_users() -> None:
    """Idempotently create the initial admin users with hashed passwords."""
    # TODO: Create users in MongoDB
    # defaults = [
    #     ("nikola",  "N1k0l!ca"),
    #     ("marija",  "Marij@ci"),
    #     ("andrej",  "m@sterMind"),
    # ]
    # for username, raw_pw in defaults:
    #     if not mongodb_connection.user_exists(username):
    #         pw_hash = generate_password_hash(raw_pw)
    #         mongodb_connection.create_user(username, pw_hash)
