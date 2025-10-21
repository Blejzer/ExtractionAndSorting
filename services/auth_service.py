# services/auth_service.py
import os
from typing import Optional, Dict, Any

from pydantic import json
from werkzeug.security import generate_password_hash, check_password_hash

from domain.models.user import User
from repositories.user_repository import UserRepository


def authenticate(username: str, password: str) -> Optional[Dict[str, Any]]:
    """Return user doc if username/password are valid; otherwise None."""
    repo = UserRepository()
    user = repo.get_by_username(username)
    if not user:
        return None
    if not check_password_hash(user.password_hash, password):
        return None
    return user.model_dump(exclude={"password_hash"})


def register_user(username: str, password: str, **extra) -> str:
    """Create a new user with a hashed password. Raises on duplicate username."""
    repo = UserRepository()
    if repo.get_by_username(username):
        raise ValueError("Username already exists")
    pw_hash = generate_password_hash(password)
    user = User(username=username, password_hash=pw_hash, **extra)
    return repo.create(user)


def change_password(username: str, new_password: str) -> bool:
    """Set a new password for an existing user. Returns True if updated."""
    repo = UserRepository()
    user = repo.get_by_username(username)
    if not user:
        return False
    pw_hash = generate_password_hash(new_password)
    return repo.update(user.id, {"password_hash": pw_hash}) == 1


def _load_admin_users_from_env() -> list[tuple[str, str]]:
        """
        Returns a list of (username, password) from env:
          1) ADMIN_USERS (JSON array of {"username","password"})
          2) ADMIN_USERS_FILE (path to JSON file with same schema)
          3) ADMIN_USERNAME + ADMIN_PASSWORD (legacy single pair)
        """
        users: list[tuple[str, str]] = []

        # 1) ADMIN_USERS as JSON string
        raw_json = os.getenv("ADMIN_USERS")
        if raw_json:
            try:
                data = json.loads(raw_json)
                for item in data:
                    u = (item.get("username") or "").strip()
                    p = item.get("password")
                    if u and p is not None:
                        users.append((u, p))
            except json.JSONDecodeError as e:
                print("❌ Failed to parse ADMIN_USERS JSON: %s", e)

        # 3) Legacy single pair
        if not users:
            u = (os.getenv("ADMIN_USERNAME") or "").strip()
            p = os.getenv("ADMIN_PASSWORD")
            if u and p is not None:
                users.append((u, p))

        # Deduplicate by username, keep the first occurrence
        seen = set()
        deduped: list[tuple[str, str]] = []
        for u, p in users:
            if u and u not in seen:
                seen.add(u)
                deduped.append((u, p))
        return deduped


def ensure_default_users() -> None:
    """Idempotently create the initial admin users with hashed passwords."""
    repo = UserRepository()
    # defaults = [
    #     ("nikola", "N1k0l!ca"),
    #     ("marija", "Marij@ci"),
    #     ("andrej", "m@sterMind"),
    # ]
    defaults = _load_admin_users_from_env()
    for username, raw_pw in defaults:
        if not repo.get_by_username(username):
            pw_hash = generate_password_hash(raw_pw)
            repo.create(User(username=username, password_hash=pw_hash))
