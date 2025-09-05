# services/auth_service.py
from typing import Optional, Dict, Any
from werkzeug.security import generate_password_hash, check_password_hash

from domain.models.user import User
from repositories.users_repository import UserRepository


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


def ensure_default_users() -> None:
    """Idempotently create the initial admin users with hashed passwords."""
    repo = UserRepository()
    defaults = [
        ("nikola", "N1k0l!ca"),
        ("marija", "Marij@ci"),
        ("andrej", "m@sterMind"),
    ]
    for username, raw_pw in defaults:
        if not repo.get_by_username(username):
            pw_hash = generate_password_hash(raw_pw)
            repo.create(User(username=username, password_hash=pw_hash))
