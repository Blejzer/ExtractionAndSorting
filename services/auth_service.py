# services/auth_service.py
from typing import Optional
from werkzeug.security import generate_password_hash, check_password_hash
from config.database import mongodb_connection

USERS = [
    ("nikola",  "N1k0l!ca"),
    ("marija",  "Marij@ci"),
    ("andrej",  "m@sterMind"),
]

def get_users_collection():
    return mongodb_connection.db["users"]

def ensure_users_collection_and_seed() -> None:
    users = get_users_collection()
    # Ensure unique index on username
    users.create_index("username", unique=True)

    # Seed users if missing (idempotent)
    for uname, pwd in USERS:
        if not users.find_one({"username": uname}):
            users.insert_one({
                "username": uname,
                "password_hash": generate_password_hash(pwd),  # pbkdf2:sha256
                "active": True,
            })

def find_user(username: str) -> Optional[dict]:
    return get_users_collection().find_one({"username": username})

def verify_password(pw_hash: str, candidate: str) -> bool:
    return check_password_hash(pw_hash, candidate)
