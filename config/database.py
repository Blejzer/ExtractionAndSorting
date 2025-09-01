# config/database.py
from __future__ import annotations

import os
from typing import Optional
from urllib.parse import quote_plus

from pymongo import MongoClient
from pymongo.client_session import ClientSession
from pymongo.server_api import ServerApi


def _build_mongo_uri() -> str:
    """
    Build the MongoDB URI.
    Precedence:
      1. TEST_MONGODB_URI (for CI/tests)
      2. MONGODB_URI (full connection string)
      3. Individual parts: DB_USER / DB_PASSWORD / DB_HOST / DB_NAME
    """
    # 1. CI/Test override
    test_uri = os.getenv("TEST_MONGODB_URI")
    if test_uri:
        return test_uri

    # 2. Full URI override
    uri = os.getenv("MONGODB_URI")
    if uri:
        return uri

    # 3. Build from components
    user = os.getenv("DB_USER", "").strip()
    pwd = os.getenv("DB_PASSWORD", "").strip()
    host = os.getenv("DB_HOST", "").strip()
    dbname = os.getenv("DB_NAME", "event_management").strip()

    if not (user and pwd and host):
        raise RuntimeError(
            "Missing Mongo credentials. Set TEST_MONGODB_URI, MONGODB_URI or "
            "DB_USER/DB_PASSWORD/DB_HOST (and optionally DB_NAME)."
        )

    return (
        f"mongodb+srv://{user}:{quote_plus(pwd)}@{host}/{dbname}"
        f"?retryWrites=true&w=majority&appName=Cluster0&tls=true"
    )



class MongoConnection:
    """
    Singleton MongoDB client & DB accessor.
    - Holds a single pooled client for the process.
    - Offers helpers to get the DB and start sessions.
    """

    _instance: Optional["MongoConnection"] = None

    def __new__(cls) -> "MongoConnection":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init_client()
        return cls._instance

    def _init_client(self) -> None:
        uri = _build_mongo_uri()
        self._client = MongoClient(uri, server_api=ServerApi("1"), tls=True)
        # Fail fast if credentials/URI are wrong
        self._client.admin.command("ping")

        # Default DB name (from URI path or DB_NAME env)
        self._db_name = os.getenv("DB_NAME", "event_management")

    @property
    def client(self) -> MongoClient:
        """Return the shared MongoClient instance."""
        return self._client

    def db(self):
        """Return the default database handle."""
        return self._client[self._db_name]

    def collection(self, name: str):
        """Return a collection handle from the default DB."""
        return self.db()[name]

    def start_session(self) -> ClientSession:
        """Start a client session (use for multi-collection transactions)."""
        return self._client.start_session()

    def close(self) -> None:
        """Close the client and reset the singleton (used in tests/shutdown)."""
        if getattr(self, "_client", None) is not None:
            self._client.close()
        type(self)._instance = None


# Module-level singleton accessor
mongodb = MongoConnection()


# ---- (Optional) Index bootstrap hook ---------------------------------------
def bootstrap_indexes() -> None:
    """
    Create essential indexes if desired (call from a migration/boot step).
    Keep this minimal; full index management should live in migrations.
    """
    if os.getenv("DB_BOOTSTRAP_INDEXES", "0") != "1":
        return

    db = mongodb.db()
    # Examples (uncomment as your repos/queries finalize):
    # db["participants"].create_index("pid", unique=True)
    # db["events"].create_index("eid", unique=True)
    # db["countries"].create_index("country", unique=True)
    # db["users"].create_index("username", unique=True)
    # db["participant_events"].create_index(
    #     [("participant_id", 1), ("event_id", 1)],
    #     unique=True
    # )