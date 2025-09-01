import os
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from urllib.parse import quote_plus
from dotenv import load_dotenv
from datetime import datetime
from typing import Optional, Dict, Any, List

# Load environment variables
load_dotenv()


class MongoDBConnection:
    """Singleton wrapper around a MongoDB client.

    Manages connection setup from environment, collection handles,
    and basic indexes. Access collections via attributes like
    `participants`, `events`, and `countries`.
    """
    _instance = None

    def __new__(cls):
        """Return the single shared instance of this connection."""
        if cls._instance is None:
            cls._instance = super(MongoDBConnection, cls).__new__(cls)
            cls._instance._initialize_connection()
        return cls._instance

    def _initialize_connection(self):
        """Create and verify a MongoDB connection using env vars.

        Reads `DB_PASSWORD`, constructs a MongoDB Atlas connection
        string, pings the server, initializes DB/collection handles,
        and ensures unique indexes on `pid` and `eid`.
        """
        try:
            db_password = os.getenv('DB_PASSWORD')

            # Debug: Check if password is being read
            print(f"DB_PASSWORD from env: {'*' * len(db_password) if db_password else 'NOT SET'}")

            if not db_password:
                raise ValueError("DB_PASSWORD environment variable is not set")

            escaped_password = quote_plus(db_password)

            # Debug: Show the connection string (without password)
            print(f"Connecting to: mongodb+srv://pfeUser:***@cluster0.k6jktp7.mongodb.net/")

            connection_string = (
                f"mongodb+srv://pfeUser:{escaped_password}@cluster0.k6jktp7.mongodb.net/"
                f"?retryWrites=true&w=majority&appName=Cluster0"
                f"&tls=true"
            )

            self.client = MongoClient(
                connection_string,
                server_api=ServerApi('1'),
                tls=True
            )

            # Verify connection
            self.client.admin.command('ping')
            print("Successfully connected to MongoDB!")

            # Initialize collections (rest of your code remains the same)
            self.db = self.client["event_management"]
            self.participants = self.db["participants"]
            self.events = self.db["events"]
            self.countries = self.db["countries"]
            self.users = self.db["users"]

            # Ensure unique index on ids
            self.participants.create_index("pid", unique=True)
            self.events.create_index("eid", unique=True)
            self.users.create_index("username", unique=True)

        except Exception as e:
            print(f"Failed to connect to MongoDB: {e}")
            raise

    # ========== PARTICIPANT CRUD OPERATIONS ==========
    def create_participant(self, pid: str, name: str, position: str, grade: str) -> str:
        """Insert a participant document.

        Parameters are stored as-is; `pid` must be unique.
        Returns the inserted document’s ObjectId as a string.
        """
        participant = {
            "pid": pid,
            "name": name,
            "position": position,
            "grade": grade
        }
        result = self.participants.insert_one(participant)
        return str(result.inserted_id)

    def get_participant(self, pid: str) -> Optional[Dict[str, Any]]:
        """Fetch a participant by `pid`.

        Returns the full participant document dict or None
        if no match is found.
        """
        return self.participants.find_one({"pid": pid})

    def update_participant(self, pid: str, update_data: Dict[str, Any]) -> int:
        """Update fields on a participant by `pid`.

        `update_data` is applied via `$set`. Returns the number
        of modified documents (0 or 1).
        """
        result = self.participants.update_one(
            {"pid": pid},
            {"$set": update_data}
        )
        return result.modified_count

    def delete_participant(self, pid: str) -> int:
        """Delete a participant by `pid`.

        Returns the number of deleted documents (0 or 1).
        """
        result = self.participants.delete_one({"pid": pid})
        return result.deleted_count

    def list_participants(self) -> List[Dict[str, Any]]:
        """Return all participants as a list of dicts.

        No pagination or filtering is applied here.
        """
        return list(self.participants.find({}))


    # ========== EVENT CRUD OPERATIONS ==========
    def create_event(self, eid: str, title: str, date_from: datetime, date_to: datetime, location: str) -> str:
        """Insert an event document with ids and date range.

        `eid` must be unique. Returns inserted ObjectId as string.
        """
        event = {
            "eid": eid,
            "title": title,
            "dateFrom": date_from,
            "dateTo": date_to,
            "location": location
        }
        result = self.events.insert_one(event)
        return str(result.inserted_id)

    def get_event(self, eid: str) -> Optional[Dict[str, Any]]:
        """Fetch an event by `eid`.

        Returns the event document dict or None if not found.
        """
        return self.events.find_one({"eid": eid})

    def update_event(self, eid: str, update_data: Dict[str, Any]) -> int:
        """Update fields on an event by `eid`.

        `update_data` is applied via `$set`. Returns modified count.
        """
        result = self.events.update_one(
            {"eid": eid},
            {"$set": update_data}
        )
        return result.modified_count

    def delete_event(self, eid: str) -> int:
        """Delete an event by `eid`.

        Returns the number of deleted documents (0 or 1).
        """
        result = self.events.delete_one({"eid": eid})
        return result.deleted_count

    def list_events(self) -> List[Dict[str, Any]]:
        """Return all events as a list of dicts.

        No pagination or filtering is applied here.
        """
        return list(self.events.find({}))


    # ========== COUNTRY CRUD OPERATIONS ==========
    def create_country(self, cid: str, country: str) -> str:
        """Insert a country document with a `cid` and name.

        Returns the inserted ObjectId as a string.
        """
        country_data = {
            "cid": cid,
            "country": country
        }
        result = self.countries.insert_one(country_data)
        return str(result.inserted_id)

    def get_country(self, cid: str) -> Optional[Dict[str, Any]]:
        """Fetch a country by `cid`.

        Returns the country document or None if not found.
        """
        return self.countries.find_one({"cid": cid})

    def update_country(self, cid: str, country: str) -> int:
        """Update a country's name by `cid`.

        Sets the `country` field. Returns modified count.
        """
        result = self.countries.update_one(
            {"cid": cid},
            {"$set": {"country": country}}
        )
        return result.modified_count

    def delete_country(self, cid: str) -> int:
        """Delete a country by `cid`.

        Returns the number of deleted documents (0 or 1).
        """
        result = self.countries.delete_one({"cid": cid})
        return result.deleted_count

    def list_countries(self) -> List[Dict[str, Any]]:
        """Return all countries as a list of dicts.

        No pagination or filtering is applied here.
        """
        return list(self.countries.find({}))


    # ========== Users (auth) repository ==========

    def get_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """Return user document by username or None if not found."""
        return self.users.find_one({"username": username})

    def create_user(self, username: str, password_hash: str, **extra) -> str:
        """Insert a new user with precomputed password_hash. Returns inserted id."""
        doc = {"username": username, "password_hash": password_hash, **extra}
        res = self.users.insert_one(doc)
        return str(res.inserted_id)

    def user_exists(self, username: str) -> bool:
        """Return True if a user with this username exists."""
        return self.users.count_documents({"username": username}, limit=1) > 0

    def set_user_password_hash(self, username: str, password_hash: str) -> int:
        """Update password_hash for a user. Returns modified count (0/1)."""
        res = self.users.update_one(
            {"username": username},
            {"$set": {"password_hash": password_hash}}
        )
        return res.modified_count

    def list_users(self, fields: Optional[Dict[str, int]] = None) -> List[Dict[str, Any]]:
        """Return all users (optionally with projection)."""
        cursor = self.users.find({}, fields or {})
        return list(cursor)


def close_connection(self):
    """Close the MongoDB client and reset the singleton.

    Safe to call multiple times; closes the underlying client
    and clears the cached `_instance`.
    """
    if self.client:
        self.client.close()
        self._instance = None
        print("MongoDB connection closed")


# Singleton instance
mongodb_connection = MongoDBConnection()