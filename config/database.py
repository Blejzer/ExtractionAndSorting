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
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoDBConnection, cls).__new__(cls)
            cls._instance._initialize_connection()
        return cls._instance

    def _initialize_connection(self):
        """Initialize the MongoDB connection"""
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

            # Ensure unique index on ids
            self.participants.create_index("pid", unique=True)
            self.events.create_index("eid", unique=True)

        except Exception as e:
            print(f"Failed to connect to MongoDB: {e}")
            raise

    # ========== PARTICIPANT CRUD OPERATIONS ==========
    def create_participant(self, pid: str, name: str, position: str, grade: str) -> str:
        """Create a new participant"""
        participant = {
            "pid": pid,
            "name": name,
            "position": position,
            "grade": grade
        }
        result = self.participants.insert_one(participant)
        return str(result.inserted_id)

    def get_participant(self, pid: str) -> Optional[Dict[str, Any]]:
        """Get a participant by pid"""
        return self.participants.find_one({"pid": pid})

    def update_participant(self, pid: str, update_data: Dict[str, Any]) -> int:
        """Update a participant"""
        result = self.participants.update_one(
            {"pid": pid},
            {"$set": update_data}
        )
        return result.modified_count


def delete_participant(self, pid: str) -> int:
    """Delete a participant"""
    result = self.participants.delete_one({"pid": pid})
    return result.deleted_count


def list_participants(self) -> List[Dict[str, Any]]:
    """List all participants"""
    return list(self.participants.find({}))


# ========== EVENT CRUD OPERATIONS ==========
def create_event(self, eid: str, title: str, date_from: datetime, date_to: datetime, location: str) -> str:
    """Create a new event"""
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
    """Get an event by eid"""
    return self.events.find_one({"eid": eid})


def update_event(self, eid: str, update_data: Dict[str, Any]) -> int:
    """Update an event"""
    result = self.events.update_one(
        {"eid": eid},
        {"$set": update_data}
    )
    return result.modified_count


def delete_event(self, eid: str) -> int:
    """Delete an event"""
    result = self.events.delete_one({"eid": eid})
    return result.deleted_count


def list_events(self) -> List[Dict[str, Any]]:
    """List all events"""
    return list(self.events.find({}))


# ========== COUNTRY CRUD OPERATIONS ==========
def create_country(self, cid: str, country: str) -> str:
    """Create a new country"""
    country_data = {
        "cid": cid,
        "country": country
    }
    result = self.countries.insert_one(country_data)
    return str(result.inserted_id)


def get_country(self, cid: str) -> Optional[Dict[str, Any]]:
    """Get a country by cid"""
    return self.countries.find_one({"cid": cid})


def update_country(self, cid: str, country: str) -> int:
    """Update a country"""
    result = self.countries.update_one(
        {"cid": cid},
        {"$set": {"country": country}}
    )
    return result.modified_count


def delete_country(self, cid: str) -> int:
    """Delete a country"""
    result = self.countries.delete_one({"cid": cid})
    return result.deleted_count


def list_countries(self) -> List[Dict[str, Any]]:
    """List all countries"""
    return list(self.countries.find({}))


def close_connection(self):
    """Close the MongoDB connection"""
    if self.client:
        self.client.close()
        self._instance = None
        print("MongoDB connection closed")


# Singleton instance
mongodb_connection = MongoDBConnection()