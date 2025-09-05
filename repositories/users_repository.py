from __future__ import annotations

from typing import Optional
from bson import ObjectId

from config.database import mongodb
from domain.models.user import User


class UserRepository:
    """CRUD operations for users collection."""

    def __init__(self) -> None:
        self.collection = mongodb.collection("users")
        # ensure index for fast username lookups
        self.collection.create_index("username", unique=True)

    def create(self, user: User) -> str:
        result = self.collection.insert_one(user.to_mongo())
        return str(result.inserted_id)

    def get_by_id(self, user_id: str) -> Optional[User]:
        doc = self.collection.find_one({"_id": ObjectId(user_id)})
        return User.from_mongo(doc)

    def get_by_username(self, username: str) -> Optional[User]:
        doc = self.collection.find_one({"username": username})
        return User.from_mongo(doc)

    def update(self, user_id: str, data: dict) -> int:
        data = {k: v for k, v in data.items() if k != "_id"}
        result = self.collection.update_one({"_id": ObjectId(user_id)}, {"$set": data})
        return result.modified_count

    def delete(self, user_id: str) -> int:
        result = self.collection.delete_one({"_id": ObjectId(user_id)})
        return result.deleted_count
