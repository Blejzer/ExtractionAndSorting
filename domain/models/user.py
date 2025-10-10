from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from bson import ObjectId


@dataclass
class User:
    """Minimal user model used for authentication tests."""

    id: Optional[str] = None
    username: str = ""
    password_hash: str = ""
    email: Optional[str] = None

    def to_mongo(self) -> dict:
        data = {
            "username": self.username,
            "password_hash": self.password_hash,
        }
        if self.email:
            data["email"] = self.email
        if self.id:
            data["_id"] = ObjectId(self.id)
        return data

    @classmethod
    def from_mongo(cls, doc: dict | None) -> User | None:
        if not doc:
            return None
        _id = doc.get("_id")
        return cls(
            id=str(_id) if _id is not None else None,
            username=doc.get("username", ""),
            password_hash=doc.get("password_hash", ""),
            email=doc.get("email"),
        )

    def model_dump(self, **_kwargs) -> dict:
        data = {
            "username": self.username,
            "password_hash": self.password_hash,
        }
        if self.email:
            data["email"] = self.email
        if self.id:
            data["_id"] = self.id
        return data
