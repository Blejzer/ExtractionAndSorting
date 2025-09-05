from __future__ import annotations

from typing import Optional
from bson import ObjectId
from pydantic import BaseModel, Field, EmailStr


class User(BaseModel):
    """User entity stored in MongoDB."""

    id: Optional[str] = Field(default=None, alias="_id", description="MongoDB identifier")
    username: str = Field(..., min_length=2, description="Unique username")
    password_hash: str = Field(..., min_length=8, description="Hashed password")
    email: Optional[EmailStr] = Field(default=None, description="Email address")

    def to_mongo(self) -> dict:
        data = self.model_dump(exclude_none=True, by_alias=True)
        if isinstance(data.get("_id"), str):
            data["_id"] = ObjectId(data["_id"])
        return data

    @classmethod
    def from_mongo(cls, doc: dict | None) -> "User | None":
        if not doc:
            return None
        doc = {**doc, "_id": str(doc.get("_id"))}
        return cls(**doc)
