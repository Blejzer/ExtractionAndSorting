from __future__ import annotations

from pydantic import BaseModel, Field, EmailStr


class User(BaseModel):
    """
    User entity.

    - uid: 3-character primary key (e.g., "U01")
    - username: display name (e.g., "testUser")
    - password: Hashed password (e.g., "1234!@#$asf!@#$asdf")
    - email: email address (e.g., "nikola@bdslab.info")
    """
    uid: str = Field(..., min_length=4, max_length=4, description="Primary key, 3 characters")
    username: str = Field(..., min_length=2, description="login required username")
    password: str = Field(..., min_length=8, description="Hashed password")
    email: EmailStr = Field(..., description="email address")

    def to_mongo(self) -> dict:
        return self.model_dump(exclude_none=True)

    @classmethod
    def from_mongo(cls, doc: dict | None) -> "User | None":
        """Deserialize from MongoDB."""
        if not doc:
            return None
        return cls(**doc)