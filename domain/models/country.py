from __future__ import annotations

from pydantic import BaseModel, Field, constr


class Country(BaseModel):
    """
    Country entity.

    - cid: 4-character primary key (e.g., "c001")
    - country: display name (e.g., "Albania")
    """
    cid: str = Field(..., min_length=4, max_length=4, description="Primary key, 4 characters")
    country: str = Field(..., min_length=2, description="Country name like 'Italy'")

    def to_mongo(self) -> dict:
        return self.model_dump(exclude_none=True)

    @classmethod
    def from_mongo(cls, doc: dict | None) -> "Country | None":
        """Deserialize from MongoDB."""
        if not doc:
            return None
        return cls(**doc)