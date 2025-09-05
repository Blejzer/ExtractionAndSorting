from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field, ConfigDict


class AttemptType(StrEnum):
    """Enumeration for test attempt type."""
    pre = "pre"
    post = "post"


class TrainingTest(BaseModel):
    """Participant test score for a specific training event."""

    model_config = ConfigDict(populate_by_name=True)

    eid: str = Field(..., min_length=1, description="Event/training identifier")
    pid: str = Field(..., min_length=1, description="Participant identifier")
    type: AttemptType = Field(..., description="Attempt type: 'pre' or 'post'")
    score: float = Field(..., ge=0, description="Score for this attempt")

    def to_mongo(self) -> dict:
        """Serialize to MongoDB-compatible dict."""
        return self.model_dump(by_alias=True, exclude_none=True)

    @classmethod
    def from_mongo(cls, doc: dict | None) -> "TrainingTest | None":
        """Deserialize from MongoDB document."""
        if not doc:
            return None
        return cls(**doc)
