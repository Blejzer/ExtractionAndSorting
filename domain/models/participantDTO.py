from __future__ import annotations

from datetime import datetime
from typing import Optional, Tuple, Dict, Any

import pandas as pd
from pydantic import BaseModel, EmailStr, field_validator, ConfigDict

from config.database import mongodb
from domain.models.participant import Grade, Gender  # your enums
from utils.helpers import empty_to_none


class ParticipantRow(BaseModel):
    """
    Light-weight row validator for import (intentionally forgiving).
    Use datetime for dob (Mongo encodes datetime, not date).
    """
    model_config = ConfigDict(populate_by_name=True)

    pid: str
    name: str
    position: str
    grade: Grade = Grade.NORMAL
    representing_country: str
    gender: Gender
    dob: datetime
    pob: str
    birth_country: str

    # Optional contact
    email: Optional[EmailStr] = None
    phone: Optional[str] = None

    @field_validator("email", "phone", mode="before")
    @classmethod
    def _empty_to_none(cls, v):
        return empty_to_none(v)

    def to_mongo(self) -> dict:
        d = self.model_dump()
        # serialize enums to raw values for Mongo
        d["grade"] = d["grade"].value
        d["gender"] = d["gender"].value
        return d

