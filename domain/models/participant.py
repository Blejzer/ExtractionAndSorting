from __future__ import annotations

from functools import partial
from datetime import date, datetime
from enum import IntEnum, StrEnum
from typing import Annotated, Any, Callable, List, Optional, Union

import pandas as pd

from pydantic import (
    AliasChoices,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    EmailStr,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from utils.dates import normalize_dob
from utils.helpers import _to_app_display_name
from utils.normalize_phones import normalize_phone

DOBField = Annotated[
    Optional[datetime],
    BeforeValidator(partial(normalize_dob, strict=True)),
]

class Gender(StrEnum):
    male = "Male"
    female = "Female"


class Grade(IntEnum):
    BLACK_LIST = 0
    NORMAL = 1
    EXCELLENT = 2


class Participant(BaseModel):
    """Canonical domain model for participant with Country relationships."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
        use_enum_values=True,
    )

    # Identity / affiliation
    pid: str = Field(..., description="Primary ID like 'P0001'", min_length=3)
    representing_country: str = Field(..., description="Country CID reference", min_length=2, max_length=10)
    gender: Gender
    grade: Grade = Field(default=Grade.NORMAL, description="Participant grade: 0=Black List, 1=Normal, 2=Excellent")

    # Name field
    name: str = Field(..., min_length=1)

    # Birth / citizenship - all use Country CID references
    dob: DOBField = Field(default=None)
    pob: Optional[str] = Field(..., description="Place of birth (city name)")
    birth_country: Optional[str] = Field(..., description="Country CID reference")
    citizenships: Optional[list[str]] = Field(
        default=None, description="List of Country CID references"
    )

    # ✅ allow None
    email: Optional[EmailStr] = None
    phone: Optional[str] = None

    # Profile
    diet_restrictions: Optional[str] = None
    organization: Optional[str] = None
    unit: Optional[str] = None
    position: Optional[str] = None
    rank: Optional[str] = None
    intl_authority: Optional[bool] = None
    bio_short: Optional[str] = None

    # Audit trail
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    audit: List[dict[str, Any]] = Field(
        default_factory=list,
        validation_alias=AliasChoices("_audit", "audit"),
        serialization_alias="_audit",
    )

    # ---------- Validators ----------

    @field_validator("name", mode="after")
    @classmethod
    def _normalize_name(cls, v: str) -> str:
        return _to_app_display_name(v)

    @field_validator("citizenships", mode="before")
    @classmethod
    def _normalize_citizenships(cls, v: Union[str, list[str], None]) -> Optional[list[str]]:
        if v is None:
            return None
        if isinstance(v, list):
            items = [s.strip() for s in v if s and str(s).strip()]
        else:
            items = [p.strip() for p in str(v).replace(",", ";").split(";") if p.strip()]
        if not items:
            return None
        seen: set[str] = set()
        unique_items: list[str] = []
        for item in items:
            if item not in seen:
                seen.add(item)
                unique_items.append(item)
        return unique_items


    @field_validator("email", "phone", mode="before")
    @classmethod
    def empty_to_none(cls, v):
        # Treat NaN, empty, or whitespace-only as None
        if v is None:
            return None
        if isinstance(v, float) and pd.isna(v):  # catches NaN from pandas
            return None
        s = str(v).strip()
        return s or None


    @field_validator("phone", mode="after")
    @classmethod
    def _validate_phone(cls, v: Optional[str]) -> Optional[str]:
        text = str(v or "").strip()
        if not text:
            return None

        normalized = normalize_phone(text)
        if normalized:
            return normalized
        raise ValueError("invalid phone number format")

    @model_validator(mode="after")
    def _require_dob_unless_legacy(self, info: ValidationInfo):
        allow_missing = bool(info.context.get("allow_missing_dob")) if info.context else False
        if self.dob is None and not allow_missing:
            raise ValueError("dob is required")
        return self

    def to_mongo(self) -> dict:
        """Serialize for Mongo (exclude None)."""
        return self.model_dump(by_alias=True, exclude_none=True)

    @classmethod
    def from_mongo(cls, doc: dict) -> "Participant":
        """Hydrate from MongoDB document."""
        return cls.model_validate(doc, context={"allow_missing_dob": True})

    # ---------- Helper Methods for Country Relationships ----------

    def get_country_references(self) -> set[str]:
        """Get all unique country CID references used by this participant"""
        country_refs = {self.representing_country, self.birth_country}
        if self.citizenships:
            country_refs.update(self.citizenships)
        return country_refs


    def to_display_dict(self, country_resolver: Callable[[str], str]) -> dict:
        """Convert to display format with resolved country names"""
        data = self.model_dump(exclude_none=True)

        # Resolve country CIDs to names
        data["representing_country_name"] = country_resolver(self.representing_country)
        data["birth_country_name"] = country_resolver(self.birth_country)
        data["citizenship_names"] = [
            country_resolver(cid) for cid in (self.citizenships or [])
        ]

        return data
