from __future__ import annotations

import pandas as pd
from datetime import datetime, timezone
from enum import IntEnum, StrEnum
from typing import Any, List, Optional, Union, Callable

from pydantic import (
    BaseModel,
    Field,
    EmailStr,
    field_validator,
    model_validator,
    ConfigDict,
    AliasChoices,
)


class Gender(StrEnum):
    male = "Male"
    female = "Female"


class Transport(StrEnum):
    pov = "Personal Vehicle (POV)"
    gov = "Government (Official) Vehicle (GOV)"
    air = "Air (Airplane)"
    other = "Other"


class DocType(StrEnum):
    passport = "Passport"
    other = "Other"


class IbanType(StrEnum):
    eur = "EURO"
    usd = "USD"
    multi = "Multi-Currency"


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
    grade: Optional[Grade] = Field(default=Grade.NORMAL, description="Participant grade: 0=Black List, 1=Normal, 2=Excellent")

    # Name field
    name: str = Field(..., min_length=1)

    # Birth / citizenship - all use Country CID references
    dob: datetime
    pob: str = Field(..., min_length=1, description="Place of birth (city name)")
    birth_country: str = Field(..., description="Country CID reference", min_length=2, max_length=10)
    citizenships: Optional[list[str]] = Field(
        default=None, description="List of Country CID references"
    )

    # ✅ allow None
    email: Optional[EmailStr] = None
    phone: Optional[str] = None

    # Travel document
    travel_doc_type: Optional[DocType] = None
    travel_doc_type_other: Optional[str] = None
    travel_doc_issue_date: Optional[datetime] = None
    travel_doc_expiry_date: Optional[datetime] = None
    travel_doc_issued_by: Optional[str] = None

    # Travel / visa
    # requires_visa_hr: bool
    transportation: Optional[Transport] = None
    transport_other: Optional[str] = None
    travelling_from: Optional[str] = Field(
        default=None, description="City/country of departure"
    )
    returning_to: Optional[str] = Field(
        default=None, description="City/country of return"
    )

    # Profile
    diet_restrictions: Optional[str] = None
    organization: Optional[str] = None
    unit: Optional[str] = None
    position: Optional[str] = None
    rank: Optional[str] = None
    intl_authority: Optional[bool] = None
    bio_short: Optional[str] = None

    # Banking
    bank_name: Optional[str] = None
    iban: Optional[str] = None
    iban_type: Optional[IbanType] = None
    swift: Optional[str] = None

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
        parts = v.split()
        if parts:
            parts[-1] = parts[-1].upper()
        return " ".join(parts)

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
        if not v:
            return None
        try:
            import phonenumbers
            num = phonenumbers.parse(v, None)
            if not phonenumbers.is_valid_number(num):
                raise ValueError("invalid phone number")
            return phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164)
        except Exception:
            if v.startswith("+") and 8 <= sum(c.isdigit() for c in v) <= 15:
                return v
            raise ValueError("invalid phone number format")

    @model_validator(mode="after")
    def _require_other_details(self):
        if (
            self.travel_doc_type == DocType.other
            and not self.travel_doc_type_other
        ):
            raise ValueError("Provide 'travel_doc_type_other' when travel_doc_type is 'Other'.")

        if self.transportation == Transport.other and not self.transport_other:
            raise ValueError("Provide 'transport_other' when transportation is 'Other'.")

        if (
            self.travel_doc_issue_date
            and self.travel_doc_expiry_date
            and self.travel_doc_issue_date > self.travel_doc_expiry_date
        ):
            raise ValueError("travel_doc_issue_date must be on/before travel_doc_expiry_date.")
        return self

    def to_mongo(self) -> dict:
        """Serialize for Mongo (exclude None)."""
        return self.model_dump(by_alias=True, exclude_none=True)

    @classmethod
    def from_mongo(cls, doc: dict) -> "Participant":
        """Hydrate from MongoDB document."""
        return cls.model_validate(doc)

    # ---------- Helper Methods for Country Relationships ----------

    def get_country_references(self) -> set[str]:
        """Get all unique country CID references used by this participant"""
        country_refs = {self.representing_country, self.birth_country}
        if self.citizenships:
            country_refs.update(self.citizenships)
        return country_refs

    # def validate_country_references(self, valid_country_cids: set[str]) -> bool:
    #     """Validate all country references against a set of valid CIDs"""
    #     participant_country_refs = self.get_country_references()
    #     return participant_country_refs.issubset(valid_country_cids)

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
