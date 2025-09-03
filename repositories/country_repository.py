from __future__ import annotations

from typing import List, Optional
from pymongo.collection import Collection
from pymongo import ASCENDING

from config.database import mongodb
from domain.models.country import Country


class CountryRepository:
    """Repository for Country model with CRUD operations."""

    def __init__(self):
        self.collection: Collection = mongodb.db()["countries"]

    def ensure_indexes(self):
        """Create necessary indexes."""
        self.collection.create_index([("cid", ASCENDING)], unique=True)
        self.collection.create_index([("name", ASCENDING)], unique=True)

    def find_by_cid(self, cid: str) -> Optional[Country]:
        """Find a country by its CID."""
        doc = self.collection.find_one({"cid": cid})
        return Country.from_mongo(doc) if doc else None

    def find_by_name(self, name: str) -> Optional[Country]:
        """Find a country by its name."""
        doc = self.collection.find_one({"name": name})
        return Country.from_mongo(doc) if doc else None

    def find_all(self) -> List[Country]:
        """Find all countries."""
        cursor = self.collection.find().sort("name", ASCENDING)
        return [Country.from_mongo(doc) for doc in cursor]

    def save(self, country: Country) -> str:
        """Save a country to the database."""
        result = self.collection.insert_one(country.to_mongo())
        return str(result.inserted_id)

    def bulk_save(self, countries: List[Country]) -> List[str]:
        """Save multiple countries to the database."""
        result = self.collection.insert_many([country.to_mongo() for country in countries])
        return [str(cid) for cid in result.inserted_ids]