from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class AttemptType(StrEnum):
    pre = "pre"
    post = "post"


@dataclass
class TrainingTest:
    eid: str
    pid: str
    type: AttemptType
    score: float

    def __post_init__(self) -> None:
        if self.score < 0:
            raise ValueError("score must be non-negative")

    def to_mongo(self) -> dict:
        return {
            "eid": self.eid,
            "pid": self.pid,
            "type": self.type.value,
            "score": self.score,
        }

    @classmethod
    def from_mongo(cls, doc: dict | None) -> TrainingTest | None:
        if not doc:
            return None
        return cls(
            eid=doc["eid"],
            pid=doc["pid"],
            type=AttemptType(doc["type"]),
            score=doc["score"],
        )

    def model_dump(self, **_kwargs) -> dict:
        return self.to_mongo()
