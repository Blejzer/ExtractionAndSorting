"""Service functions for handling training test scores."""

from __future__ import annotations

from typing import List, Optional, Dict, Any

from domain.models.test import TrainingTest, AttemptType
from repositories.test_repository import TrainingTestRepository


_repo = TrainingTestRepository()


def record_test_score(data: Dict[str, Any]) -> TrainingTest:
    """Create or update a test score entry."""
    test = TrainingTest(**data)
    _repo.save(test)
    return test


def get_test_score(eid: str, pid: str, attempt: str) -> Optional[TrainingTest]:
    """Retrieve a specific test score."""
    return _repo.find(eid, pid, AttemptType(attempt))


def list_event_tests(eid: str) -> List[TrainingTest]:
    """Return all test scores for an event."""
    return _repo.find_by_event(eid)
