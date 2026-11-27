"""Service functions for handling training test scores."""

from __future__ import annotations

from typing import List, Optional, Dict, Any
from math import isfinite

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


def event_test_statistics(eid: str) -> dict:
    """Summarize recorded participant percentages; absent tests are not zeroes."""
    scores = {"pre": [], "post": []}
    for test in list_event_tests(eid):
        if test.eid != eid or test.type not in scores:
            continue
        value = test.score
        if isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value) and 0 <= value <= 100:
            scores[test.type].append(value)
    result = {}
    for attempt, values in scores.items():
        count = len(values)
        over = sum(score > 75 for score in values)
        result[attempt] = dict(count=count, average=sum(values) / count if count else None,
                               over_75_count=over, over_75_percent=over / count * 100 if count else None)
    return result
