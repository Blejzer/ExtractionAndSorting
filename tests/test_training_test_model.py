import os
import sys

import pytest

# Ensure project root on sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from domain.models.training_test_model import TrainingTest, AttemptType


def test_training_test_to_from_mongo_roundtrip():
    t = TrainingTest(eid="E1", pid="P1", type=AttemptType.pre, score=88.5)
    doc = t.to_mongo()
    assert doc == {"eid": "E1", "pid": "P1", "type": "pre", "score": 88.5}
    assert TrainingTest.from_mongo(doc) == t
    assert TrainingTest.from_mongo(None) is None


def test_training_test_score_non_negative():
    with pytest.raises(ValueError):
        TrainingTest(eid="E1", pid="P1", type=AttemptType.post, score=-5)
