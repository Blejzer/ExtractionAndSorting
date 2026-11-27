from types import SimpleNamespace

import pytest

import services.tests_service as service


def test_event_averages_and_percentages_use_only_recorded_scores_and_strictly_above_75(monkeypatch):
    records=[SimpleNamespace(eid="E1",pid=f"P{i}",type=attempt,score=score)
             for attempt,values in [("pre",[0,75,100]),("post",[75,75.01,100])]
             for i,score in enumerate(values)]
    records.append(SimpleNamespace(eid="OTHER",pid="P9",type="pre",score=100))
    calls=[]
    def find(eid):calls.append(eid);return records
    monkeypatch.setattr(service._repo,"find_by_event",find)
    stats=service.event_test_statistics("E1")
    assert calls==["E1"]
    assert stats["pre"]==dict(count=3,average=pytest.approx(175/3),over_75_count=1,over_75_percent=pytest.approx(100/3))
    assert stats["post"]["count"]==3 and stats["post"]["average"]==pytest.approx(250.01/3)
    assert stats["post"]["over_75_count"]==2 and stats["post"]["over_75_percent"]==pytest.approx(200/3)


def test_missing_tests_are_unknown_and_recorded_zero_is_included(monkeypatch):
    monkeypatch.setattr(service._repo,"find_by_event",lambda eid:[SimpleNamespace(eid=eid,pid="P1",type="post",score=0)])
    stats=service.event_test_statistics("E1")
    assert stats["pre"]==dict(count=0,average=None,over_75_count=0,over_75_percent=None)
    assert stats["post"]==dict(count=1,average=0,over_75_count=0,over_75_percent=0)
