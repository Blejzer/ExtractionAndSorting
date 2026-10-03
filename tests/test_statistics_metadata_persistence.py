from domain.models.event import Event
import services.events_service as events_service


def test_reset_overrides_writes_explicit_nulls_to_mongo(monkeypatch):
    existing = Event(eid="E1", title="Event", training_areas=["crypto"], invited_countries=["AL"], expected_per_country=4)
    class Repo:
        def find_by_eid(self, eid):
            return existing
        def update(self, eid, payload):
            document = existing.to_mongo()
            document.update(payload)
            return Event.from_mongo(document)
    monkeypatch.setattr(events_service, "_repo", Repo())
    updated = events_service.update_event("E1", {"training_areas": None, "invited_countries": None, "expected_per_country": None})
    assert updated.training_areas is None
    assert updated.invited_countries is None
    assert updated.expected_per_country is None


def test_unrelated_event_edit_preserves_reporting_metadata(monkeypatch):
    existing = Event(eid="E1", title="Event", training_areas=["crypto"], invited_countries=["AL"], expected_per_country=4)
    class Repo:
        def find_by_eid(self, eid):
            return existing
        def update(self, eid, payload):
            return Event.from_mongo(payload)
    monkeypatch.setattr(events_service, "_repo", Repo())
    updated = events_service.update_event("E1", {"title": "Updated"})
    assert updated.title == "Updated"
    assert updated.training_areas == ["crypto"]
    assert updated.invited_countries == ["AL"]
