import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app import create_app
import services.participant_event_service as svc
import routes.participant_events as pe_routes


class DummyParticipant:
    def __init__(self, pid):
        self.pid = pid

    def model_dump(self):
        return {"pid": self.pid}


def test_api_participants_for_event(monkeypatch):
    def dummy_func(eid):
        return {"participants": [DummyParticipant("P1")], "avg_pre": 80, "avg_post": 90}

    monkeypatch.setattr(svc, "event_participants_with_scores", dummy_func)
    monkeypatch.setattr(pe_routes, "event_participants_with_scores", dummy_func)

    app = create_app()
    client = app.test_client()

    resp = client.get("/api/participant-events/event/E1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["participants"][0]["pid"] == "P1"
    assert data["avg_post"] == 90
