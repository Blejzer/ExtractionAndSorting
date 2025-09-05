import os
import sys

# Ensure the project root is on sys.path for importing the services package
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def test_fetch_main_stats(monkeypatch):
    """Verify that fetch_main_stats aggregates counts and the latest event."""

    class DummyCursor:
        def __init__(self, doc):
            self.doc = doc

        def sort(self, *_, **__):
            return self

        def limit(self, *_):
            return iter([self.doc] if self.doc else [])

    class DummyCollection:
        def __init__(self, count, doc=None):
            self._count = count
            self._doc = doc

        def count_documents(self, *_):
            return self._count

        def find(self, *_):
            return DummyCursor(self._doc)

    class DummyDB:
        def __init__(self):
            self.data = {
                "participants": DummyCollection(10),
                "events": DummyCollection(3, {"title": "Test Event", "dateFrom": "2024-05-01"}),
                "countries": DummyCollection(2),
            }

        def __getitem__(self, name):
            return self.data[name]

    class DummyMongo:
        def db(self):
            return DummyDB()

    import services.main_service as main_service
    monkeypatch.setattr(main_service, "mongodb", DummyMongo())

    stats = main_service.fetch_main_stats()

    assert stats["participants"] == 10
    assert stats["events"] == 3
    assert stats["countries"] == 2
    assert stats["latest_event"] == "Test Event"
    assert stats["latest_event_date"] == "2024-05-01"

