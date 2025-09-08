# tests/conftest.py
import sys
import types
from pathlib import Path

# Ensure project root is on sys.path
sys.path.append(str(Path(__file__).resolve().parents[1]))

# Create a dummy module for config.database
import types
def _make_dummy_db():
    class DummyCollection:
        def find_one(self, *args, **kwargs):
            return None
        def create_index(self, *args, **kwargs):
            pass
    class DummyMongoConn:
        def __getitem__(self, name):
            return DummyCollection()
        def collection(self, name):
            return DummyCollection()
    return DummyMongoConn()

dummy_module = types.ModuleType("config.database")
dummy_module.mongodb = _make_dummy_db()

sys.modules["config.database"] = dummy_module
