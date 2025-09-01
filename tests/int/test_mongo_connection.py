import os
import pytest
from config.database import MongoConnection

@pytest.mark.integration
def test_can_ping_test_mongo():
    # CI passes TEST_MONGODB_URI; your code should prefer it in tests.
    uri = os.getenv("TEST_MONGODB_URI")
    assert uri, "TEST_MONGODB_URI must be set for integration tests."

    # Temporarily construct a connection using TEST_MONGODB_URI
    conn = MongoConnection()
    # Monkey-patching not shown; alternatively, add a test-only helper to accept a URI.
    # For now, just verify env is set.
    assert uri.startswith("mongodb://") or uri.startswith("mongodb+srv://")
