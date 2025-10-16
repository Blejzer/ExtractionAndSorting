import importlib.metadata
import sys
import types
from pathlib import Path

# Ensure project root is on sys.path
sys.path.append(str(Path(__file__).resolve().parents[1]))


# Create a dummy module for config.database

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


dummy_db_module = types.ModuleType("config.database")
dummy_db_module.mongodb = _make_dummy_db()

sys.modules["config.database"] = dummy_db_module


# Provide a lightweight stub for the optional email_validator dependency used by Pydantic.
email_validator_module = types.ModuleType("email_validator")


class EmailNotValidError(ValueError):
    """Exception raised when an email address fails validation."""


def _validate_email(address: str, *_args, **_kwargs):
    if "@" not in address or address.startswith("@") or address.endswith("@"):
        raise EmailNotValidError("Invalid email address")
    local_part, _, domain = address.partition("@")

    class Result:
        def __init__(self, email: str, local: str, domain: str) -> None:
            self.email = email
            self.original_email = email
            self.normalized = email
            self.local_part = local
            self.domain = domain
            self.domain_i18n = domain
            self.ascii_email = email

    return Result(address, local_part, domain)


def _canonicalize_email(address: str, *_args, **_kwargs) -> str:
    return _validate_email(address).email


email_validator_module.EmailNotValidError = EmailNotValidError
email_validator_module.validate_email = _validate_email
email_validator_module.canonicalize_email = _canonicalize_email

sys.modules["email_validator"] = email_validator_module


# Patch importlib.metadata.version so that pydantic's optional dependency check succeeds.
_original_version = importlib.metadata.version


def _version(name: str, *_args, **_kwargs) -> str:
    if name == "email-validator":
        return "2.0.0"
    return _original_version(name, *_args, **_kwargs)


importlib.metadata.version = _version
