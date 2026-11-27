"""Private, expiring workbook storage while the user decides about AI analysis."""

import os
from pathlib import Path
import re
import secrets
import time

from services.prepost_import_service import MAX_UPLOAD_BYTES, TestImportError

PENDING_TTL = 1800


class PendingUploadError(TestImportError):
    pass


class PendingWorkbooks:
    def __init__(self, directory):
        self.directory = Path(directory)

    def _path(self, upload_id):
        if not isinstance(upload_id, str) or not re.fullmatch(r"[0-9a-f]{48}", upload_id):
            raise PendingUploadError("Invalid pending upload. Upload the workbook again.")
        return self.directory / f"upload_{upload_id}.xlsx"

    def cleanup(self):
        if self.directory.exists():
            for pattern in ("upload_*.xlsx", "processing_*.xlsx"):
                for path in self.directory.glob(pattern):
                    try:
                        if time.time() - path.stat().st_mtime >= PENDING_TTL:
                            path.unlink(missing_ok=True)
                    except FileNotFoundError:
                        pass

    def put(self, data):
        if not data or len(data) > MAX_UPLOAD_BYTES:
            raise PendingUploadError("Choose an .xlsx file of at most 5 MB.")
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.cleanup()
        upload_id = secrets.token_hex(24)
        path = self._path(upload_id)
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(fd, "wb") as output:
                output.write(data)
        except OSError:
            path.unlink(missing_ok=True)
            raise
        return upload_id

    def discard(self, upload_id):
        self._path(upload_id).unlink(missing_ok=True)

    def consume(self, upload_id):
        """Claim once, read and remove. Concurrent/repeated approvals cannot send twice."""
        self.cleanup()
        path = self._path(upload_id)
        claimed = self.directory / f"processing_{upload_id}_{secrets.token_hex(6)}.xlsx"
        try:
            path.rename(claimed)
        except FileNotFoundError as exc:
            raise PendingUploadError("The pending upload expired or was already used. Upload the workbook again.") from exc
        try:
            with claimed.open("rb") as source:
                data = source.read(MAX_UPLOAD_BYTES + 1)
            if not data or len(data) > MAX_UPLOAD_BYTES:
                raise PendingUploadError("The pending workbook changed. Upload it again.")
            return data
        finally:
            claimed.unlink(missing_ok=True)
