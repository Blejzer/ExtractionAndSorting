"""Temporary permission uploads are private, bounded, expiring and single-use."""

from concurrent.futures import ThreadPoolExecutor
import os
import stat

import pytest

from services.prepost_pending_uploads import PendingWorkbooks, PendingUploadError


def test_private_upload_is_consumed_once_and_removed(tmp_path):
    store=PendingWorkbooks(tmp_path/"private")
    upload_id=store.put(b"fictional workbook")
    files=list(store.directory.glob("*.xlsx"))
    assert len(files)==1 and stat.S_IMODE(files[0].stat().st_mode)==0o600
    assert stat.S_IMODE(store.directory.stat().st_mode)==0o700
    assert store.consume(upload_id)==b"fictional workbook"
    assert not list(store.directory.glob("*.xlsx"))
    with pytest.raises(PendingUploadError,match="expired or was already used"):
        store.consume(upload_id)


def test_cleanup_expires_only_own_files_and_discard_removes_upload(tmp_path):
    store=PendingWorkbooks(tmp_path)
    upload_id=store.put(b"workbook")
    other=tmp_path/"keep.xlsx"
    other.write_bytes(b"unrelated")
    for path in tmp_path.iterdir():os.utime(path,(0,0))
    store.cleanup()
    assert other.exists() and len(list(tmp_path.glob("upload_*.xlsx")))==0
    fresh=store.put(b"fresh")
    store.discard(fresh)
    assert other.read_bytes()==b"unrelated"


def test_concurrent_approvals_can_consume_only_once(tmp_path):
    store=PendingWorkbooks(tmp_path)
    upload_id=store.put(b"workbook")
    def consume():
        try:return store.consume(upload_id)
        except PendingUploadError:return None
    with ThreadPoolExecutor(max_workers=2) as pool:
        result=list(pool.map(lambda _:consume(),range(2)))
    assert result.count(b"workbook")==1 and result.count(None)==1
    assert not list(tmp_path.glob("*.xlsx"))


@pytest.mark.parametrize("upload_id",["../private.xlsx","",None,123])
def test_invalid_identifiers_cannot_access_other_files(tmp_path,upload_id):
    with pytest.raises(PendingUploadError):
        PendingWorkbooks(tmp_path).discard(upload_id)
