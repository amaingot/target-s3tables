"""Unit tests for the retry helper and retriable-exception classification."""

from __future__ import annotations

import logging

import pytest
from pyiceberg.exceptions import CommitFailedException

from target_s3tables.iceberg import _is_retriable_exception, retry


class _HttpError(Exception):
    """Minimal stand-in for a transient HTTP error with a status_code."""

    def __init__(self, status_code: int, message: str = "boom") -> None:
        super().__init__(message)
        self.status_code = status_code


def test_commit_failed_exception_is_retriable() -> None:
    exc = CommitFailedException("branch main has changed")
    assert _is_retriable_exception(exc) is True


def test_retry_recovers_from_commit_conflict_and_calls_before_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("target_s3tables.iceberg.time.sleep", lambda _s: None)

    attempts = {"n": 0}
    seen_excs: list[BaseException] = []

    def flaky() -> str:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise CommitFailedException("branch main has changed")
        return "ok"

    def before_retry(exc: BaseException) -> None:
        seen_excs.append(exc)

    result = retry(
        flaky,
        log=logging.getLogger("test"),
        op="append",
        max_attempts=5,
        base_delay_s=0.0,
        max_delay_s=0.0,
        before_retry=before_retry,
    )

    assert result == "ok"
    assert attempts["n"] == 3
    # before_retry runs once before each retry, not before the first attempt,
    # and receives the caught exception each time.
    assert len(seen_excs) == 2
    assert all(isinstance(e, CommitFailedException) for e in seen_excs)


def test_retry_gives_up_after_max_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("target_s3tables.iceberg.time.sleep", lambda _s: None)

    def always_conflict() -> None:
        raise CommitFailedException("branch main has changed")

    with pytest.raises(CommitFailedException):
        retry(
            always_conflict,
            log=logging.getLogger("test"),
            op="append",
            max_attempts=3,
            base_delay_s=0.0,
            max_delay_s=0.0,
        )


def test_retry_passes_http_error_to_before_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """before_retry should receive transient HTTP errors as well, so the
    callback can decide whether the exception is one it cares about."""
    monkeypatch.setattr("target_s3tables.iceberg.time.sleep", lambda _s: None)

    attempts = {"n": 0}
    seen_excs: list[BaseException] = []

    def flaky() -> str:
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise _HttpError(503, "service unavailable")
        return "ok"

    result = retry(
        flaky,
        log=logging.getLogger("test"),
        op="append",
        max_attempts=5,
        base_delay_s=0.0,
        max_delay_s=0.0,
        before_retry=seen_excs.append,
    )

    assert result == "ok"
    assert len(seen_excs) == 1
    assert isinstance(seen_excs[0], _HttpError)
    assert seen_excs[0].status_code == 503


# Adapted from amaingot/target-s3tables#44 (key and bucket shortened) so the
# substring check is exercised end-to-end against a realistic PyArrow error
# string.
_CRC64NVME_MESSAGE = (
    "When uploading part for key 'data/00000-0-abc.parquet' in bucket "
    "'x--table-s3': AWS Error UNKNOWN (HTTP status 400) during UploadPart "
    "operation: Unable to parse ExceptionName: BadDigest Message: The "
    "CRC64NVME you specified did not match the calculated checksum."
)


def test_pyarrow_crc64nvme_oserror_is_retriable() -> None:
    exc = OSError(_CRC64NVME_MESSAGE)
    assert _is_retriable_exception(exc) is True


def test_pyarrow_baddigest_oserror_is_retriable() -> None:
    exc = OSError("BadDigest: digest mismatch on UploadPart")
    assert _is_retriable_exception(exc) is True


def test_unrelated_oserror_is_not_retriable() -> None:
    """Regression guard: only the CRC64NVME / BadDigest signature should retry.

    Blanket OSError retries would swallow fatal errors like disk-full or bad
    credentials and burn 8 attempts of backoff on each.
    """
    assert _is_retriable_exception(OSError("No space left on device")) is False
    assert _is_retriable_exception(OSError("Permission denied")) is False


def test_retry_recovers_from_crc64nvme_oserror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("target_s3tables.iceberg.time.sleep", lambda _s: None)

    attempts = {"n": 0}

    def flaky() -> str:
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise OSError(_CRC64NVME_MESSAGE)
        return "ok"

    result = retry(
        flaky,
        log=logging.getLogger("test"),
        op="append",
        max_attempts=5,
        base_delay_s=0.0,
        max_delay_s=0.0,
    )

    assert result == "ok"
    assert attempts["n"] == 2
