"""Tests for commit retry behavior."""

from __future__ import annotations

import logging
import threading
import time

import pytest

from target_s3tables.retry import CommitRetryConfig, get_table_commit_lock, run_with_commit_retries


class CommitFailedException(Exception):
    """Local stand-in for commit failure."""


class CommitStateUnknownException(Exception):
    """Local stand-in for unknown commit state."""


def _retry_config() -> CommitRetryConfig:
    return CommitRetryConfig(
        num_retries=3,
        min_wait_ms=1,
        max_wait_ms=10,
        total_timeout_ms=10000,
        backoff_multiplier=2.0,
        jitter=0.0,
        status_check_num_retries=2,
        status_check_min_wait_ms=1,
        status_check_max_wait_ms=10,
    )


def test_run_with_commit_retries_retries_conflicts(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"load": 0, "commit": 0}
    sleeps: list[float] = []

    def table_loader() -> object:
        calls["load"] += 1
        return object()

    def commit_fn(_table: object) -> str:
        calls["commit"] += 1
        if calls["commit"] < 3:
            raise CommitFailedException("Requirement failed: branch main has changed")
        return "ok"

    monkeypatch.setattr(time, "sleep", lambda delay: sleeps.append(delay))

    result = run_with_commit_retries(
        operation_name="append",
        table_id=("db", "table"),
        commit_fn=commit_fn,
        table_loader=table_loader,
        config=_retry_config(),
        log=logging.getLogger(__name__),
    )

    assert result == "ok"
    assert calls["commit"] == 3
    assert calls["load"] == 3
    assert len(sleeps) == 2


def test_non_retryable_commit_failed_raises() -> None:
    calls = {"commit": 0}

    def table_loader() -> object:
        return object()

    def commit_fn(_table: object) -> None:
        calls["commit"] += 1
        raise CommitFailedException("Validation failed")

    with pytest.raises(CommitFailedException):
        run_with_commit_retries(
            operation_name="append",
            table_id=("db", "table"),
            commit_fn=commit_fn,
            table_loader=table_loader,
            config=_retry_config(),
            log=logging.getLogger(__name__),
        )

    assert calls["commit"] == 1


def test_unknown_commit_state_status_check_success(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"commit": 0, "status": 0}

    def table_loader() -> object:
        return object()

    def commit_fn(_table: object) -> None:
        calls["commit"] += 1
        raise CommitStateUnknownException("Commit state unknown")

    def status_check(_table: object) -> bool:
        calls["status"] += 1
        return True

    monkeypatch.setattr(time, "sleep", lambda _delay: None)

    run_with_commit_retries(
        operation_name="append",
        table_id=("db", "table"),
        commit_fn=commit_fn,
        table_loader=table_loader,
        config=_retry_config(),
        log=logging.getLogger(__name__),
        status_check_fn=status_check,
    )

    assert calls["commit"] == 1
    assert calls["status"] == 1


def test_unknown_commit_state_retries_after_status_checks(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"commit": 0, "status": 0}

    def table_loader() -> object:
        return object()

    def commit_fn(_table: object) -> str:
        calls["commit"] += 1
        if calls["commit"] == 1:
            raise CommitStateUnknownException("Commit state unknown")
        return "ok"

    def status_check(_table: object) -> bool:
        calls["status"] += 1
        return False

    monkeypatch.setattr(time, "sleep", lambda _delay: None)

    result = run_with_commit_retries(
        operation_name="append",
        table_id=("db", "table"),
        commit_fn=commit_fn,
        table_loader=table_loader,
        config=_retry_config(),
        log=logging.getLogger(__name__),
        status_check_fn=status_check,
    )

    assert result == "ok"
    assert calls["commit"] == 2
    assert calls["status"] == _retry_config().status_check_num_retries


def test_table_commit_lock_serializes() -> None:
    table_id = ("db", "lock_test")
    lock = get_table_commit_lock(table_id)
    entered: list[str] = []
    first_entered = threading.Event()
    release = threading.Event()

    def worker(name: str, hold: bool) -> None:
        with lock:
            entered.append(name)
            if hold:
                first_entered.set()
                release.wait(1)

    thread_one = threading.Thread(target=worker, args=("first", True))
    thread_two = threading.Thread(target=worker, args=("second", False))

    thread_one.start()
    first_entered.wait(1)
    thread_two.start()

    time.sleep(0.05)
    assert entered == ["first"]

    release.set()
    thread_one.join()
    thread_two.join()

    assert entered == ["first", "second"]
