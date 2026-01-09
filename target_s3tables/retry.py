"""Commit retry utilities for Iceberg operations."""

from __future__ import annotations

import logging
import random
import threading
import time
import typing as t
from dataclasses import dataclass

try:  # pragma: no cover - optional dependency details
    from pyiceberg.exceptions import CommitFailedException, CommitStateUnknownException
except Exception:  # noqa: BLE001
    CommitFailedException = None  # type: ignore[assignment]
    CommitStateUnknownException = None  # type: ignore[assignment]


_UNKNOWN_STATE_PATTERNS = (
    "commit state unknown",
    "unknown commit state",
    "commitstateunknown",
)

_NETWORK_ERROR_NAMES = (
    "timeout",
    "connectionerror",
    "connectionreset",
    "connecttimeout",
    "readtimeout",
    "chunkedencodingerror",
    "protocolerror",
    "ssLError",
)


@dataclass(frozen=True)
class CommitRetryConfig:  # pylint: disable=too-many-instance-attributes
    """Retry tuning for Iceberg commit operations."""

    num_retries: int
    min_wait_ms: int
    max_wait_ms: int
    total_timeout_ms: int
    backoff_multiplier: float
    jitter: float
    status_check_num_retries: int
    status_check_min_wait_ms: int
    status_check_max_wait_ms: int
    max_attempts_override: int | None = None

    def max_attempts(self) -> int:
        attempts = max(1, self.num_retries + 1)
        if self.max_attempts_override is not None:
            attempts = min(attempts, max(1, self.max_attempts_override))
        return attempts


_TABLE_LOCKS: dict[str, threading.RLock] = {}
_TABLE_LOCKS_GUARD = threading.Lock()


def get_table_commit_lock(table_id: tuple[str, ...] | str) -> threading.RLock:
    """Return a per-table lock for commit serialization within the process."""
    if isinstance(table_id, tuple):
        key = ".".join(table_id)
    else:
        key = str(table_id)
    with _TABLE_LOCKS_GUARD:
        lock = _TABLE_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _TABLE_LOCKS[key] = lock
        return lock


def compute_backoff(
    attempt: int,
    *,
    min_wait_ms: int,
    max_wait_ms: int,
    multiplier: float,
    jitter: float,
) -> float:
    """Return exponential backoff (seconds) with jitter."""
    attempt = max(1, attempt)
    base_ms = min_wait_ms * (multiplier ** (attempt - 1))
    base_ms = min(base_ms, max_wait_ms)
    if jitter:
        jitter_factor = 1.0 + random.uniform(-jitter, jitter)
    else:
        jitter_factor = 1.0
    delay_ms = max(0.0, base_ms * jitter_factor)
    return delay_ms / 1000.0


def is_retryable_commit_failed(exc: Exception) -> bool:
    """Return True if the exception looks like a commit conflict."""
    if not _is_commit_failed_exception(exc):
        return False
    message = str(exc).lower()
    if not message:
        return False
    if "branch" in message and ("has changed" in message or "was created concurrently" in message):
        return True
    if "expected id" in message and ("branch" in message or "snapshot" in message):
        return True
    if "expected snapshot" in message:
        return True
    if "reference has changed" in message:
        return True
    if "requirement failed" in message and any(token in message for token in ("branch", "ref")):
        return True
    return False


def is_unknown_commit_state(exc: Exception) -> bool:
    """Return True if the exception indicates commit state uncertainty."""
    if _is_commit_failed_exception(exc):
        return False
    if _is_commit_state_unknown_exception(exc):
        return True
    message = str(exc).lower()
    if any(token in message for token in _UNKNOWN_STATE_PATTERNS):
        return True
    return _is_transport_or_server_error(exc)


def run_with_commit_retries(  # noqa: PLR0913
    *,
    operation_name: str,
    table_id: tuple[str, ...] | str,
    commit_fn: t.Callable[[t.Any], t.Any],
    table_loader: t.Callable[[], t.Any],
    config: CommitRetryConfig,
    log: logging.Logger,
    status_check_fn: t.Callable[[t.Any], bool] | None = None,
) -> t.Any:
    """Run a commit operation with Iceberg-style retries and status checks."""
    start = time.monotonic()
    attempt = 0
    max_attempts = config.max_attempts()
    table_name = ".".join(table_id) if isinstance(table_id, tuple) else str(table_id)

    while True:
        attempt += 1
        table = table_loader()
        snapshot_id = _current_snapshot_id(table)
        try:
            result = commit_fn(table)
            if attempt > 1:
                elapsed_s = time.monotonic() - start
                log.info(
                    "Commit %s succeeded for table '%s' after %d attempts in %.2fs.",
                    operation_name,
                    table_name,
                    attempt,
                    elapsed_s,
                )
            return result
        except Exception as exc:  # noqa: BLE001
            if is_retryable_commit_failed(exc):
                if _retry_budget_exhausted(attempt, start, config):
                    raise _retry_budget_error(
                        operation_name,
                        table_name,
                        attempt,
                        start,
                        exc,
                    ) from exc
                delay = compute_backoff(
                    attempt,
                    min_wait_ms=config.min_wait_ms,
                    max_wait_ms=config.max_wait_ms,
                    multiplier=config.backoff_multiplier,
                    jitter=config.jitter,
                )
                log.warning(
                    "Commit conflict on %s for table '%s' (attempt %d/%d, snapshot=%s): %s. "
                    "Retrying in %.2fs.",
                    operation_name,
                    table_name,
                    attempt,
                    max_attempts,
                    snapshot_id,
                    _format_exception_short(exc),
                    delay,
                )
                time.sleep(delay)
                continue

            if is_unknown_commit_state(exc):
                log.warning(
                    "Commit state unknown on %s for table '%s' (attempt %d/%d, snapshot=%s): %s.",
                    operation_name,
                    table_name,
                    attempt,
                    max_attempts,
                    snapshot_id,
                    _format_exception_short(exc),
                )
                if status_check_fn is not None:
                    if _run_status_checks(
                        status_check_fn,
                        table_loader,
                        config,
                        log,
                        operation_name=operation_name,
                        table_name=table_name,
                    ):
                        log.info(
                            "Commit %s for table '%s' verified by status check.",
                            operation_name,
                            table_name,
                        )
                        return None

                if _retry_budget_exhausted(attempt, start, config):
                    raise _retry_budget_error(
                        operation_name,
                        table_name,
                        attempt,
                        start,
                        exc,
                    ) from exc

                delay = compute_backoff(
                    attempt,
                    min_wait_ms=config.min_wait_ms,
                    max_wait_ms=config.max_wait_ms,
                    multiplier=config.backoff_multiplier,
                    jitter=config.jitter,
                )
                log.warning(
                    "Retrying %s for table '%s' after unknown commit state in %.2fs.",
                    operation_name,
                    table_name,
                    delay,
                )
                time.sleep(delay)
                continue

            raise


def _retry_budget_exhausted(attempt: int, start: float, config: CommitRetryConfig) -> bool:
    if attempt >= config.max_attempts():
        return True
    elapsed_ms = (time.monotonic() - start) * 1000.0
    return elapsed_ms >= config.total_timeout_ms


def _retry_budget_error(
    operation_name: str,
    table_name: str,
    attempt: int,
    start: float,
    exc: Exception,
) -> RuntimeError:
    elapsed_s = time.monotonic() - start
    msg = (
        f"Iceberg commit retries exhausted for '{table_name}' on '{operation_name}' "
        f"after {attempt} attempts in {elapsed_s:.2f}s. Last error: {exc}"
    )
    return RuntimeError(msg)


def _run_status_checks(  # noqa: PLR0913
    status_check_fn: t.Callable[[t.Any], bool],
    table_loader: t.Callable[[], t.Any],
    config: CommitRetryConfig,
    log: logging.Logger,
    *,
    operation_name: str,
    table_name: str,
) -> bool:
    for attempt in range(1, config.status_check_num_retries + 1):
        table = table_loader()
        if status_check_fn(table):
            return True
        if attempt >= config.status_check_num_retries:
            break
        delay = compute_backoff(
            attempt,
            min_wait_ms=config.status_check_min_wait_ms,
            max_wait_ms=config.status_check_max_wait_ms,
            multiplier=config.backoff_multiplier,
            jitter=config.jitter,
        )
        log.info(
            "Status check %d/%d for %s on table '%s' not confirmed. Sleeping %.2fs.",
            attempt,
            config.status_check_num_retries,
            operation_name,
            table_name,
            delay,
        )
        time.sleep(delay)
    return False


def _is_commit_failed_exception(exc: Exception) -> bool:
    if CommitFailedException is not None and isinstance(exc, CommitFailedException):
        return True
    return exc.__class__.__name__.lower() == "commitfailedexception"


def _is_commit_state_unknown_exception(exc: Exception) -> bool:
    if CommitStateUnknownException is not None and isinstance(exc, CommitStateUnknownException):
        return True
    return exc.__class__.__name__.lower() in {
        "commitstateunknownexception",
        "commitstateunknownerror",
    }


def _is_transport_or_server_error(exc: Exception) -> bool:
    status_code = getattr(exc, "status_code", None)
    if status_code is None and hasattr(exc, "response"):
        status_code = getattr(getattr(exc, "response"), "status_code", None)
    if isinstance(status_code, int) and status_code in {500, 502, 503, 504}:
        return True

    for inner in _exception_chain(exc):
        name = inner.__class__.__name__.lower()
        if any(token in name for token in _NETWORK_ERROR_NAMES):
            return True
        message = str(inner).lower()
        if "timed out" in message or "connection reset" in message:
            return True
        if "connection aborted" in message or "broken pipe" in message:
            return True
    return False


def _exception_chain(exc: Exception) -> t.Iterator[Exception]:
    seen: set[int] = set()
    current: Exception | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = t.cast(Exception | None, current.__cause__ or current.__context__)


def _format_exception_short(exc: Exception) -> str:
    return f"{exc.__class__.__name__}: {exc}"


def _current_snapshot_id(table: t.Any) -> str | None:
    snapshot_id = None
    candidate = getattr(table, "current_snapshot_id", None)
    if callable(candidate):
        try:
            snapshot_id = candidate()
        except Exception:  # noqa: BLE001
            snapshot_id = None
    else:
        snapshot_id = candidate

    if snapshot_id is not None:
        return str(snapshot_id)

    candidate = getattr(table, "current_snapshot", None)
    if callable(candidate):
        try:
            snapshot = candidate()
        except Exception:  # noqa: BLE001
            snapshot = None
    else:
        snapshot = candidate

    if snapshot is None:
        return None

    snapshot_id = getattr(snapshot, "snapshot_id", None)
    if callable(snapshot_id):
        try:
            snapshot_id = snapshot_id()
        except Exception:  # noqa: BLE001
            snapshot_id = None
    return str(snapshot_id) if snapshot_id is not None else None
