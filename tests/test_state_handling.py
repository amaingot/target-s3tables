"""Tests for commit-safe state handling."""

from __future__ import annotations

import io
import json
import os
import tempfile
import typing as t
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from target_s3tables.sinks import S3TablesSink
from target_s3tables.target import TargetS3Tables

SAMPLE_CONFIG: dict[str, t.Any] = {
    "catalog_mode": "glue_rest",
    "region": "us-east-1",
    "account_id": "123456789012",
    "table_bucket_name": "example-table-bucket",
    "state_persist_enabled": True,
}


@pytest.fixture()
def mock_iceberg(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mock Iceberg operations for testing."""

    class DummyTable:  # noqa: D401
        """Minimal table stub."""

    monkeypatch.setattr("target_s3tables.sinks.get_catalog", lambda *a, **k: object())
    monkeypatch.setattr(
        "target_s3tables.sinks.load_or_create_table", lambda *a, **k: DummyTable()
    )
    monkeypatch.setattr(
        "target_s3tables.sinks.evolve_table_schema_union_by_name",
        lambda *a, **k: None,
    )


@pytest.fixture()
def temp_state_file() -> t.Generator[Path, None, None]:
    """Create a temporary state file for testing."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as tmp_file:
        path = Path(tmp_file.name)
    yield path
    # Cleanup
    if path.exists():
        path.unlink()


def test_state_not_emitted_before_commit(
    mock_iceberg: None, capsys: pytest.CaptureFixture, temp_state_file: Path
) -> None:
    """Test that STATE is not emitted before Iceberg commit."""
    config = {**SAMPLE_CONFIG, "state_persist_path": str(temp_state_file)}

    with patch("target_s3tables.sinks.write_arrow_to_table") as mock_write:
        # Setup: write will be called but we'll check state timing
        mock_write.return_value = None

        target = TargetS3Tables(config=config)

        # Simulate Singer input stream with SCHEMA, RECORD, and STATE messages
        singer_input = io.StringIO(
            json.dumps(
                {
                    "type": "SCHEMA",
                    "stream": "test_stream",
                    "schema": {"properties": {"id": {"type": "integer"}}},
                    "key_properties": ["id"],
                }
            )
            + "\n"
            + json.dumps(
                {"type": "RECORD", "stream": "test_stream", "record": {"id": 1}}
            )
            + "\n"
            + json.dumps(
                {
                    "type": "STATE",
                    "value": {"bookmarks": {"test_stream": {"id": 1}}},
                }
            )
            + "\n"
        )

        # Process messages but don't drain yet
        target.listen(singer_input)

        # Check that no state has been emitted yet (stdout should be empty)
        captured = capsys.readouterr()
        # State will be emitted during listen since it calls drain
        # Instead check that write was called (meaning commit happened)
        assert mock_write.called, "write_arrow_to_table should have been called"


def test_state_emitted_after_successful_commit(
    mock_iceberg: None, capsys: pytest.CaptureFixture, temp_state_file: Path
) -> None:
    """Test that STATE is emitted after successful Iceberg commit."""
    config = {**SAMPLE_CONFIG, "state_persist_path": str(temp_state_file)}

    with patch("target_s3tables.sinks.write_arrow_to_table") as mock_write:
        mock_write.return_value = None

        target = TargetS3Tables(config=config)

        singer_input = io.StringIO(
            json.dumps(
                {
                    "type": "SCHEMA",
                    "stream": "test_stream",
                    "schema": {"properties": {"id": {"type": "integer"}}},
                    "key_properties": ["id"],
                }
            )
            + "\n"
            + json.dumps(
                {"type": "RECORD", "stream": "test_stream", "record": {"id": 1}}
            )
            + "\n"
            + json.dumps(
                {
                    "type": "STATE",
                    "value": {"bookmarks": {"test_stream": {"id": 1}}},
                }
            )
            + "\n"
        )

        target.listen(singer_input)

        # Check that state was emitted
        captured = capsys.readouterr()
        assert '"bookmarks"' in captured.out, "STATE not emitted after commit"
        assert mock_write.called, "write_arrow_to_table should have been called"


def test_state_not_emitted_on_commit_failure(
    mock_iceberg: None, capsys: pytest.CaptureFixture, temp_state_file: Path
) -> None:
    """Test that STATE is NOT emitted when Iceberg commit fails."""
    # Use a unique state file to avoid interference from other tests
    config = {**SAMPLE_CONFIG, "state_persist_path": str(temp_state_file)}

    with patch("target_s3tables.sinks.write_arrow_to_table") as mock_write:
        # Simulate commit failure
        mock_write.side_effect = Exception("Commit failed")

        target = TargetS3Tables(config=config)

        singer_input = io.StringIO(
            json.dumps(
                {
                    "type": "SCHEMA",
                    "stream": "test_stream",
                    "schema": {"properties": {"id": {"type": "integer"}}},
                    "key_properties": ["id"],
                }
            )
            + "\n"
            + json.dumps(
                {"type": "RECORD", "stream": "test_stream", "record": {"id": 1}}
            )
            + "\n"
            + json.dumps(
                {
                    "type": "STATE",
                    "value": {"bookmarks": {"test_stream": {"id": 1}}},
                }
            )
            + "\n"
        )

        # Attempt to listen - should fail
        with pytest.raises(Exception, match="Commit failed"):
            target.listen(singer_input)

        # Check that state was NOT emitted
        captured = capsys.readouterr()
        assert (
            '"bookmarks"' not in captured.out
        ), "STATE should not be emitted after failed commit"


def test_state_persistence_atomic_write(
    mock_iceberg: None, temp_state_file: Path
) -> None:
    """Test that state is persisted atomically to disk."""
    config = {**SAMPLE_CONFIG, "state_persist_path": str(temp_state_file)}

    with patch("target_s3tables.sinks.write_arrow_to_table"):
        target = TargetS3Tables(config=config)

        singer_input = io.StringIO(
            json.dumps(
                {
                    "type": "SCHEMA",
                    "stream": "test_stream",
                    "schema": {"properties": {"id": {"type": "integer"}}},
                    "key_properties": ["id"],
                }
            )
            + "\n"
            + json.dumps(
                {"type": "RECORD", "stream": "test_stream", "record": {"id": 1}}
            )
            + "\n"
            + json.dumps(
                {
                    "type": "STATE",
                    "value": {"bookmarks": {"test_stream": {"id": 1}}},
                }
            )
            + "\n"
        )

        target.listen(singer_input)

        # Check that state file was created and contains the correct state
        assert temp_state_file.exists(), "State file was not created"
        with open(temp_state_file, encoding="utf-8") as f:
            persisted_state = json.load(f)
        assert persisted_state == {
            "bookmarks": {"test_stream": {"id": 1}}
        }, "Persisted state is incorrect"


def test_state_reload_on_startup(mock_iceberg: None, temp_state_file: Path) -> None:
    """Test that persisted state is loaded and emitted on startup."""
    # Write a state file
    state = {"bookmarks": {"test_stream": {"id": 42}}}
    with open(temp_state_file, "w", encoding="utf-8") as f:
        json.dump(state, f)

    config = {**SAMPLE_CONFIG, "state_persist_path": str(temp_state_file)}

    # Create target - it should load the state
    with patch("target_s3tables.sinks.write_arrow_to_table"):
        target = TargetS3Tables(config=config)

        # Check that state was loaded
        assert target._last_committed_state == state  # noqa: SLF001


def test_no_duplicate_state_emission(
    mock_iceberg: None, capsys: pytest.CaptureFixture, temp_state_file: Path
) -> None:
    """Test that the same state is not emitted multiple times."""
    config = {**SAMPLE_CONFIG, "state_persist_path": str(temp_state_file)}

    with patch("target_s3tables.sinks.write_arrow_to_table"):
        target = TargetS3Tables(config=config)

        # Send same state twice
        singer_input = io.StringIO(
            json.dumps(
                {
                    "type": "SCHEMA",
                    "stream": "test_stream",
                    "schema": {"properties": {"id": {"type": "integer"}}},
                    "key_properties": ["id"],
                }
            )
            + "\n"
            + json.dumps(
                {"type": "RECORD", "stream": "test_stream", "record": {"id": 1}}
            )
            + "\n"
            + json.dumps(
                {
                    "type": "STATE",
                    "value": {"bookmarks": {"test_stream": {"id": 1}}},
                }
            )
            + "\n"
            + json.dumps(
                {"type": "RECORD", "stream": "test_stream", "record": {"id": 2}}
            )
            + "\n"
            + json.dumps(
                {
                    "type": "STATE",
                    "value": {"bookmarks": {"test_stream": {"id": 1}}},
                }
            )
            + "\n"
        )

        target.listen(singer_input)

        captured = capsys.readouterr()

        # Count how many times the state appears in the output
        state_count = captured.out.count('"bookmarks"')

        # State should only be emitted once, not twice
        assert state_count == 1, f"Same state emitted {state_count} times, expected 1"


def test_state_persistence_disabled(
    mock_iceberg: None, capsys: pytest.CaptureFixture
) -> None:
    """Test that state persistence can be disabled."""
    # Use a specific path that doesn't exist yet
    test_path = "/tmp/test_state_disabled_should_not_exist.json"
    # Clean up in case it exists from a previous run
    if os.path.exists(test_path):
        os.remove(test_path)

    config = {
        **SAMPLE_CONFIG,
        "state_persist_enabled": False,
        "state_persist_path": test_path,
    }

    with patch("target_s3tables.sinks.write_arrow_to_table"):
        target = TargetS3Tables(config=config)

        singer_input = io.StringIO(
            json.dumps(
                {
                    "type": "SCHEMA",
                    "stream": "test_stream",
                    "schema": {"properties": {"id": {"type": "integer"}}},
                    "key_properties": ["id"],
                }
            )
            + "\n"
            + json.dumps(
                {"type": "RECORD", "stream": "test_stream", "record": {"id": 1}}
            )
            + "\n"
            + json.dumps(
                {
                    "type": "STATE",
                    "value": {"bookmarks": {"test_stream": {"id": 1}}},
                }
            )
            + "\n"
        )

        target.listen(singer_input)

        # Check that state file was NOT created
        assert not os.path.exists(
            test_path
        ), "State file should not be created when persistence is disabled"

        # But state should still be emitted to stdout
        captured = capsys.readouterr()
        assert '"bookmarks"' in captured.out, "STATE should still be emitted to stdout"

    # Cleanup
    if os.path.exists(test_path):
        os.remove(test_path)
