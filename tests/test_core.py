"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import typing as t

import pytest
from singer_sdk.testing import get_target_test_class

from target_s3tables.target import TargetS3Tables

SAMPLE_CONFIG: dict[str, t.Any] = {
    # Dummy-but-valid config. Iceberg calls are monkeypatched in the test class below.
    "catalog_mode": "glue_rest",
    "region": "us-east-1",
    "account_id": "123456789012",
    "table_bucket_name": "example-table-bucket",
}


# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(
    target_class=TargetS3Tables,
    config=SAMPLE_CONFIG,
)


class TestTargetS3Tables(StandardTargetTests):  # type: ignore[misc, valid-type]
    """Standard Target Tests."""

    @pytest.fixture(autouse=True)
    def _mock_iceberg(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Disable real AWS/Iceberg calls for the SDK test suite."""

        class DummyTable:  # noqa: D401
            """Minimal table stub."""

        monkeypatch.setattr("target_s3tables.sinks.get_catalog", lambda *a, **k: object())
        monkeypatch.setattr("target_s3tables.sinks.load_or_create_table", lambda *a, **k: DummyTable())
        monkeypatch.setattr(
            "target_s3tables.sinks.evolve_table_schema_union_by_name",
            lambda *a, **k: None,
        )
        monkeypatch.setattr("target_s3tables.sinks.write_arrow_to_table", lambda *a, **k: None)

    @pytest.fixture(scope="class")
    def resource(self):  # noqa: ANN201
        """Generic external resource.

        This fixture is useful for setup and teardown of external resources,
        such output folders, tables, buckets etc. for use during testing.

        Example usage can be found in the SDK samples test suite:
        https://github.com/meltano/sdk/tree/main/tests/packages
        """
        return "resource"


# TODO: Create additional tests as appropriate for your target.
