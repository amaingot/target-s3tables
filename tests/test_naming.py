"""Unit tests for stream/table naming and sanitization."""

from __future__ import annotations

from target_s3tables.config import ParsedConfig
from target_s3tables.iceberg import sanitize_identifier, table_identifier_for_stream


def test_sanitize_identifier_basic() -> None:
    assert sanitize_identifier("My Table") == "my_table"
    assert sanitize_identifier("  spaces  ") == "spaces"
    assert sanitize_identifier("9lives") == "_9lives"


def test_table_identifier_for_stream_mapping_and_prefix() -> None:
    cfg = ParsedConfig.from_mapping(
        {
            "catalog_mode": "glue_rest",
            "region": "us-east-1",
            "namespace": "My DB",
            "account_id": "123456789012",
            "table_bucket_name": "mybucket",
            "sanitize_names": True,
            "table_name_prefix": "pre-",
            "table_name_mapping": {"animals": "Animal Table"},
        },
    )

    table_id = table_identifier_for_stream(cfg, stream_name="animals")
    assert table_id == ("my_db", "pre_animal_table")

