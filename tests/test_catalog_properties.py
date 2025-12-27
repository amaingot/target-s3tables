"""Unit tests for catalog property generation."""

from __future__ import annotations

import logging
from unittest.mock import patch

from target_s3tables.config import ParsedConfig
from target_s3tables.iceberg import clear_catalog_cache, get_catalog


def test_get_catalog_props_glue_rest() -> None:
    config = ParsedConfig.from_mapping(
        {
            "catalog_mode": "glue_rest",
            "region": "us-east-1",
            "namespace": "default",
            "account_id": "123456789012",
            "table_bucket_name": "mybucket",
        },
    )

    clear_catalog_cache()
    with patch("target_s3tables.iceberg.load_catalog") as mocked:
        mocked.return_value = object()
        _ = get_catalog(config, log=logging.getLogger("test"))

        _, kwargs = mocked.call_args
        assert kwargs["type"] == "rest"
        assert kwargs["uri"] == "https://glue.us-east-1.amazonaws.com/iceberg"
        assert kwargs["warehouse"] == "123456789012:s3tablescatalog/mybucket"
        assert kwargs["rest.sigv4-enabled"] == "true"
        assert kwargs["rest.signing-name"] == "glue"
        assert kwargs["rest.signing-region"] == "us-east-1"


def test_get_catalog_props_s3tables_rest_has_prefix() -> None:
    arn = "arn:aws:s3tables:us-east-1:123456789012:bucket/mybucket"
    config = ParsedConfig.from_mapping(
        {
            "catalog_mode": "s3tables_rest",
            "region": "us-east-1",
            "namespace": "default",
            "table_bucket_arn": arn,
        },
    )

    clear_catalog_cache()
    with patch("target_s3tables.iceberg.load_catalog") as mocked:
        mocked.return_value = object()
        _ = get_catalog(config, log=logging.getLogger("test"))

        _, kwargs = mocked.call_args
        assert kwargs["type"] == "rest"
        assert kwargs["uri"] == "https://s3tables.us-east-1.amazonaws.com/iceberg"
        assert kwargs["warehouse"] == arn
        assert kwargs["rest.sigv4-enabled"] == "true"
        assert kwargs["rest.signing-name"] == "s3tables"
        assert kwargs["rest.signing-region"] == "us-east-1"
        assert kwargs["prefix"] and "arn%3Aaws%3As3tables" in kwargs["prefix"]

