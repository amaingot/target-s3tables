"""Unit tests for config validation."""

from __future__ import annotations

import pytest
from singer_sdk.exceptions import ConfigValidationError

from target_s3tables.config import validate_config
from target_s3tables.target import TargetS3Tables


def test_validate_config_glue_requires_warehouse_or_parts() -> None:
    with pytest.raises(ValueError, match="glue_warehouse"):
        validate_config({"catalog_mode": "glue_rest", "region": "us-east-1"})


def test_validate_config_s3tables_requires_arn() -> None:
    with pytest.raises(ValueError, match="table_bucket_arn"):
        validate_config(
            {
                "catalog_mode": "s3tables_rest",
                "region": "us-east-1",
                "namespace": "default",
            },
        )


def test_validate_config_s3tables_namespace_single_level() -> None:
    with pytest.raises(ValueError, match="single-level"):
        validate_config(
            {
                "catalog_mode": "s3tables_rest",
                "region": "us-east-1",
                "namespace": "foo.bar",
                "table_bucket_arn": "arn:aws:s3tables:us-east-1:123456789012:bucket/mybucket",
            },
        )


def test_target_raises_config_validation_error() -> None:
    with pytest.raises(ConfigValidationError):
        TargetS3Tables(config={"catalog_mode": "glue_rest"})

