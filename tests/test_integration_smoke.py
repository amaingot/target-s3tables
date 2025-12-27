"""Optional integration smoke tests (skipped by default).

Run with:
  TARGET_S3TABLES_INTEGRATION=1 uv run pytest -k integration
"""

from __future__ import annotations

import logging
import os

import pytest

from target_s3tables.config import ParsedConfig
from target_s3tables.iceberg import get_catalog


@pytest.mark.integration
def test_integration_load_catalog() -> None:
    if os.getenv("TARGET_S3TABLES_INTEGRATION") != "1":
        pytest.skip("Set TARGET_S3TABLES_INTEGRATION=1 to run.")

    region = os.environ["TARGET_S3TABLES_REGION"]
    catalog_mode = os.getenv("TARGET_S3TABLES_CATALOG_MODE", "glue_rest")
    cfg: dict[str, object] = {
        "catalog_mode": catalog_mode,
        "region": region,
        "namespace": os.getenv("TARGET_S3TABLES_NAMESPACE", "default"),
    }

    if catalog_mode == "glue_rest":
        cfg["account_id"] = os.environ["TARGET_S3TABLES_ACCOUNT_ID"]
        cfg["table_bucket_name"] = os.environ["TARGET_S3TABLES_TABLE_BUCKET_NAME"]
    else:
        cfg["table_bucket_arn"] = os.environ["TARGET_S3TABLES_TABLE_BUCKET_ARN"]

    parsed = ParsedConfig.from_mapping(cfg)
    catalog = get_catalog(parsed, log=logging.getLogger("integration"))
    assert catalog is not None

