"""Singer target for Amazon S3 Tables (managed Apache Iceberg)."""

from __future__ import annotations

import logging
import typing as t

from singer_sdk import typing as th
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.target_base import Target

from target_s3tables.config import (
    apply_aws_env_overrides,
    validate_config,
)
from target_s3tables.sinks import S3TablesSink


class TargetS3Tables(Target):
    """Load Singer streams into Amazon S3 Tables via PyIceberg REST catalogs."""

    name = "target-s3tables"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "catalog_mode",
            th.StringType,
            allowed_values=["glue_rest", "s3tables_rest"],
            default="glue_rest",
            description="Iceberg REST catalog mode to use (AWS Glue recommended).",
        ),
        th.Property(
            "region",
            th.StringType(nullable=False),
            required=True,
            description="AWS region for the Iceberg REST endpoint (e.g. us-east-1).",
        ),
        th.Property(
            "namespace",
            th.StringType,
            default="default",
            description="Iceberg namespace (database).",
        ),
        th.Property(
            "write_mode",
            th.StringType,
            allowed_values=["append", "overwrite"],
            default="append",
            description="Write mode: append for incremental; overwrite to replace table contents.",
        ),
        th.Property(
            "batch_size_rows",
            th.IntegerType,
            default=5000,
            description="Max rows per Iceberg commit.",
        ),
        th.Property(
            "batch_max_bytes",
            th.IntegerType,
            nullable=True,
            default=None,
            description="Optional approximate byte limit for an in-memory batch.",
        ),
        th.Property(
            "sanitize_names",
            th.BooleanType,
            default=True,
            description="Sanitize stream/table/column names to Iceberg/AWS-friendly identifiers.",
        ),
        th.Property(
            "create_tables",
            th.BooleanType,
            default=True,
            description="Create Iceberg tables when missing.",
        ),
        th.Property(
            "evolve_schema",
            th.BooleanType,
            default=True,
            description="Evolve Iceberg schema when stream schema changes.",
        ),
        th.Property(
            "table_name_prefix",
            th.StringType,
            default="",
            description="Prefix applied to all Iceberg table names.",
        ),
        th.Property(
            "table_name_mapping",
            th.ObjectType(additional_properties=th.StringType),
            default={},
            description="Mapping of Singer stream name -> Iceberg table name.",
        ),
        # Glue REST mode:
        th.Property(
            "glue_uri",
            th.StringType,
            nullable=True,
            default=None,
            description="Glue Iceberg REST endpoint URI. "
            "Defaults to https://glue.<region>.amazonaws.com/iceberg.",
        ),
        th.Property(
            "glue_warehouse",
            th.StringType,
            nullable=True,
            default=None,
            description=(
                "Glue warehouse string: <account-id>:s3tablescatalog/<table-bucket-name>."
            ),
        ),
        th.Property(
            "account_id",
            th.StringType,
            nullable=True,
            default=None,
            description="AWS account id (used to build glue_warehouse if not provided).",
        ),
        th.Property(
            "table_bucket_name",
            th.StringType,
            nullable=True,
            default=None,
            description=(
                "S3 Tables table bucket name (used to build glue_warehouse if not provided)."
            ),
        ),
        # S3 Tables REST direct mode:
        th.Property(
            "s3tables_uri",
            th.StringType,
            nullable=True,
            default=None,
            description=(
                "S3 Tables Iceberg REST endpoint URI. "
                "Defaults to https://s3tables.<region>.amazonaws.com/iceberg."
            ),
        ),
        th.Property(
            "table_bucket_arn",
            th.StringType,
            nullable=True,
            default=None,
            description=(
                "Table bucket ARN: arn:aws:s3tables:<region>:<accountID>:bucket/<bucketname>."
            ),
        ),
        # SigV4:
        th.Property(
            "sigv4_enabled",
            th.BooleanType,
            default=True,
            description="Enable AWS SigV4 request signing for the Iceberg REST catalog.",
        ),
        th.Property(
            "signing_name",
            th.StringType,
            nullable=True,
            default=None,
            description="SigV4 signing name (defaults to glue or s3tables based on mode).",
        ),
        th.Property(
            "signing_region",
            th.StringType,
            nullable=True,
            default=None,
            description="SigV4 signing region (defaults to `region`).",
        ),
        # AWS credential overrides (optional):
        th.Property(
            "aws_access_key_id",
            th.StringType,
            nullable=True,
            default=None,
            description=(
                "Optional AWS access key id override "
                "(otherwise use default AWS credential chain)."
            ),
        ),
        th.Property(
            "aws_secret_access_key",
            th.StringType,
            nullable=True,
            default=None,
            secret=True,
            description=(
                "Optional AWS secret access key override "
                "(otherwise use default AWS credential chain)."
            ),
        ),
        th.Property(
            "aws_session_token",
            th.StringType,
            nullable=True,
            default=None,
            secret=True,
            description="Optional AWS session token override.",
        ),
        # Advanced:
        th.Property(
            "table_properties",
            th.ObjectType(additional_properties=th.StringType),
            default={},
            description="Iceberg table properties passed at create_table time.",
        ),
        th.Property(
            "snapshot_properties",
            th.ObjectType(additional_properties=th.StringType),
            default={},
            description="Snapshot properties passed to append/overwrite calls (when supported).",
        ),
        th.Property(
            "debug_http",
            th.BooleanType,
            default=False,
            description="Enable debug logging for HTTP/SigV4 interactions.",
        ),
        th.Property(
            "log_level",
            th.StringType,
            nullable=True,
            default=None,
            description="Optional log level override for this process (e.g. DEBUG, INFO).",
        ),
    ).to_dict()

    default_sink_class = S3TablesSink

    def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
        super().__init__(*args, **kwargs)
        apply_aws_env_overrides(self.config)
        _set_log_level_from_config(self.config)

    @property
    def state(self) -> t.Mapping[str, t.Any]:
        """Return the latest Singer state seen by the target."""
        return self._latest_state or {}

    def _validate_config(self, *, raise_errors: bool = True) -> list[str]:
        errors = super()._validate_config(raise_errors=False)
        try:
            validate_config(self._config)
        # pylint: disable-next=broad-except
        except Exception as exc:  # noqa: BLE001
            errors.append(str(exc))

        if errors and raise_errors:
            config_jsonschema = self.config_jsonschema
            self.append_builtin_config(config_jsonschema)
            raise ConfigValidationError(
                "Config validation failed",
                errors=errors,
                schema=config_jsonschema,
            )
        return errors


if __name__ == "__main__":
    # pylint: disable-next=no-value-for-parameter
    TargetS3Tables.cli()


def _set_log_level_from_config(config: t.Mapping[str, t.Any]) -> None:
    level = config.get("log_level")
    if not level:
        return
    try:
        logging.getLogger().setLevel(str(level).upper())
    # pylint: disable-next=broad-except
    except Exception:  # noqa: BLE001
        return
