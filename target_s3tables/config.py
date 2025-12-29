"""Configuration models and validation for target-s3tables."""

from __future__ import annotations

import os
import re
import typing as t
from dataclasses import dataclass
from urllib.parse import quote

CatalogMode = t.Literal["glue_rest", "s3tables_rest"]
WriteMode = t.Literal["append", "overwrite"]


_ACCOUNT_ID_RE = re.compile(r"^\d{12}$")
_S3TABLES_BUCKET_ARN_RE = re.compile(
    r"^arn:(?P<partition>aws|aws-us-gov|aws-cn):s3tables:(?P<region>[a-z0-9-]+):"
    r"(?P<account_id>\d{12}):bucket/(?P<bucket>[a-z0-9][a-z0-9.-]{1,61}[a-z0-9])$",
)


def _as_bool_str(value: bool) -> str:
    return "true" if value else "false"


def _default_glue_uri(region: str) -> str:
    """Return the default AWS Glue Iceberg REST URI for a given region."""
    return f"https://glue.{region}.amazonaws.com/iceberg"


def _default_s3tables_uri(region: str) -> str:
    """Return the default S3 Tables Iceberg REST URI for a given region."""
    return f"https://s3tables.{region}.amazonaws.com/iceberg"


def parse_s3tables_bucket_arn(arn: str) -> dict[str, str]:
    """Parse and validate an S3 Tables table bucket ARN.

    Expected format:
      arn:aws:s3tables:<region>:<account-id>:bucket/<bucketname>
    """
    match = _S3TABLES_BUCKET_ARN_RE.match(arn)
    if not match:
        msg = (
            "Invalid `table_bucket_arn`. Expected format: "
            "`arn:aws:s3tables:<region>:<accountID>:bucket/<bucketname>`."
        )
        raise ValueError(msg)
    return t.cast("dict[str, str]", match.groupdict())


def validate_aws_account_id(account_id: str) -> None:
    """Validate an AWS account ID (12 digits)."""
    if not _ACCOUNT_ID_RE.match(account_id):
        msg = "Invalid `account_id`. Expected a 12-digit AWS account ID."
        raise ValueError(msg)


def validate_glue_warehouse(warehouse: str) -> None:
    """Validate an AWS Glue Iceberg REST warehouse string."""
    # Example: 123456789012:s3tablescatalog/my-table-bucket
    if not re.match(r"^\d{12}:s3tablescatalog\/.+$", warehouse):
        msg = (
            "Invalid `glue_warehouse`. Expected format: "
            "`<account-id>:s3tablescatalog/<table-bucket-name>`."
        )
        raise ValueError(msg)


def validate_namespace(*, namespace: str, catalog_mode: CatalogMode) -> None:
    """Validate an Iceberg namespace for the selected catalog mode."""
    if not namespace or not namespace.strip():
        raise ValueError("Invalid `namespace`. Must be a non-empty string.")

    # AWS S3 Tables direct endpoint supports only single-level namespaces.
    if catalog_mode == "s3tables_rest" and "." in namespace:
        raise ValueError(
            "Invalid `namespace` for `s3tables_rest`: S3 Tables supports only single-level "
            "namespaces (no '.').",
        )


def validate_aws_creds(config: t.Mapping[str, t.Any]) -> None:
    """Validate optional static AWS credentials, if provided."""
    access_key = config.get("aws_access_key_id")
    secret_key = config.get("aws_secret_access_key")
    session_token = config.get("aws_session_token")
    if any(v is not None for v in (access_key, secret_key, session_token)) and not (
        access_key and secret_key
    ):
        raise ValueError(
            "If providing static credentials, both `aws_access_key_id` and "
            "`aws_secret_access_key` are required.",
        )


def apply_aws_env_overrides(config: t.Mapping[str, t.Any]) -> None:
    """Optionally set standard AWS env vars for credential discovery.

    This uses the standard AWS credential chain (just with explicit env overrides),
    which PyIceberg/botocore can pick up for SigV4 signing.
    """
    if config.get("aws_access_key_id"):
        os.environ["AWS_ACCESS_KEY_ID"] = str(config["aws_access_key_id"])
    if config.get("aws_secret_access_key"):
        os.environ["AWS_SECRET_ACCESS_KEY"] = str(config["aws_secret_access_key"])
    if config.get("aws_session_token"):
        os.environ["AWS_SESSION_TOKEN"] = str(config["aws_session_token"])


@dataclass(frozen=True)
class ParsedConfig:  # pylint: disable=too-many-instance-attributes
    """Parsed and normalized plugin configuration."""

    catalog_mode: CatalogMode
    region: str
    namespace: str
    write_mode: WriteMode
    batch_size_rows: int
    batch_max_bytes: int | None
    sanitize_names: bool
    create_tables: bool
    evolve_schema: bool

    table_name_prefix: str
    table_name_mapping: dict[str, str]

    # Glue REST
    glue_uri: str
    glue_warehouse: str | None
    account_id: str | None
    table_bucket_name: str | None

    # S3 Tables REST (direct)
    s3tables_uri: str
    table_bucket_arn: str | None

    # SigV4
    sigv4_enabled: bool
    signing_name: str
    signing_region: str

    # Advanced
    table_properties: dict[str, str]
    snapshot_properties: dict[str, str]
    debug_http: bool

    @classmethod
    def from_mapping(cls, config: t.Mapping[str, t.Any]) -> ParsedConfig:
        """Parse and normalize a raw Singer SDK config mapping."""
        catalog_mode = t.cast(CatalogMode, config.get("catalog_mode", "glue_rest"))
        region = t.cast(str, config.get("region"))
        namespace = t.cast(str, config.get("namespace", "default"))
        write_mode = t.cast(WriteMode, config.get("write_mode", "append"))

        glue_uri = t.cast(str | None, config.get("glue_uri")) or _default_glue_uri(region)
        s3tables_uri = t.cast(str | None, config.get("s3tables_uri")) or _default_s3tables_uri(
            region,
        )

        sigv4_enabled = bool(config.get("sigv4_enabled", True))
        signing_region = t.cast(str | None, config.get("signing_region")) or region

        default_signing_name = "glue" if catalog_mode == "glue_rest" else "s3tables"
        signing_name = t.cast(str | None, config.get("signing_name")) or default_signing_name

        return cls(
            catalog_mode=catalog_mode,
            region=region,
            namespace=namespace,
            write_mode=write_mode,
            batch_size_rows=int(config.get("batch_size_rows", 5000)),
            batch_max_bytes=t.cast(int | None, config.get("batch_max_bytes")),
            sanitize_names=bool(config.get("sanitize_names", True)),
            create_tables=bool(config.get("create_tables", True)),
            evolve_schema=bool(config.get("evolve_schema", True)),
            table_name_prefix=str(config.get("table_name_prefix") or ""),
            table_name_mapping=dict(config.get("table_name_mapping") or {}),
            glue_uri=glue_uri,
            glue_warehouse=t.cast(str | None, config.get("glue_warehouse")),
            account_id=t.cast(str | None, config.get("account_id")),
            table_bucket_name=t.cast(str | None, config.get("table_bucket_name")),
            s3tables_uri=s3tables_uri,
            table_bucket_arn=t.cast(str | None, config.get("table_bucket_arn")),
            sigv4_enabled=sigv4_enabled,
            signing_name=signing_name,
            signing_region=signing_region,
            table_properties={
                str(k): str(v) for k, v in (config.get("table_properties") or {}).items()
            },
            snapshot_properties={
                str(k): str(v)
                for k, v in (config.get("snapshot_properties") or {}).items()
            },
            debug_http=bool(config.get("debug_http", False)),
        )

    def rest_catalog_properties(self) -> dict[str, str]:
        """Build PyIceberg REST catalog properties for `load_catalog()`."""
        uri, warehouse, prefix = self._rest_uri_warehouse_prefix()
        props: dict[str, str] = {
            "type": "rest",
            "uri": uri,
            "warehouse": warehouse,
            "rest.sigv4-enabled": _as_bool_str(self.sigv4_enabled),
            "rest.signing-name": self.signing_name,
            "rest.signing-region": self.signing_region,
        }
        if prefix is not None:
            # Different clients use different keys for prefix; PyIceberg uses `prefix`.
            props["prefix"] = prefix
            props["rest.prefix"] = prefix
        return props

    def _rest_uri_warehouse_prefix(self) -> tuple[str, str, str | None]:
        """Return the REST URI, warehouse, and optional prefix for the catalog."""
        if self.catalog_mode == "glue_rest":
            warehouse = self.glue_warehouse or self._build_glue_warehouse()
            return self.glue_uri, warehouse, None

        # Direct S3 Tables REST
        if not self.table_bucket_arn:
            raise ValueError("Missing required `table_bucket_arn` for `s3tables_rest`.")
        _ = parse_s3tables_bucket_arn(self.table_bucket_arn)
        # AWS docs: prefix is the URL-encoded table bucket ARN.
        prefix = quote(self.table_bucket_arn, safe="")
        return self.s3tables_uri, self.table_bucket_arn, prefix

    def _build_glue_warehouse(self) -> str:
        """Build the AWS Glue warehouse string."""
        if self.glue_warehouse:
            return self.glue_warehouse
        if not self.account_id or not self.table_bucket_name:
            raise ValueError(
                "Missing `glue_warehouse`. Provide either `glue_warehouse` directly or "
                "both `account_id` and `table_bucket_name`.",
            )
        validate_aws_account_id(self.account_id)
        return f"{self.account_id}:s3tablescatalog/{self.table_bucket_name}"


def validate_config(config: t.Mapping[str, t.Any]) -> None:
    """Run additional validations beyond JSONSchema."""
    catalog_mode = t.cast(CatalogMode, config.get("catalog_mode", "glue_rest"))
    region = config.get("region")
    if not region or not isinstance(region, str):
        raise ValueError("Missing required `region`.")

    validate_aws_creds(config)

    namespace = t.cast(str, config.get("namespace", "default"))
    validate_namespace(namespace=namespace, catalog_mode=catalog_mode)

    if catalog_mode == "glue_rest":
        glue_warehouse = config.get("glue_warehouse")
        account_id = config.get("account_id")
        table_bucket_name = config.get("table_bucket_name")
        if glue_warehouse:
            validate_glue_warehouse(str(glue_warehouse))
        else:
            if not account_id or not table_bucket_name:
                raise ValueError(
                    "For `glue_rest`, provide either `glue_warehouse` or both "
                    "`account_id` and `table_bucket_name`.",
                )
            validate_aws_account_id(str(account_id))
    elif catalog_mode == "s3tables_rest":
        arn = config.get("table_bucket_arn")
        if not arn:
            raise ValueError("Missing required `table_bucket_arn` for `s3tables_rest`.")
        arn_parts = parse_s3tables_bucket_arn(str(arn))
        if arn_parts["region"] != region:
            raise ValueError(
                f"`table_bucket_arn` region '{arn_parts['region']}' does not match "
                f"`region` '{region}'.",
            )
    else:
        raise ValueError(f"Unsupported `catalog_mode`: {catalog_mode}")
