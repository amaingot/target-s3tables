"""Singer sinks for writing to Amazon S3 Tables (managed Iceberg)."""

from __future__ import annotations

import json
import typing as t

from singer_sdk.sinks import BatchSink

from target_s3tables.config import ParsedConfig
from target_s3tables.iceberg import (
    evolve_table_schema_union_by_name,
    get_catalog,
    load_or_create_table,
    records_to_arrow_table,
    singer_schema_to_arrow_schema,
    table_identifier_for_stream,
    write_arrow_to_table,
)


class S3TablesSink(BatchSink):
    """Batch sink which commits Arrow batches into Iceberg tables."""

    def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
        super().__init__(*args, **kwargs)
        self._parsed_config = ParsedConfig.from_mapping(self.config)
        self._table_id = table_identifier_for_stream(
            self._parsed_config,
            stream_name=self.stream_name,
        )
        self._catalog: t.Any | None = None
        self._table: t.Any | None = None

        self._arrow_schema, self._field_specs = singer_schema_to_arrow_schema(
            self.schema,
            sanitize_names=self._parsed_config.sanitize_names,
            log=self.logger,
        )

        self._batch_bytes: int = 0

    def start_batch(self, context: dict) -> None:
        """Start a batch.

        Developers may optionally add additional markers to the `context` dict,
        which is unique to this batch.

        Args:
            context: Stream partition or context dictionary.
        """
        self._batch_bytes = 0
        context["approx_bytes"] = 0

    @property
    def is_full(self) -> bool:
        if super().is_full:
            return True
        max_bytes = self._parsed_config.batch_max_bytes
        return bool(max_bytes and self._batch_bytes >= max_bytes)

    def process_record(self, record: dict, context: dict) -> None:
        """Stage a record for batch processing."""
        super().process_record(record, context)
        max_bytes = self._parsed_config.batch_max_bytes
        if not max_bytes:
            return
        self._batch_bytes += _approx_record_size_bytes(record)
        context["approx_bytes"] = self._batch_bytes

    def setup(self) -> None:
        """Initialize the sink and ensure the destination table exists."""
        super().setup()
        self._ensure_table()

    def process_batch(self, context: dict) -> None:
        """Write the current batch as one Iceberg commit."""
        records: list[dict[str, t.Any]] = list(context.get("records") or [])
        if not records:
            return

        table = self._ensure_table()

        if self._parsed_config.evolve_schema:
            evolve_table_schema_union_by_name(
                table,
                arrow_schema=self._arrow_schema,
                log=self.logger,
            )

        arrow_table = records_to_arrow_table(
            records,
            arrow_schema=self._arrow_schema,
            specs=self._field_specs,
        )

        self.logger.info(
            "Writing %d rows to Iceberg table '%s' (mode=%s)...",
            arrow_table.num_rows,
            ".".join(self._table_id),
            self._parsed_config.write_mode,
        )
        try:
            write_arrow_to_table(
                table,
                arrow_table=arrow_table,
                config=self._parsed_config,
                log=self.logger,
            )
        except Exception as exc:  # noqa: BLE001
            if _is_auth_error(exc):
                raise RuntimeError(_auth_hint(self._parsed_config)) from exc
            raise
        self.logger.info(
            "Committed %d rows to Iceberg table '%s'.",
            arrow_table.num_rows,
            ".".join(self._table_id),
        )

    def mark_drained(self) -> None:
        super().mark_drained()
        self._batch_bytes = 0

    def _ensure_table(self):  # noqa: ANN001
        if self._table is not None:
            return self._table

        if self._catalog is None:
            try:
                self._catalog = get_catalog(self._parsed_config, log=self.logger)
            except Exception as exc:  # noqa: BLE001
                if _is_auth_error(exc):
                    raise RuntimeError(_auth_hint(self._parsed_config)) from exc
                raise

        try:
            self._table = load_or_create_table(
                self._catalog,
                table_id=self._table_id,
                singer_schema=self.schema,
                config=self._parsed_config,
                log=self.logger,
            )
        except Exception as exc:  # noqa: BLE001
            if _is_auth_error(exc):
                raise RuntimeError(_auth_hint(self._parsed_config)) from exc
            raise

        return self._table


def _approx_record_size_bytes(record: dict[str, t.Any]) -> int:
    try:
        return len(json.dumps(record, default=str).encode("utf-8"))
    # pylint: disable-next=broad-except
    except Exception:  # noqa: BLE001
        return 0


def _auth_hint(config: ParsedConfig) -> str:
    return (
        "Failed to access Iceberg REST catalog/table. Verify AWS credentials and permissions, "
        f"`catalog_mode`={config.catalog_mode}, `region`={config.region}, "
        f"`signing_name`={config.signing_name}, `signing_region`={config.signing_region}."
    )


def _is_auth_error(exc: Exception) -> bool:
    status_code = getattr(exc, "status_code", None)
    if status_code is None and hasattr(exc, "response"):
        status_code = getattr(getattr(exc, "response"), "status_code", None)
    if isinstance(status_code, int) and status_code in {401, 403}:
        return True

    message = str(exc).lower()
    return any(
        token in message
        for token in (
            "accessdenied",
            "not authorized",
            "unauthorized",
            "forbidden",
            "signaturedoesnotmatch",
            "invalidsignature",
        )
    )
