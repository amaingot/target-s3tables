"""Unit tests for Singer JSON Schema -> Arrow/Iceberg conversion."""

from __future__ import annotations

import datetime as dt
import logging

import pyarrow as pa

from target_s3tables.iceberg import (
    records_to_arrow_table,
    singer_schema_to_arrow_schema,
    singer_schema_to_iceberg_schema,
)


def test_jsonschema_to_arrow_schema_primitives_and_nested() -> None:
    singer_schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": ["null", "string"]},
            "price": {"type": "number"},
            "active": {"type": "boolean"},
            "created_at": {"type": "string", "format": "date-time"},
            "birthday": {"type": "string", "format": "date"},
            "at_time": {"type": "string", "format": "time"},
            "tags": {"type": "array", "items": {"type": "string"}},
            "attrs": {"type": "object", "additionalProperties": {"type": "integer"}},
            "nested": {
                "type": "object",
                "properties": {"x": {"type": "integer"}, "y": {"type": ["null", "string"]}},
                "required": ["x"],
            },
            "weird": {"anyOf": [{"type": "string"}, {"type": "integer"}]},
        },
        "required": ["id"],
    }

    arrow_schema, _ = singer_schema_to_arrow_schema(
        singer_schema,
        sanitize_names=False,
        log=logging.getLogger("test"),
    )

    assert arrow_schema.field("id").type == pa.int64()
    assert arrow_schema.field("id").nullable is False
    assert arrow_schema.field("name").type == pa.string()
    assert arrow_schema.field("name").nullable is True
    assert arrow_schema.field("created_at").type == pa.timestamp("us", tz="UTC")
    assert arrow_schema.field("birthday").type == pa.date32()
    assert arrow_schema.field("at_time").type == pa.time64("us")

    assert pa.types.is_list(arrow_schema.field("tags").type)
    assert pa.types.is_map(arrow_schema.field("attrs").type)
    assert pa.types.is_struct(arrow_schema.field("nested").type)

    # anyOf beyond nullable pattern coerces to string
    assert arrow_schema.field("weird").type == pa.string()


def test_jsonschema_to_iceberg_schema_has_deterministic_field_ids() -> None:
    singer_schema = {
        "type": "object",
        "properties": {
            "attrs": {"type": "object", "additionalProperties": {"type": "integer"}},
            "tags": {"type": "array", "items": {"type": "string"}},
        },
    }
    schema1 = singer_schema_to_iceberg_schema(
        singer_schema,
        sanitize_names=False,
        log=logging.getLogger("test"),
    )
    schema2 = singer_schema_to_iceberg_schema(
        singer_schema,
        sanitize_names=False,
        log=logging.getLogger("test"),
    )

    assert sorted(schema1.field_ids) == sorted(schema2.field_ids) == [1, 2, 3, 4, 5]
    assert schema1.model_dump() == schema2.model_dump()


def test_records_to_arrow_table_coerces_datetime_like_strings() -> None:
    singer_schema = {
        "type": "object",
        "properties": {
            "ts": {"type": "string", "format": "date-time"},
            "d": {"type": "string", "format": "date"},
            "t": {"type": "string", "format": "time"},
        },
    }
    arrow_schema, specs = singer_schema_to_arrow_schema(
        singer_schema,
        sanitize_names=False,
        log=logging.getLogger("test"),
    )

    table = records_to_arrow_table(
        [
            {"ts": "2025-01-01T00:00:00Z", "d": "2025-01-01", "t": "12:34:56"},
        ],
        arrow_schema=arrow_schema,
        specs=specs,
    )

    assert table.column("ts")[0].as_py() == dt.datetime(2025, 1, 1, 0, 0, tzinfo=dt.timezone.utc)
    assert table.column("d")[0].as_py() == dt.date(2025, 1, 1)
    assert table.column("t")[0].as_py() == dt.time(12, 34, 56)

