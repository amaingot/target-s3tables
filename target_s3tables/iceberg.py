"""Iceberg helpers for Amazon S3 Tables via PyIceberg REST catalogs."""

from __future__ import annotations

import datetime as dt
import itertools
import json
import logging
import random
import re
import threading
import time
import typing as t
from dataclasses import dataclass

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimeType,
    TimestamptzType,
)

from target_s3tables.config import ParsedConfig


_CATALOG_LOCK = threading.Lock()
_CATALOG_CACHE: dict[tuple[tuple[str, str], ...], t.Any] = {}


def clear_catalog_cache() -> None:
    """Clear the internal catalog cache (useful for tests)."""
    with _CATALOG_LOCK:
        _CATALOG_CACHE.clear()


def get_catalog(config: ParsedConfig, *, log: logging.Logger) -> t.Any:
    """Initialize (or return cached) PyIceberg Catalog instance."""
    props = config.rest_catalog_properties()
    cache_key = tuple(sorted(props.items()))
    with _CATALOG_LOCK:
        cached = _CATALOG_CACHE.get(cache_key)
        if cached is not None:
            return cached

    if config.debug_http:
        _enable_http_debug_logging()

    catalog = load_catalog("target-s3tables", **props)
    with _CATALOG_LOCK:
        _CATALOG_CACHE[cache_key] = catalog
    log.debug(
        "Loaded Iceberg catalog using properties: %s",
        {k: v for k, v in props.items() if "secret" not in k},
    )
    return catalog


def _enable_http_debug_logging() -> None:
    for name in ("urllib3", "requests", "botocore", "pyiceberg"):
        logging.getLogger(name).setLevel(logging.DEBUG)


# pylint: disable-next=too-many-arguments
def retry(  # noqa: PLR0913
    func: t.Callable[[], t.Any],
    *,
    log: logging.Logger,
    op: str,
    max_attempts: int = 8,
    base_delay_s: float = 1.0,
    max_delay_s: float = 60.0,
) -> t.Any:
    """Retry helper with exponential backoff + jitter for transient HTTP errors."""
    attempt = 0
    while True:
        attempt += 1
        try:
            return func()
        # pylint: disable-next=broad-except
        except Exception as exc:  # noqa: BLE001
            if not _is_retriable_exception(exc) or attempt >= max_attempts:
                raise

            delay = min(max_delay_s, base_delay_s * (2 ** (attempt - 1)))
            delay *= random.uniform(0.5, 1.5)
            log.warning(
                "Retrying %s after error (attempt %d/%d): %s; sleeping %.2fs",
                op,
                attempt,
                max_attempts,
                _format_exception_short(exc),
                delay,
            )
            time.sleep(delay)


def _is_retriable_exception(exc: Exception) -> bool:
    """Determine if an exception is transient and should be retried."""
    status_code = getattr(exc, "status_code", None)
    if status_code is None and hasattr(exc, "response"):
        status_code = getattr(getattr(exc, "response"), "status_code", None)

    if isinstance(status_code, int):
        if status_code == 429:
            return True
        if 500 <= status_code <= 599:
            return True
        return False

    # Network-ish / unknown transient errors.
    exc_name = exc.__class__.__name__.lower()
    return any(
        token in exc_name
        for token in (
            "timeout",
            "connectionerror",
            "temporarilyunavailable",
            "throttl",
        )
    )


def _format_exception_short(exc: Exception) -> str:
    return f"{exc.__class__.__name__}: {exc}"


def sanitize_identifier(name: str) -> str:
    """Sanitize an identifier to be Iceberg/AWS-friendly."""
    cleaned = name.strip().lower()
    cleaned = re.sub(r"[^a-z0-9_]+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    if not cleaned:
        return "_"
    if cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    return cleaned


def _dedupe_sanitized_names(names: t.Sequence[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    used: set[str] = set()
    for original in sorted(names):
        base = sanitize_identifier(original)
        candidate = base
        suffix = 2
        while candidate in used:
            candidate = f"{base}_{suffix}"
            suffix += 1
        used.add(candidate)
        result[original] = candidate
    return result


def sanitize_namespace(namespace: str) -> str:
    """Sanitize a namespace (dot-separated) into Iceberg-friendly identifiers."""
    return ".".join(sanitize_identifier(part) for part in namespace.split("."))


def table_identifier_for_stream(config: ParsedConfig, *, stream_name: str) -> tuple[str, ...]:
    """Return an Iceberg identifier tuple for a Singer stream."""
    namespace = config.namespace
    table_name = config.table_name_mapping.get(stream_name, stream_name)
    if config.table_name_prefix:
        table_name = f"{config.table_name_prefix}{table_name}"

    if config.sanitize_names:
        namespace = sanitize_namespace(namespace)
        table_name = sanitize_identifier(table_name)

    namespace_parts = tuple(part for part in namespace.split(".") if part)
    return (*namespace_parts, table_name)


def load_or_create_table(
    catalog: t.Any,
    *,
    table_id: tuple[str, ...],
    singer_schema: dict[str, t.Any],
    config: ParsedConfig,
    log: logging.Logger,
) -> Table:
    """Load the Iceberg table or create it if missing and allowed."""
    def _load() -> Table:
        return t.cast(Table, catalog.load_table(table_id))

    try:
        return retry(_load, log=log, op=f"load_table({'.'.join(table_id)})")
    except NoSuchTableError:
        if not config.create_tables:
            raise RuntimeError(
                f"Iceberg table '{'.'.join(table_id)}' does not exist and `create_tables=false`.",
            ) from None

    namespace = table_id[:-1]
    iceberg_schema = singer_schema_to_iceberg_schema(
        singer_schema,
        sanitize_names=config.sanitize_names,
        log=log,
    )

    _create_namespace_if_needed(catalog, namespace=namespace, log=log)

    def _create() -> Table:
        return t.cast(
            Table,
            catalog.create_table(
                table_id,
                schema=iceberg_schema,
                properties=config.table_properties,
            ),
        )

    table = retry(_create, log=log, op=f"create_table({'.'.join(table_id)})")
    return table


def evolve_table_schema_union_by_name(
    table: Table,
    *,
    arrow_schema: pa.Schema,
    log: logging.Logger,
) -> None:
    """Evolve table schema using union-by-name (adds new columns, keeps existing IDs)."""

    def _evolve() -> None:
        with table.update_schema() as update:
            update.union_by_name(arrow_schema)

    retry(_evolve, log=log, op="update_schema")


def _create_namespace_if_needed(
    catalog: t.Any,
    *,
    namespace: tuple[str, ...],
    log: logging.Logger,
) -> None:
    """Create the namespace if supported by the catalog (best-effort)."""
    if not namespace:
        return

    if not hasattr(catalog, "create_namespace"):
        return

    def _create_ns() -> None:
        catalog.create_namespace(namespace)

    try:
        retry(_create_ns, log=log, op=f"create_namespace({'.'.join(namespace)})")
    except Exception as exc:  # noqa: BLE001
        # Best-effort: ignore "already exists" errors.
        if "already exists" in str(exc).lower():
            return
        raise


@dataclass(frozen=True)
class FieldSpec:  # pylint: disable=too-many-instance-attributes
    """Field specification for record -> Arrow conversion."""

    source_name: str
    target_name: str
    arrow_type: pa.DataType
    nullable: bool
    kind: t.Literal["primitive", "struct", "list", "map"]
    children: tuple[FieldSpec, ...] = ()
    element: FieldSpec | None = None
    map_value: FieldSpec | None = None


def singer_schema_to_arrow_schema(
    singer_schema: dict[str, t.Any],
    *,
    sanitize_names: bool,
    log: logging.Logger,
) -> tuple[pa.Schema, tuple[FieldSpec, ...]]:
    """Convert Singer JSON Schema (subset) to a PyArrow Schema + coercion plan."""
    properties = singer_schema.get("properties") or {}
    if not isinstance(properties, dict):
        properties = {}

    required = set(singer_schema.get("required") or [])
    names = list(properties.keys())
    name_map = _dedupe_sanitized_names(names) if sanitize_names else {n: n for n in names}

    specs: list[FieldSpec] = []
    fields: list[pa.Field] = []
    for source_name in sorted(names):
        prop_schema = properties[source_name] or {}
        target_name = name_map[source_name]
        spec = _jsonschema_to_fieldspec(
            source_name=source_name,
            target_name=target_name,
            schema=t.cast(dict[str, t.Any], prop_schema),
            required_in_parent=source_name in required,
            sanitize_names=sanitize_names,
            log=log,
        )
        specs.append(spec)
        fields.append(pa.field(target_name, spec.arrow_type, nullable=spec.nullable))

    return pa.schema(fields), tuple(specs)


# pylint: disable-next=too-many-arguments,too-many-locals,too-many-return-statements
def _jsonschema_to_fieldspec(  # noqa: PLR0913
    *,
    source_name: str,
    target_name: str,
    schema: dict[str, t.Any],
    required_in_parent: bool,
    sanitize_names: bool,
    log: logging.Logger,
) -> FieldSpec:
    normalized, nullable = _normalize_nullable_schema(schema, log=log)
    nullable = nullable or not required_in_parent

    json_type = normalized.get("type")
    if json_type == "string":
        arrow_type = _arrow_string_type_for_format(normalized.get("format"))
        return FieldSpec(source_name, target_name, arrow_type, nullable, "primitive")
    if json_type == "integer":
        return FieldSpec(source_name, target_name, pa.int64(), nullable, "primitive")
    if json_type == "number":
        return FieldSpec(source_name, target_name, pa.float64(), nullable, "primitive")
    if json_type == "boolean":
        return FieldSpec(source_name, target_name, pa.bool_(), nullable, "primitive")
    if json_type == "array":
        items = t.cast(dict[str, t.Any], normalized.get("items") or {})
        element_spec = _jsonschema_to_fieldspec(
            source_name=f"{source_name}[]",
            target_name="element",
            schema=items,
            required_in_parent=True,
            sanitize_names=sanitize_names,
            log=log,
        )
        list_type = pa.list_(
            pa.field(
                "element",
                element_spec.arrow_type,
                nullable=element_spec.nullable,
            ),
        )
        return FieldSpec(
            source_name,
            target_name,
            list_type,
            nullable,
            "list",
            element=element_spec,
        )
    if json_type == "object":
        props = normalized.get("properties")
        if isinstance(props, dict) and props:
            required = set(normalized.get("required") or [])
            names = list(props.keys())
            name_map = _dedupe_sanitized_names(names) if sanitize_names else {n: n for n in names}
            child_specs: list[FieldSpec] = []
            child_fields: list[pa.Field] = []
            for child_source in sorted(names):
                child_schema = t.cast(dict[str, t.Any], props.get(child_source) or {})
                child_target = name_map[child_source]
                child_spec = _jsonschema_to_fieldspec(
                    source_name=child_source,
                    target_name=child_target,
                    schema=child_schema,
                    required_in_parent=child_source in required,
                    sanitize_names=sanitize_names,
                    log=log,
                )
                child_specs.append(child_spec)
                child_fields.append(
                    pa.field(child_target, child_spec.arrow_type, nullable=child_spec.nullable),
                )
            return FieldSpec(
                source_name,
                target_name,
                pa.struct(child_fields),
                nullable,
                "struct",
                children=tuple(child_specs),
            )

        additional = normalized.get("additionalProperties")
        if additional is False:
            log.warning(
                "Object field '%s' has `additionalProperties=false` and no `properties`. "
                "Coercing to string.",
                source_name,
            )
            return FieldSpec(source_name, target_name, pa.string(), nullable, "primitive")

        value_schema: dict[str, t.Any]
        if additional is True or additional is None:
            # In JSON Schema, unspecified/true additionalProperties means values may be any JSON type,
            # including null. Default to a nullable string representation.
            value_schema = {"type": ["null", "string"]}
        elif isinstance(additional, dict):
            # When additionalProperties is an untyped schema (e.g. {}), values may be any JSON type
            # including null. Default to a nullable string representation.
            if not any(k in additional for k in ("type", "anyOf", "oneOf")):
                value_schema = {"type": ["null", "string"]}
            else:
                value_schema = t.cast(dict[str, t.Any], additional)
        else:
            value_schema = {"type": ["null", "string"]}
        value_spec = _jsonschema_to_fieldspec(
            source_name=f"{source_name}{{}}",
            target_name="value",
            schema=value_schema,
            required_in_parent=True,
            sanitize_names=sanitize_names,
            log=log,
        )
        map_type = pa.map_(
            pa.field("key", pa.string(), nullable=False),
            pa.field("value", value_spec.arrow_type, nullable=True),
        )
        return FieldSpec(source_name, target_name, map_type, nullable, "map", map_value=value_spec)

    # Fallback for unsupported schemas.
    log.warning("Unsupported JSON Schema type for field '%s'. Coercing to string.", source_name)
    return FieldSpec(source_name, target_name, pa.string(), True, "primitive")


def _arrow_string_type_for_format(fmt: str | None) -> pa.DataType:
    if fmt == "date":
        return pa.date32()
    if fmt == "time":
        return pa.time64("us")
    if fmt == "date-time":
        return pa.timestamp("us", tz="UTC")
    return pa.string()


def _normalize_nullable_schema(
    schema: dict[str, t.Any],
    *,
    log: logging.Logger,
) -> tuple[dict[str, t.Any], bool]:
    # Handle common Singer nullable pattern: {"type": ["null", "<t>"]}
    if isinstance(schema.get("type"), list):
        type_list = [t for t in schema["type"] if isinstance(t, str)]
        nullable = "null" in type_list
        non_null_types = [t for t in type_list if t != "null"]
        if len(non_null_types) == 1:
            return {**schema, "type": non_null_types[0]}, nullable
        log.warning("Union types beyond nullable pattern detected. Coercing to string.")
        return {"type": "string"}, True

    # Handle anyOf/oneOf beyond nullable pattern.
    for key in ("anyOf", "oneOf"):
        variants = schema.get(key)
        if isinstance(variants, list) and variants:
            dict_variants = [v for v in variants if isinstance(v, dict)]
            if len(dict_variants) != len(variants):
                log.warning("%s contains non-object variants. Coercing to string.", key)
                return {"type": "string"}, True

            def _is_null_variant(v: dict[str, t.Any]) -> bool:
                vt = v.get("type")
                if vt == "null":
                    return True
                if isinstance(vt, list):
                    vt_list = [x for x in vt if isinstance(x, str)]
                    return "null" in vt_list and all(x == "null" for x in vt_list)
                return False

            nullable_variants = [v for v in dict_variants if _is_null_variant(v)]
            non_null_variants = [v for v in dict_variants if not _is_null_variant(v)]
            if len(non_null_variants) == 1 and len(nullable_variants) >= 1:
                normalized, _ = _normalize_nullable_schema(non_null_variants[0], log=log)
                return normalized, True
            log.warning("%s beyond nullable pattern detected. Coercing to string.", key)
            return {"type": "string"}, True

    # Default: no nullable semantics inferred.
    return schema, False


def records_to_arrow_table(
    records: t.Sequence[dict[str, t.Any]],
    *,
    arrow_schema: pa.Schema,
    specs: tuple[FieldSpec, ...],
) -> pa.Table:
    """Convert a list of Singer records into a typed PyArrow table."""
    cooked: list[dict[str, t.Any]] = []
    for record in records:
        cooked.append(_coerce_record(record, specs))
    return pa.Table.from_pylist(cooked, schema=arrow_schema)


def _coerce_record(record: dict[str, t.Any], specs: tuple[FieldSpec, ...]) -> dict[str, t.Any]:
    out: dict[str, t.Any] = {}
    for spec in specs:
        value = record.get(spec.source_name)
        out[spec.target_name] = _coerce_value(value, spec)
    return out


# pylint: disable-next=too-many-branches,too-many-return-statements
def _coerce_value(value: t.Any, spec: FieldSpec) -> t.Any:  # noqa: ANN401, PLR0911
    if value is None:
        if spec.nullable:
            return None
        if spec.kind == "struct":
            return {}
        if spec.kind == "list":
            return []
        if spec.kind == "map":
            return {}
        return None

    if spec.kind == "struct":
        if not isinstance(value, dict):
            return {} if not spec.nullable else None
        out: dict[str, t.Any] = {}
        for child in spec.children:
            out[child.target_name] = _coerce_value(value.get(child.source_name), child)
        return out

    if spec.kind == "list":
        if not isinstance(value, list):
            return [] if not spec.nullable else None
        assert spec.element is not None
        out_list: list[t.Any] = []
        for v in value:
            coerced = _coerce_value(v, spec.element)
            if coerced is None and not spec.element.nullable:
                continue
            out_list.append(coerced)
        return out_list

    if spec.kind == "map":
        if not isinstance(value, dict):
            return {} if not spec.nullable else None
        assert spec.map_value is not None
        out_map: dict[str, t.Any] = {}
        for k, v in value.items():
            coerced = _coerce_value(v, spec.map_value)
            if coerced is None and not spec.map_value.nullable:
                continue
            out_map[str(k)] = coerced
        return out_map

    # Primitives:
    arrow_type = spec.arrow_type
    if pa.types.is_string(arrow_type):
        if isinstance(value, (dict, list)):
            return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)
        return str(value)
    if pa.types.is_int64(arrow_type):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    if pa.types.is_float64(arrow_type):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    if pa.types.is_boolean(arrow_type):
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "t", "1", "yes", "y"}:
                return True
            if lowered in {"false", "f", "0", "no", "n"}:
                return False
            return None
        return bool(value)
    if pa.types.is_date32(arrow_type):
        return _coerce_date(value)
    if pa.types.is_time64(arrow_type):
        return _coerce_time(value)
    if pa.types.is_timestamp(arrow_type):
        return _coerce_datetime(value)

    return None


def _coerce_date(value: t.Any) -> dt.date | None:  # noqa: ANN401
    if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
        return value
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return dt.date.fromisoformat(value.strip())
        except ValueError:
            return None
    return None


def _coerce_time(value: t.Any) -> dt.time | None:  # noqa: ANN401
    if isinstance(value, dt.time):
        return value
    if isinstance(value, str):
        try:
            return dt.time.fromisoformat(value.strip())
        except ValueError:
            return None
    return None


def _coerce_datetime(value: t.Any) -> dt.datetime | None:  # noqa: ANN401
    if isinstance(value, dt.datetime):
        result = value
    elif isinstance(value, str):
        s = value.strip()
        if s.endswith("Z"):
            s = f"{s[:-1]}+00:00"
        try:
            result = dt.datetime.fromisoformat(s)
        except ValueError:
            return None
    elif isinstance(value, (int, float)):
        result = dt.datetime.fromtimestamp(float(value), tz=dt.timezone.utc)
    else:
        return None

    if result.tzinfo is None:
        result = result.replace(tzinfo=dt.timezone.utc)
    return result


def singer_schema_to_iceberg_schema(
    singer_schema: dict[str, t.Any],
    *,
    sanitize_names: bool,
    log: logging.Logger,
) -> Schema:
    """Convert Singer JSON Schema (subset) to a PyIceberg Schema with deterministic IDs."""
    properties = singer_schema.get("properties") or {}
    if not isinstance(properties, dict):
        properties = {}

    required = set(singer_schema.get("required") or [])
    names = list(properties.keys())
    name_map = _dedupe_sanitized_names(names) if sanitize_names else {n: n for n in names}
    next_id = itertools.count(1).__next__

    fields: list[NestedField] = []
    for source_name in sorted(names):
        field_schema = t.cast(dict[str, t.Any], properties[source_name] or {})
        target_name = name_map[source_name]
        nested = _jsonschema_to_nested_field(
            name=target_name,
            schema=field_schema,
            field_id=next_id(),
            required_in_parent=source_name in required,
            next_id=next_id,
            sanitize_names=sanitize_names,
            log=log,
        )
        fields.append(nested)

    return Schema(*fields)


# pylint: disable-next=too-many-arguments
def _jsonschema_to_nested_field(  # noqa: PLR0913
    *,
    name: str,
    schema: dict[str, t.Any],
    field_id: int,
    required_in_parent: bool,
    next_id: t.Callable[[], int],
    sanitize_names: bool,
    log: logging.Logger,
) -> NestedField:
    normalized, nullable = _normalize_nullable_schema(schema, log=log)
    required = bool(required_in_parent) and not nullable
    field_type = _jsonschema_to_iceberg_type(
        normalized,
        next_id=next_id,
        sanitize_names=sanitize_names,
        log=log,
    )
    return NestedField(field_id=field_id, name=name, field_type=field_type, required=required)


# pylint: disable-next=too-many-branches,too-many-locals,too-many-return-statements
def _jsonschema_to_iceberg_type(
    schema: dict[str, t.Any],
    *,
    next_id: t.Callable[[], int],
    sanitize_names: bool,
    log: logging.Logger,
) -> t.Any:  # noqa: ANN401, PLR0911
    json_type = schema.get("type")
    if json_type == "string":
        fmt = schema.get("format")
        if fmt == "date":
            return DateType()
        if fmt == "time":
            return TimeType()
        if fmt == "date-time":
            return TimestamptzType()
        return StringType()
    if json_type == "integer":
        return LongType()
    if json_type == "number":
        return DoubleType()
    if json_type == "boolean":
        return BooleanType()
    if json_type == "array":
        items = t.cast(dict[str, t.Any], schema.get("items") or {"type": "string"})
        normalized, nullable = _normalize_nullable_schema(items, log=log)
        element_id = next_id()
        element_type = _jsonschema_to_iceberg_type(
            normalized,
            next_id=next_id,
            sanitize_names=sanitize_names,
            log=log,
        )
        return ListType(element_id=element_id, element=element_type, element_required=not nullable)
    if json_type == "object":
        props = schema.get("properties")
        if isinstance(props, dict) and props:
            required = set(schema.get("required") or [])
            names = list(props.keys())
            name_map = _dedupe_sanitized_names(names) if sanitize_names else {n: n for n in names}
            fields: list[NestedField] = []
            for source_name in sorted(names):
                child_schema = t.cast(dict[str, t.Any], props.get(source_name) or {})
                child_name = name_map[source_name]
                fields.append(
                    _jsonschema_to_nested_field(
                        name=child_name,
                        schema=child_schema,
                        field_id=next_id(),
                        required_in_parent=source_name in required,
                        next_id=next_id,
                        sanitize_names=sanitize_names,
                        log=log,
                    ),
                )
            return StructType(*fields)

        additional = schema.get("additionalProperties")
        if additional is False:
            log.warning("Object schema without properties; coercing to string.")
            return StringType()

        value_schema: dict[str, t.Any]
        if additional is True or additional is None:
            # In JSON Schema, unspecified/true additionalProperties means values may be any JSON type,
            # including null. Default to a nullable string representation.
            value_schema = {"type": ["null", "string"]}
        elif isinstance(additional, dict):
            # When additionalProperties is an untyped schema (e.g. {}), values may be any JSON type
            # including null. Default to a nullable string representation.
            if not any(k in additional for k in ("type", "anyOf", "oneOf")):
                value_schema = {"type": ["null", "string"]}
            else:
                value_schema = t.cast(dict[str, t.Any], additional)
        else:
            value_schema = {"type": ["null", "string"]}
        normalized_value, value_nullable = _normalize_nullable_schema(value_schema, log=log)
        key_id = next_id()
        value_id = next_id()
        value_type = _jsonschema_to_iceberg_type(
            normalized_value,
            next_id=next_id,
            sanitize_names=sanitize_names,
            log=log,
        )
        return MapType(
            key_id=key_id,
            key_type=StringType(),
            value_id=value_id,
            value_type=value_type,
            value_required=not value_nullable,
        )

    log.warning("Unsupported JSON Schema type '%s'; coercing to string.", json_type)
    return StringType()


def is_table_partitioned(table: Table) -> bool:
    """Return True if the Iceberg table has a non-empty partition spec."""
    try:
        spec = table.spec()
        fields = getattr(spec, "fields", None)
        return bool(fields)
    # pylint: disable-next=broad-except
    except Exception:  # noqa: BLE001
        return False


def write_arrow_to_table(
    table: Table,
    *,
    arrow_table: pa.Table,
    config: ParsedConfig,
    log: logging.Logger,
) -> None:
    """Append or overwrite an Iceberg table using a PyArrow table."""
    snapshot_props = config.snapshot_properties or None

    def _append() -> None:
        if snapshot_props:
            table.append(arrow_table, snapshot_properties=snapshot_props)
        else:
            table.append(arrow_table)

    def _overwrite() -> None:
        if snapshot_props:
            table.overwrite(arrow_table, snapshot_properties=snapshot_props)
        else:
            table.overwrite(arrow_table)

    try:
        if config.write_mode == "overwrite":
            retry(_overwrite, log=log, op="overwrite")
        else:
            retry(_append, log=log, op="append")
    except TypeError:
        # Older PyIceberg versions may not support snapshot_properties kwarg.
        if config.write_mode == "overwrite":
            retry(lambda: table.overwrite(arrow_table), log=log, op="overwrite")
        else:
            retry(lambda: table.append(arrow_table), log=log, op="append")
    except Exception as exc:  # noqa: BLE001
        if config.write_mode == "append" and is_table_partitioned(table):
            raise RuntimeError(
                "Append failed for a partitioned Iceberg table. PyIceberg partitioned writes may "
                "be limited depending on table spec and version. Consider using unpartitioned "
                "tables for now, switching to a compatible dynamic partition overwrite workflow, "
                "or using an engine with broader partitioned write support.",
            ) from exc
        raise
