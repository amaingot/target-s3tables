# target-s3tables

Singer target (Meltano Singer SDK) which loads Singer streams into **Amazon S3 Tables** using **Apache Iceberg** via **PyIceberg** and the **Iceberg REST catalog** with **AWS SigV4** signing.

This is **not** a “write Parquet files to an S3 bucket” target — it uses Iceberg catalog operations and is intended for **S3 Tables table buckets**.

## Install

Local dev:

```bash
pip install -e .
```

Or with `uv`:

```bash
uv sync
uv run target-s3tables --version
```

## AWS auth

By default, the target relies on the standard AWS credential chain (env vars, profiles, ECS/EC2 roles, etc.). You can optionally pass `aws_access_key_id`, `aws_secret_access_key`, and `aws_session_token` in config to override via environment variables.

## Catalog modes

### 1) Glue Iceberg REST endpoint (recommended)

Uses the AWS Glue Iceberg REST endpoint (centralized governance).

Example config (`config.glue.json`):

```json
{
  "catalog_mode": "glue_rest",
  "region": "us-east-1",
  "namespace": "default",
  "account_id": "123456789012",
  "table_bucket_name": "my-table-bucket",
  "sigv4_enabled": true,
  "signing_name": "glue",
  "signing_region": "us-east-1",
  "write_mode": "append",
  "batch_size_rows": 5000
}
```

Notes:

- `glue_uri` defaults to `https://glue.<region>.amazonaws.com/iceberg`
- `glue_warehouse` defaults to `<account-id>:s3tablescatalog/<table-bucket-name>`

### 2) S3 Tables Iceberg REST endpoint (direct)

Uses the S3 Tables Iceberg REST endpoint (direct access to a single table bucket).

Example config (`config.s3tables.json`):

```json
{
  "catalog_mode": "s3tables_rest",
  "region": "us-east-1",
  "namespace": "default",
  "table_bucket_arn": "arn:aws:s3tables:us-east-1:123456789012:bucket/my-table-bucket",
  "sigv4_enabled": true,
  "signing_name": "s3tables",
  "signing_region": "us-east-1",
  "write_mode": "append",
  "batch_size_rows": 5000
}
```

Notes:

- `s3tables_uri` defaults to `https://s3tables.<region>.amazonaws.com/iceberg`
- S3 Tables direct mode supports **single-level namespaces only** (no `foo.bar`).
- The required REST path prefix is the URL-encoded table bucket ARN (handled automatically).

## Usage

Run directly:

```bash
target-s3tables --about
target-s3tables --version
tap-smoke-test | target-s3tables --config config.glue.json
```

Environment-variable config (loads `.env` in the working directory when `--config=ENV` is used):

```bash
tap-smoke-test | target-s3tables --config=ENV
```

## Schema evolution

- If `create_tables=true`, tables are created on first sight of a stream schema.
- If `evolve_schema=true`, schema updates are applied via `table.update_schema().union_by_name(...)`.

If you write to an existing **partitioned** table and `append` fails, the target raises a message with options (unpartitioned tables, dynamic partition overwrite for compatible cases, or another engine).

## Meltano (custom loader plugin)

Add to `meltano.yml`:

```yaml
plugins:
  loaders:
  - name: target-s3tables
    namespace: target_s3tables
    pip_url: -e .
    settings:
    - name: catalog_mode
    - name: region
    - name: namespace
    - name: account_id
    - name: table_bucket_name
    - name: table_bucket_arn
    - name: write_mode
    - name: batch_size_rows
    - name: sanitize_names
    - name: create_tables
    - name: evolve_schema
    - name: signing_name
    - name: signing_region
    - name: sigv4_enabled
    - name: table_properties
    - name: snapshot_properties
    - name: debug_http
    - name: aws_access_key_id
    - name: aws_secret_access_key
      kind: password
    - name: aws_session_token
      kind: password
```

Run:

```bash
meltano run <tap-name> target-s3tables
```

## Settings reference (`--about`)

Copy-paste of `target-s3tables --about --format=markdown`:

# `target-s3tables`

Load Singer streams into Amazon S3 Tables via PyIceberg REST catalogs.

Built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Capabilities

- `about`
- `stream-maps`
- `schema-flattening`
- `structured-logging`
- `validate-records`

## Supported Python Versions

- 3.10
- 3.11
- 3.12
- 3.13
- 3.14

## Settings

| Setting | Required | Default | Description |
|:--------|:--------:|:-------:|:------------|
| catalog_mode | False | glue_rest | Iceberg REST catalog mode to use (AWS Glue recommended). |
| region | True | None | AWS region for the Iceberg REST endpoint (e.g. us-east-1). |
| namespace | False | default | Iceberg namespace (database). |
| write_mode | False | append | Write mode: append for incremental; overwrite to replace table contents. |
| batch_size_rows | False | 5000 | Max rows per Iceberg commit. |
| batch_max_bytes | False | None | Optional approximate byte limit for an in-memory batch. |
| sanitize_names | False | True | Sanitize stream/table/column names to Iceberg/AWS-friendly identifiers. |
| create_tables | False | True | Create Iceberg tables when missing. |
| evolve_schema | False | True | Evolve Iceberg schema when stream schema changes. |
| table_name_prefix | False |  | Prefix applied to all Iceberg table names. |
| table_name_mapping | False | {} | Mapping of Singer stream name -> Iceberg table name. |
| glue_uri | False | None | Glue Iceberg REST endpoint URI. Defaults to https://glue.<region>.amazonaws.com/iceberg. |
| glue_warehouse | False | None | Glue warehouse string: <account-id>:s3tablescatalog/<table-bucket-name>. |
| account_id | False | None | AWS account id (used to build glue_warehouse if not provided). |
| table_bucket_name | False | None | S3 Tables table bucket name (used to build glue_warehouse if not provided). |
| s3tables_uri | False | None | S3 Tables Iceberg REST endpoint URI. Defaults to https://s3tables.<region>.amazonaws.com/iceberg. |
| table_bucket_arn | False | None | Table bucket ARN: arn:aws:s3tables:<region>:<accountID>:bucket/<bucketname>. |
| sigv4_enabled | False | True | Enable AWS SigV4 request signing for the Iceberg REST catalog. |
| signing_name | False | None | SigV4 signing name (defaults to glue or s3tables based on mode). |
| signing_region | False | None | SigV4 signing region (defaults to `region`). |
| aws_access_key_id | False | None | Optional AWS access key id override (otherwise use default AWS credential chain). |
| aws_secret_access_key | False | None | Optional AWS secret access key override (otherwise use default AWS credential chain). |
| aws_session_token | False | None | Optional AWS session token override. |
| table_properties | False | {} | Iceberg table properties passed at create_table time. |
| snapshot_properties | False | {} | Snapshot properties passed to append/overwrite calls (when supported). |
| debug_http | False | False | Enable debug logging for HTTP/SigV4 interactions. |
| log_level | False | None | Optional log level override for this process (e.g. DEBUG, INFO). |
| add_record_metadata | False | None | Whether to add metadata fields to records. |
| load_method | False | TargetLoadMethods.APPEND_ONLY | The method to use when loading data into the destination. `append-only` will always write all input records whether that records already exists or not. `upsert` will update existing records and insert new records. `overwrite` will delete all existing records and insert all input records. |
| validate_records | False | True | Whether to validate the schema of the incoming streams. |
| stream_maps | False | None | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_maps.__else__ | False | None | Currently, only setting this to `__NULL__` is supported. This will remove all other streams. |
| stream_map_config | False | None | User-defined config values to be used within map expressions. |
| faker_config | False | None | Config for the [`Faker`](https://faker.readthedocs.io/en/master/) instance variable `fake` used within map expressions. Only applicable if the plugin specifies `faker` as an additional dependency (through the `singer-sdk` `faker` extra or directly). |
| faker_config.seed | False | None | Value to seed the Faker generator for deterministic output: https://faker.readthedocs.io/en/master/#seeding-the-generator |
| faker_config.locale | False | None | One or more LCID locale strings to produce localized output for: https://faker.readthedocs.io/en/master/#localization |
| flattening_enabled | False | None | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth | False | None | The max depth to flatten schemas. |
| flattening_max_key_length | False | None | The maximum length of a flattened key. |

A full list of supported settings and capabilities is available by running: `target-s3tables --about`
