# Changelog

## Unreleased

- Add Iceberg commit retries with exponential backoff, status checks for unknown commit state, and
  optional metadata-only retries to avoid rewriting data files.
- Add commit retry tuning settings, per-table commit locking, and optional commit retry table
  properties for existing tables.
