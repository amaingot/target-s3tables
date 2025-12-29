# Repository Guidelines

## Project Structure & Module Organization

- `target_s3tables/`: Core Singer target implementation.
  - `target.py`: CLI entrypoint (`target-s3tables`) and settings schema.
  - `sinks.py`: Singer SDK sinks for writing batches/streams.
  - `iceberg.py`: PyIceberg catalog/table interactions (Glue/S3 Tables REST).
  - `config.py`: Config parsing, validation, and AWS env overrides.
- `tests/`: `pytest` suite (`test_*.py`). Includes optional AWS integration smoke tests.
- `meltano.yml`: Local Meltano project wiring `tap-smoke-test -> target-s3tables`.
- Tooling/config: `pyproject.toml`, `uv.lock`, `.pre-commit-config.yaml`, `.github/workflows/`.

## Build, Test, and Development Commands

- `uv sync --frozen --all-groups`: Create/update the local environment from `uv.lock` (includes `dev`/`test`/`typing` groups).
- `uv run target-s3tables --about`: Run the target CLI in the managed env.
- `uv run pytest`: Run unit tests (after installing the `test` dependency group).
- `uvx --with=tox-uv tox -e typing`: Run type checks (mypy + ty), matching CI.
- `uvx --with=tox-uv tox -e py312`: Run tests in a tox env (see `[tool.tox]` in `pyproject.toml`).
- `pre-commit run --all-files`: Run repo hooks (Ruff hooks run in check mode via `--diff`).
- `uv tool install meltano` then `meltano run tap-smoke-test target-s3tables`: Run the sample pipeline from `meltano.yml`.

## Coding Style & Naming Conventions

- Python >= 3.10, 4-space indentation, type hints encouraged.
- Ruff formatting and linting (`line-length = 100`) are configured in `pyproject.toml` and enforced via `pre-commit`.
- Naming: `snake_case` for functions/vars, `PascalCase` for classes, `UPPER_SNAKE_CASE` for constants.

## Testing Guidelines

- Framework: `pytest`. Tests live in `tests/` and are named `test_*.py`.
- Integration smoke tests are marked `integration` and require `TARGET_S3TABLES_INTEGRATION=1` plus `TARGET_S3TABLES_*` env vars (see `tests/test_integration_smoke.py`).

## Commit & Pull Request Guidelines

- Commit subjects follow the existing history’s imperative style (e.g., “Add …”, “Update …”, “Refactor …”, “Fix …”).
- PRs include: a short description, how you tested (`uv run pytest`/tox plus `pre-commit run --all-files`), and any AWS-side assumptions (catalog mode/region/table bucket).
- Keep diffs focused; update `uv.lock` when changing dependencies.

## Security & Configuration Tips

- Do not commit credentials. Prefer the standard AWS credential chain; use `.env.example` → `.env` for local overrides.
- Local Meltano state lives in `.meltano/` (ignored); keep secrets in `.secrets/` (ignored).
