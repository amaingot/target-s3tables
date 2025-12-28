"""S3Tables entry point."""

from __future__ import annotations

from target_s3tables.target import TargetS3Tables


def main() -> None:
    """Run the target CLI (used by `python -m target_s3tables`)."""
    # pylint: disable=no-value-for-parameter
    TargetS3Tables.cli()


if __name__ == "__main__":
    main()
