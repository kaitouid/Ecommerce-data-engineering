from __future__ import annotations

import argparse
import logging

from data_pipeline.config import get_settings
from data_pipeline.ingestion import CsvIngestor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="E-commerce data engineering pipeline")
    parser.add_argument(
        "--task",
        choices=["ingest", "validate", "transform", "load", "analytics", "full"],
        default="full",
        help="Pipeline task to execute.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.task != "ingest":
        parser.error(f"task '{args.task}' is not implemented yet; use --task ingest for Phase 2")

    settings = get_settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger = logging.getLogger("data_pipeline")
    report = CsvIngestor(
        source_dir=settings.dataset_dir,
        manifest_dir=f"{settings.processed_data_dir}/ingestion",
        logger=logger,
    ).ingest_all()

    log_method = logger.info if report.succeeded else logger.error
    log_method(
        "Ingestion run %s completed with status=%s, datasets=%d, rows=%d, "
        "duration_seconds=%.3f, manifest=%s",
        report.run_id,
        "success" if report.succeeded else "failed",
        len(report.results),
        sum(result.row_count or 0 for result in report.results),
        report.duration_seconds,
        report.manifest_path,
    )
    return 0 if report.succeeded else 1


if __name__ == "__main__":
    raise SystemExit(main())
