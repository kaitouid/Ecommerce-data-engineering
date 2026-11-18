from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any


EXPECTED_DATASETS: dict[str, tuple[str, tuple[str, ...]]] = {
    "customers": (
        "olist_customers_dataset.csv",
        (
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ),
    ),
    "orders": (
        "olist_orders_dataset.csv",
        (
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ),
    ),
    "order_items": (
        "olist_order_items_dataset.csv",
        (
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ),
    ),
    "products": (
        "olist_products_dataset.csv",
        (
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ),
    ),
    "sellers": (
        "olist_sellers_dataset.csv",
        ("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"),
    ),
    "payments": (
        "olist_order_payments_dataset.csv",
        ("order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"),
    ),
    "reviews": (
        "olist_order_reviews_dataset.csv",
        (
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
        ),
    ),
    "geolocation": (
        "olist_geolocation_dataset.csv",
        (
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state",
        ),
    ),
    "category_translation": (
        "product_category_name_translation.csv",
        ("product_category_name", "product_category_name_english"),
    ),
}


class CsvIngestionError(ValueError):
    """Raised when a source CSV cannot be safely inspected."""


@dataclass(frozen=True)
class IngestionResult:
    """Metadata for one source CSV inspected by an ingestion run."""

    dataset: str
    source_file: str
    status: str
    row_count: int | None = None
    columns: tuple[str, ...] = ()
    encoding: str | None = None
    sha256: str | None = None
    size_bytes: int | None = None
    error: str | None = None


@dataclass(frozen=True)
class IngestionReport:
    """Summary and manifest location for one ingestion run."""

    run_id: str
    started_at: str
    duration_seconds: float
    results: tuple[IngestionResult, ...]
    manifest_path: Path

    @property
    def succeeded(self) -> bool:
        """Return whether every expected source file was ingested successfully."""
        return all(result.status == "success" for result in self.results)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation of the run."""
        successful = sum(result.status == "success" for result in self.results)
        return {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "duration_seconds": self.duration_seconds,
            "status": "success" if self.succeeded else "failed",
            "summary": {
                "datasets": len(self.results),
                "succeeded": successful,
                "failed": len(self.results) - successful,
                "rows_read": sum(result.row_count or 0 for result in self.results),
            },
            "datasets": [asdict(result) for result in self.results],
        }


class CsvIngestor:
    """Inspect Olist CSVs without changing or moving the source files."""

    def __init__(
        self,
        source_dir: str | Path,
        manifest_dir: str | Path,
        logger: logging.Logger | None = None,
    ) -> None:
        self.source_dir = Path(source_dir)
        self.manifest_dir = Path(manifest_dir)
        self.logger = logger or logging.getLogger(__name__)

    def ingest_all(self, run_id: str | None = None) -> IngestionReport:
        """Inspect all expected CSVs and persist a manifest, including on failure."""
        effective_run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        if not re.fullmatch(r"[A-Za-z0-9_-]+", effective_run_id):
            raise ValueError("run_id may contain only letters, numbers, underscores, and hyphens")

        started = datetime.now(timezone.utc)
        started_at = started.isoformat()
        results: list[IngestionResult] = []

        if not self.source_dir.is_dir():
            message = f"Source directory does not exist: {self.source_dir}"
            self.logger.error(message)
            results.extend(
                IngestionResult(
                    dataset=dataset,
                    source_file=filename,
                    status="failed",
                    error=message,
                )
                for dataset, (filename, _) in EXPECTED_DATASETS.items()
            )
        else:
            for dataset, (filename, required_columns) in EXPECTED_DATASETS.items():
                result = self._inspect_dataset(dataset, filename, required_columns)
                results.append(result)
                if result.status == "success":
                    self.logger.info(
                        "Ingested %s: rows=%d sha256=%s",
                        dataset,
                        result.row_count,
                        result.sha256,
                    )
                else:
                    self.logger.error("Could not ingest %s: %s", dataset, result.error)

        manifest_path = self.manifest_dir / effective_run_id / "manifest.json"
        report = IngestionReport(
            run_id=effective_run_id,
            started_at=started_at,
            duration_seconds=(datetime.now(timezone.utc) - started).total_seconds(),
            results=tuple(results),
            manifest_path=manifest_path,
        )
        self._write_manifest(report)
        return report

    def _inspect_dataset(
        self,
        dataset: str,
        filename: str,
        required_columns: tuple[str, ...],
    ) -> IngestionResult:
        source_path = self.source_dir / filename
        if not source_path.is_file():
            return IngestionResult(
                dataset=dataset,
                source_file=filename,
                status="failed",
                error=f"Required source file is missing: {source_path}",
            )

        try:
            initial_stat = source_path.stat()
            if initial_stat.st_size == 0:
                raise CsvIngestionError("File is empty (zero bytes)")

            sha256 = self._sha256(source_path)
            row_count, columns, encoding = self._scan_csv(source_path, required_columns)
            final_stat = source_path.stat()
            if (initial_stat.st_size, initial_stat.st_mtime_ns) != (
                final_stat.st_size,
                final_stat.st_mtime_ns,
            ):
                raise CsvIngestionError("File changed while it was being ingested")

            return IngestionResult(
                dataset=dataset,
                source_file=filename,
                status="success",
                row_count=row_count,
                columns=columns,
                encoding=encoding,
                sha256=sha256,
                size_bytes=final_stat.st_size,
            )
        except (CsvIngestionError, OSError, csv.Error) as error:
            return IngestionResult(
                dataset=dataset,
                source_file=filename,
                status="failed",
                error=str(error),
            )

    @staticmethod
    def _scan_csv(
        source_path: Path,
        required_columns: tuple[str, ...],
    ) -> tuple[int, tuple[str, ...], str]:
        last_decode_error: UnicodeDecodeError | None = None
        for encoding in ("utf-8-sig", "latin-1"):
            try:
                with source_path.open("r", encoding=encoding, newline="") as source:
                    reader = csv.reader(source, strict=True)
                    columns = next(reader, None)
                    if not columns:
                        raise CsvIngestionError("File is empty or has no header row")
                    if any(not column.strip() for column in columns):
                        raise CsvIngestionError("Header contains an empty column name")
                    if len(columns) != len(set(columns)):
                        raise CsvIngestionError("Header contains duplicate column names")

                    missing_columns = sorted(set(required_columns) - set(columns))
                    if missing_columns:
                        raise CsvIngestionError(
                            f"Missing required columns: {', '.join(missing_columns)}"
                        )

                    row_count = 0
                    for line_number, row in enumerate(reader, start=2):
                        if len(row) != len(columns):
                            raise CsvIngestionError(
                                f"Malformed row at line {line_number}: expected "
                                f"{len(columns)} fields, found {len(row)}"
                            )
                        row_count += 1

                    if row_count == 0:
                        raise CsvIngestionError("File contains a header but no data rows")
                    return row_count, tuple(columns), encoding
            except UnicodeDecodeError as error:
                last_decode_error = error

        if last_decode_error is not None:
            raise CsvIngestionError(
                f"Unable to decode CSV: {last_decode_error}"
            ) from last_decode_error
        raise CsvIngestionError("Unable to read CSV")

    @staticmethod
    def _sha256(source_path: Path) -> str:
        digest = hashlib.sha256()
        with source_path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _write_manifest(self, report: IngestionReport) -> None:
        report.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(report.to_dict(), indent=2, ensure_ascii=False)
        temporary_path: Path | None = None
        try:
            with NamedTemporaryFile(
                "w",
                encoding="utf-8",
                dir=report.manifest_path.parent,
                delete=False,
            ) as temporary_file:
                temporary_file.write(payload)
                temporary_file.write("\n")
                temporary_path = Path(temporary_file.name)
            try:
                os.link(temporary_path, report.manifest_path)
            except FileExistsError as error:
                raise FileExistsError(
                    f"Manifest already exists for run_id '{report.run_id}'; "
                    "choose a unique run_id to avoid overwriting prior metadata"
                ) from error
        finally:
            if temporary_path is not None and temporary_path.exists():
                temporary_path.unlink()
