from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from data_pipeline.ingestion.csv_ingestor import CsvIngestor, EXPECTED_DATASETS


class CsvIngestorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        self.source_dir = self.root / "source"
        self.source_dir.mkdir()
        self.manifest_dir = self.root / "manifests"
        self._create_source_files()

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def _create_source_files(self) -> None:
        for dataset, (filename, columns) in EXPECTED_DATASETS.items():
            path = self.source_dir / filename
            with path.open("w", encoding="utf-8", newline="") as source:
                writer = csv.writer(source)
                writer.writerow(columns)
                writer.writerow([f"{dataset}-sample"] * len(columns))

    def _ingestor(self) -> CsvIngestor:
        return CsvIngestor(self.source_dir, self.manifest_dir)

    def test_ingests_expected_files_and_writes_manifest(self) -> None:
        original_files = {
            path.name: path.read_bytes() for path in self.source_dir.iterdir()
        }

        report = self._ingestor().ingest_all(run_id="test-run")

        self.assertTrue(report.succeeded)
        self.assertEqual(len(EXPECTED_DATASETS), len(report.results))
        self.assertTrue(all(result.row_count == 1 for result in report.results))
        self.assertTrue(all(len(result.sha256 or "") == 64 for result in report.results))
        self.assertTrue(report.manifest_path.is_file())
        self.assertEqual(
            original_files,
            {path.name: path.read_bytes() for path in self.source_dir.iterdir()},
        )

        manifest = json.loads(report.manifest_path.read_text(encoding="utf-8"))
        self.assertEqual("success", manifest["status"])
        self.assertEqual(len(EXPECTED_DATASETS), manifest["summary"]["succeeded"])

    def test_missing_file_fails_run_and_is_reported(self) -> None:
        missing_filename = EXPECTED_DATASETS["orders"][0]
        (self.source_dir / missing_filename).unlink()

        report = self._ingestor().ingest_all(run_id="missing-file")

        self.assertFalse(report.succeeded)
        missing_result = next(result for result in report.results if result.dataset == "orders")
        self.assertEqual("failed", missing_result.status)
        self.assertIn("missing", missing_result.error or "")
        self.assertTrue(report.manifest_path.is_file())

    def test_empty_and_header_only_files_fail(self) -> None:
        filename, columns = EXPECTED_DATASETS["customers"]
        (self.source_dir / filename).write_bytes(b"")
        report = self._ingestor().ingest_all(run_id="empty-file")
        result = next(item for item in report.results if item.dataset == "customers")
        self.assertEqual("failed", result.status)
        self.assertIn("empty", result.error or "")

        with (self.source_dir / filename).open("w", encoding="utf-8", newline="") as source:
            csv.writer(source).writerow(columns)
        report = self._ingestor().ingest_all(run_id="header-only")
        result = next(item for item in report.results if item.dataset == "customers")
        self.assertEqual("failed", result.status)
        self.assertIn("no data rows", result.error or "")

    def test_missing_required_column_fails_file(self) -> None:
        filename, _ = EXPECTED_DATASETS["orders"]
        (self.source_dir / filename).write_text("order_id,customer_id\n1,2\n", encoding="utf-8")

        report = self._ingestor().ingest_all(run_id="bad-header")

        result = next(item for item in report.results if item.dataset == "orders")
        self.assertEqual("failed", result.status)
        self.assertIn("Missing required columns", result.error or "")

    def test_malformed_row_fails_file(self) -> None:
        filename, columns = EXPECTED_DATASETS["orders"]
        (self.source_dir / filename).write_text(
            ",".join(columns) + "\nonly-one-field\n",
            encoding="utf-8",
        )

        report = self._ingestor().ingest_all(run_id="malformed-row")

        result = next(item for item in report.results if item.dataset == "orders")
        self.assertEqual("failed", result.status)
        self.assertIn("Malformed row at line 2", result.error or "")

    def test_latin1_csv_is_supported(self) -> None:
        filename, columns = EXPECTED_DATASETS["customers"]
        row = ["customer-1", "unique-1", "12345", "São Paulo", "SP"]
        with (self.source_dir / filename).open("w", encoding="latin-1", newline="") as source:
            writer = csv.writer(source)
            writer.writerow(columns)
            writer.writerow(row)

        report = self._ingestor().ingest_all(run_id="latin1")

        result = next(item for item in report.results if item.dataset == "customers")
        self.assertEqual("success", result.status)
        self.assertEqual("latin-1", result.encoding)
        self.assertEqual(1, result.row_count)

    def test_existing_run_manifest_is_not_overwritten(self) -> None:
        first_report = self._ingestor().ingest_all(run_id="repeat")
        original_manifest = first_report.manifest_path.read_bytes()

        with self.assertRaises(FileExistsError):
            self._ingestor().ingest_all(run_id="repeat")

        self.assertEqual(original_manifest, first_report.manifest_path.read_bytes())

    def test_run_id_cannot_escape_manifest_directory(self) -> None:
        with self.assertRaises(ValueError):
            self._ingestor().ingest_all(run_id="../outside")


if __name__ == "__main__":
    unittest.main()
