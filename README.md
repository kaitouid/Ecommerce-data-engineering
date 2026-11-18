# E-Commerce Data Engineering Pipeline

A phased portfolio project for ingesting and analyzing the Brazilian E-Commerce
Public Dataset by Olist. The repository is under active development; this phase
implements CSV ingestion only.

## Requirements

- Python 3.12
- Git

## Install

From the repository root:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
cp .env.example .env
```

The `.env` file is local configuration and is excluded from Git. Do not put
production credentials in it.

## Provide source data

Download the Olist CSV files manually from the
[Kaggle dataset page](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
and place the original files in `data/raw/`. This project does not download data
from Kaggle. Ingestion reads those files without modifying or moving them.

If the original dataset is not available, use the small fabricated files in
`data/sample/` for development. They are explicitly synthetic and must not be used
to report real business results.

## Run Phase 2 ingestion

For downloaded Olist data in `data/raw/`:

```bash
python -m data_pipeline.main --task ingest
```

For the synthetic smoke-test files:

```bash
DATASET_DIR=data/sample python -m data_pipeline.main --task ingest
```

The ingestor checks that all nine expected files exist, verifies required headers,
scans CSV rows, supports UTF-8 (including a BOM) and Latin-1, and computes a SHA-256
checksum for each successful file. Each run writes an ingestion manifest to
`data/processed/ingestion/<run-id>/manifest.json`. Failed files are recorded in
the manifest, and the command exits nonzero if any required file fails. The
original source CSVs are never rewritten.

## Tests

```bash
python -m pytest
```

The ingestion tests use temporary CSV fixtures and do not require the Olist
download or PostgreSQL.

## Project status

- Phase 1: project scaffold and local PostgreSQL configuration
- Phase 2: CSV source ingestion and metadata manifest
- Later phases: validation, transformation, warehouse loading, incremental
  processing, analytics, automation, and full project documentation
