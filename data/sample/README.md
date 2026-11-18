# Synthetic development sample

These small CSVs are fabricated solely to exercise the ingestion workflow when the
Olist dataset has not been downloaded. They are not a subset of the Kaggle dataset,
do not represent actual customers or transactions, and must not be used to claim
real business results.

To run ingestion against these files from the repository root:

```bash
DATASET_DIR=data/sample python -m data_pipeline.main --task ingest
```

Install the project first (`python -m pip install -e '.[dev]'`) and use Python 3.12.
For the full project, manually download the Olist dataset from Kaggle and place the
original CSVs in `data/raw/`.
