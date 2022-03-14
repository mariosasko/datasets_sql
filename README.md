# datasets-sql

A 🤗 Datasets extension package that provides support for executing arbitrary SQL queries on `Dataset` objects. It uses [DuckDB](https://duckdb.org/) as a SQL engine.

## Installation

```bash
pip install datasets-sql
```

## Quick Start

```python
from datasets import load_dataset
from datasets_sql import query

dset = load_dataset("imdb", split="train")

# Remove the rows where the `text` field has less than 100 characters
dset = query("SELECT text FROM dset WHERE length(text) > 100")
```