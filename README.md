# datasets-sql

A 🤗 Datasets extension package that provides support for executing arbitrary SQL queries on `Dataset` objects. It uses [DuckDB](https://duckdb.org/) as a SQL engine and follows its [query syntax](https://duckdb.org/docs/sql/introduction#querying-a-table).

## Installation

```bash
pip install datasets_sql
```

## Quick Start

```python
from datasets import load_dataset, Dataset
from datasets_sql import query

imdb_dset = load_dataset("imdb", split="train")

# Remove the rows where the `text` field has less than 1000 characters
imdb_query_dset1 = query("SELECT text FROM imdb_dset WHERE length(text) > 1000")

# Count the number of rows per label
imdb_query_dset2 = query("SELECT label, COUNT(*) as num_rows FROM imdb_dset GROUP BY label")

# Remove duplicated rows
imdb_query_dset3 = query("SELECT DISTINCT text FROM imdb_dset")

# Get the average length of the `text` field
imdb_query_dset4 = query("SELECT AVG(length(text)) as avg_text_length FROM imdb_dset")

order_customer_dset = Dataset.from_dict({
    "order_id": [10001, 10002, 10003],
    "customer_id": [3, 1, 2],
})

customer_dset = Dataset.from_dict({
    "customer_id": [1, 2, 3],
    "name": ["John", "Jane", "Mary"],
})

# Join two tables
join_query_dset = query(
    "SELECT order_id, name FROM order_customer_dset INNER JOIN customer_dset ON order_customer_dset.customer_id = customer_dset.customer_id"
)
```
