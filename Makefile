.PHONY: quality style test

# Check that source code meets quality standards

quality:
	black --check --line-length 119 --target-version py37 datasets_sql
	isort --check-only datasets_sql
	flake8 datasets_sql

# Format source code automatically

style:
	black --line-length 119 --target-version py37 datasets_sql
	isort datasets_sql

test:
	python -m pytest -n auto --dist=loadfile -s -v ./tests/