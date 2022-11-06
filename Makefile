.PHONY: quality style test

# Check that source code meets quality standards

quality:
	black --check --line-length 119 --target-version py37 datasets_sql tests
	isort --check-only datasets_sql tests
	flake8 datasets_sql tests

# Format source code automatically

style:
	black --line-length 119 --target-version py37 datasets_sql tests
	isort datasets_sql tests

test:
	python -m pytest -sv ./tests/