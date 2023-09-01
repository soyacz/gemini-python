# Gemini python
Port of Gemini tool (Go language) to Python.

# How it works
When user starts 'gemini' (`console.py`) it first creates schema (if not exists).
Then starts number of workers (`GeminiProcess`) that run the main part of Gemini:
inserting data to SUT and oracle, comparing results and reporting errors.
## Data generation
Each `GeminiProcess` upon startup generates partitions data (values for all partition keys,
which number is configured by `--token-number-slices` start arg).
Because seed is specified it generates always the same partitions.
Each `GeminiProcess` is a separate OS process and works synchronously to avoid problems with Python
driver which cannot operate async on 2 different databases.
It works in a loop, where in each iteration it generates values for clustering keys and columns for selected partition
and inserts them to SUT and oracle.
### History store
Each `GeminiProcess` has its own`HistoryStore` (currently in sqlite database, in ramdisk if created)
that stores all the partition and clustering keys values that were inserted. This is used for future
read operations where we can query only data that was inserted. (Todo: use history store to verify data ressurection (including oracle).)
## How data is queried
Main `GeminiProcess` loop also can generate read queries (alternates between insert and read in mixed mode).
Select filters (partition and clustering keys values) are taken randomly from `HistoryStore` and then query is executed.
Data is compared with oracle and errors are reported (depending on `--max-mutation-retries` parameter -
if set to 0 then error is reported immediately, otherwise it retries `--max-mutation-retries` times after
`--max-mutation-retries-backoff` time).

## Missing features
- only simple column types
- no support for collections
- no support for materialized views
- no support for LWT
- no support for counters
- no support for user defined types
- more

# How to run
## Prerequisites
- Python 3.9
- poetry
- docker (for testing locally)

## Running locally
```
cd gemini-python
poetry shell
scripts/start_db.sh
gemini --help
gemini -t 192.168.100.2 -o 192.168.100.3 --drop-schema
```

# Working with code

Before commit:
```
git add ...  # add all the changes, remember to update the version
pre-commit

# or manually:
black .
scripts/run_mypy.sh
pylint gemini_python
pytest .

# also worth to check coverage:
coverage run --source=gemini_python -m pytest .
coverage report --fail-under 80

```
Release:
```
1. update version in pyproject.toml
2. pre-commit run --all

3. poetry build
3. docker build --tag scylladb/hydra-loaders:gemini-python-0.4.0 .
4. docker push scylladb/hydra-loaders:gemini-python-0.4.0

```
