# Gemini python
Port of Gemini tool (Go language) to Python.

# Info
Development:
```
cd gemini-python
poetry shell
gemini --help
scripts/start_db.sh
gemini -t 192.168.100.2 -o 192.168.100.3
```
Before commit:
```
git add -p  # add all the changes
pre-commit

# or manually:
black .
scripts/run_mypy.sh
pylint gemini_python
pytest .

# also worth to check coverage:
coverage run --source=gemini_python -m pytest .

```
Release:
```
1. update version in pyproject.toml
2. pre-commit run --all

3. poetry build
3. docker build --tag scylladb/hydra-loaders:gemini-python-0.4.0 .
4. docker push scylladb/hydra-loaders:gemini-python-0.4.0

```
