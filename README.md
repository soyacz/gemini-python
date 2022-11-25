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


```
