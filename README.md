# eeql
Entity-Event Query Language (EEQL) - a DSL and Python interface for querying event data.

## Release Automation
Pull requests to `main` publish prereleases to TestPyPI for quick validation. Merges to
`main` publish releases to PyPI when `pyproject.toml` has a new version.

TestPyPI install:

```bash
pip install -i https://test.pypi.org/simple eeql==<version>
```
