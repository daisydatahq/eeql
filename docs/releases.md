# Release Automation

## Secrets
Add the following GitHub Actions secrets:
- `TEST_PYPI_API_TOKEN` (TestPyPI)
- `PYPI_API_TOKEN` (PyPI)

## Two-Stage Publish Flow
1. Pull requests to `main` build and upload to TestPyPI.
2. Merges to `main` publish to PyPI when `pyproject.toml` changed.
3. If the version already exists on PyPI, the workflow skips the upload.

## Install From TestPyPI
```bash
pip install -i https://test.pypi.org/simple eeql==<version>
```
