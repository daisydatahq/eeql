# Contributing

## Versioning
- Bump the package version in `pyproject.toml` under `[project].version`.
- Every version bump should be unique; duplicate versions are skipped by CI.

## Release Flow
- Pull requests to `main` publish a prerelease build to TestPyPI.
- Merges to `main` publish to PyPI when `pyproject.toml` changed.

## Required Secrets
Configure these in the GitHub repo settings:
- `TEST_PYPI_API_TOKEN`
- `PYPI_API_TOKEN`
