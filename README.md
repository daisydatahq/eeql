# eeql
Entity-Event Query Language (EEQL) - a DSL and Python interface for querying event data.

## EEQL language pipeline
- Parse: `eeql.parser.parse(text)` → AST with spans.
- Validate: `eeql.validator.validate_query(ast, catalog)` enforces spec (aggregation-required, entity/column existence, join uniqueness).
- Compile (stub): `eeql.compiler.compile_to_dataset(ast, catalog)` returns a placeholder result; full Dataset wiring lands in DAI-154.

## LSP / editor support
- Run `eeql-lsp` (installed via console script) to expose diagnostics/completions/hover over the Language Server Protocol.
- Provide a Catalog via `EEQL_CATALOG_MODULE` env var (must expose `build()`), or use the built-in demo catalog.

## Release Automation
Pull requests to `main` publish prereleases to TestPyPI for quick validation. Merges to
`main` publish releases to PyPI when `pyproject.toml` has a new version.

TestPyPI install:

```bash
pip install -i https://test.pypi.org/simple eeql==<version>
```
