# EEQL LSP / Integration Guide

## What ships in `eeql`
- `eeql.lsp_core`: pure language services (`diagnostics`, `completions`, `hover`) taking plain text + a Catalog.
- `eeql.lsp.server`: a pygls-based Language Server Protocol (LSP) entrypoint. Installed as `eeql-lsp`.
- `eeql.catalog.demo`: tiny in-memory catalog for quick testing.
- See `docs/http_adapter_example.md` for a FastAPI/WebSocket adapter pattern.

## Running the LSP (VS Code / Cursor)
```bash
# Optional: point to your catalog builder module with build()
export EEQL_CATALOG_MODULE=eeql.catalog.demo
uv run --active python -m eeql.lsp.server   # or: eeql-lsp
```
Configure your editor to launch `eeql-lsp` for the `eeql` language.

## HTTP / WebSocket example
```bash
uv run --active python -m uvicorn path.to.your_adapter:app --reload
# POST /diagnostics {text, line?, col?}
# WS /eeql with {"op":"completions","text":"...","line":1,"col":5}
```

## Catalog contract
- Provide an object implementing `Catalog` (has_event/get_event/get_entity/get_attribute/default_entity).
- Events are real `eeql.core.Event` with entities and attributes.
- See `eeql.catalog.demo.build()` for a reference shape.

## Notebook magic (concept)
- Call `lsp_core.diagnostics(text, catalog)` and `compiler.compile_to_dataset(...)` inside a `%%eeql` cell.
- Register a completer that calls `lsp_core.completions(text, pos, catalog)` for inline suggestions.
