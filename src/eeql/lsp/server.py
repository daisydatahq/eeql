from __future__ import annotations

import importlib
import os
from functools import lru_cache

from pygls.lsp.server import LanguageServer
from lsprotocol import types

from eeql.lsp import core as lsp_core
from eeql.catalog.demo import build as demo_build


SERVER_NAME = "eeql-lsp"
SERVER_VERSION = "0.1.0"


def _ls_range(rng: lsp_core.Range) -> types.Range:
    return types.Range(
        start=types.Position(line=rng.start.line, character=rng.start.character),
        end=types.Position(line=rng.end.line, character=rng.end.character),
    )


def _ls_diag(d: lsp_core.Diagnostic) -> types.Diagnostic:
    return types.Diagnostic(range=_ls_range(d.range), message=d.message)


def _ls_completion(c: lsp_core.CompletionItem) -> types.CompletionItem:
    return types.CompletionItem(label=c.label, detail=c.detail)


def _ls_hover(h: lsp_core.Hover) -> types.Hover:
    return types.Hover(contents=types.MarkupContent(kind=types.MarkupKind.Markdown, value=h.contents))


@lru_cache(maxsize=1)
def _load_catalog():
    module_path = os.environ.get("EEQL_CATALOG_MODULE")
    if module_path:
        mod = importlib.import_module(module_path)
        if not hasattr(mod, "build"):
            raise RuntimeError("EEQL_CATALOG_MODULE must expose a build() function")
        return mod.build()
    # fallback to demo
    return demo_build()


class EEQLServer(LanguageServer):
    pass


ls = EEQLServer(SERVER_NAME, SERVER_VERSION)


@ls.feature(types.INITIALIZE)
def on_initialize(ls: EEQLServer, params: types.InitializeParams):
    return None


def _publish(ls: EEQLServer, uri: str, text: str):
    catalog = _load_catalog()
    diags = lsp_core.diagnostics(text, catalog)
    ls.text_document_publish_diagnostics(
        types.PublishDiagnosticsParams(uri=uri, diagnostics=[_ls_diag(d) for d in diags])
    )


@ls.feature(types.TEXT_DOCUMENT_DID_OPEN)
def did_open(ls: EEQLServer, params: types.DidOpenTextDocumentParams):
    doc = ls.workspace.get_text_document(params.text_document.uri)
    _publish(ls, doc.uri, doc.source)


@ls.feature(types.TEXT_DOCUMENT_DID_CHANGE)
def did_change(ls: EEQLServer, params: types.DidChangeTextDocumentParams):
    doc = ls.workspace.get_text_document(params.text_document.uri)
    _publish(ls, doc.uri, doc.source)


# Trigger completions on space/newline/parens/commas so suggestions appear without typing prefixes.
@ls.feature(
    types.TEXT_DOCUMENT_COMPLETION,
    types.CompletionOptions(trigger_characters=[" ", "\n", "(", ","]),
)
def completion(ls: EEQLServer, params: types.CompletionParams):
    doc = ls.workspace.get_text_document(params.text_document.uri)
    pos = params.position
    catalog = _load_catalog()
    items = lsp_core.completions(
        doc.source, lsp_core.Position(pos.line, pos.character), catalog
    )
    return types.CompletionList(is_incomplete=False, items=[_ls_completion(i) for i in items])


@ls.feature(types.TEXT_DOCUMENT_HOVER)
def hover(ls: EEQLServer, params: types.HoverParams):
    doc = ls.workspace.get_text_document(params.text_document.uri)
    pos = params.position
    catalog = _load_catalog()
    h = lsp_core.hover(doc.source, lsp_core.Position(pos.line, pos.character), catalog)
    return None if not h else _ls_hover(h)


def main():
    ls.start_io()


if __name__ == "__main__":
    main()
