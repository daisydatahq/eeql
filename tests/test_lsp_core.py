import pytest

from eeql.lsp import core as lsp_core
from eeql.engine import parser, validator
from eeql.catalog.demo import build as demo_build


def test_diagnostics_ok():
    catalog = demo_build()
    text = """
    select first user_signed_up (
      user_id as user_id
    )
    """
    diags = lsp_core.diagnostics(text, catalog)
    assert diags == []


def test_diagnostics_unknown_event():
    catalog = demo_build()
    text = "select first made_up_event ( id as id )"
    diags = lsp_core.diagnostics(text, catalog)
    assert diags
    assert "Unknown event" in diags[0].message


def test_diagnostics_missing_selector_on_select():
    catalog = demo_build()
    text = "select user_signed_up ( user_id as user_id )"
    diags = lsp_core.diagnostics(text, catalog)
    assert diags
    assert "must specify a selector" in diags[0].message


def test_completions_keywords_and_events():
    catalog = demo_build()
    text = "user"
    pos = lsp_core.Position(line=0, character=4)
    items = lsp_core.completions(text, pos, catalog)
    labels = [i.label for i in items]
    # events from demo catalog should show up with prefix 'user'
    assert any("user_signed_up" == l for l in labels)


def test_hover_event():
    catalog = demo_build()
    text = "user_signed_up"
    pos = lsp_core.Position(line=0, character=len("user_signed_up"))
    h = lsp_core.hover(text, pos, catalog)
    assert h is not None
    assert "event user_signed_up" in h.contents
