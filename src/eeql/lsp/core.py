from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional
import re

from eeql.engine import parser, validator, compiler
from eeql.catalog.interface import Catalog
from eeql.ast import SelectorKind
from eeql.core.Aggregation import AGGREGATION_REGISTRY


@dataclass
class Position:
    line: int
    character: int


@dataclass
class Range:
    start: Position
    end: Position


@dataclass
class Diagnostic:
    message: str
    range: Range
    severity: str = "error"


@dataclass
class CompletionItem:
    label: str
    kind: str = "keyword"
    detail: Optional[str] = None


@dataclass
class Hover:
    contents: str
    range: Optional[Range] = None


KEYWORD_COMPLETIONS = [
    "select",
    "join",
    "first",
    "last",
    "nth",
    "all",
    "before",
    "since",
    "between",
    "after",
    "default_entity",
    "using",
    "filter",
    "additional_join_expressions",
]

SELECTOR_TOKENS = [s.value for s in SelectorKind]
JOIN_DIRS = ["before", "after", "between", "since", "all"]
AGG_FUNCS = list(AGGREGATION_REGISTRY.keys())
_SEP = re.compile(r"[\\s(),]+")


def _span_to_range(span) -> Range:
    return Range(
        start=Position(span.start_line - 1, span.start_col - 1),
        end=Position(span.end_line - 1, span.end_col - 1),
    )


def diagnostics(text: str, catalog: Catalog) -> List[Diagnostic]:
    diags: List[Diagnostic] = []
    try:
        ast = parser.parse_with_spans(text)
        validator.validate_query(ast, catalog)
    except validator.ValidationError as err:
        span = getattr(err, "span", None)
        rng = _span_to_range(span) if span else Range(Position(0, 0), Position(0, 1))
        diags.append(Diagnostic(message=str(err), range=rng))
    except compiler.CompileError as err:
        span = getattr(err, "span", None)
        rng = _span_to_range(span) if span else Range(Position(0, 0), Position(0, 1))
        diags.append(Diagnostic(message=str(err), range=rng))
    except Exception as err:  # syntax errors etc.
        diags.append(Diagnostic(message=str(err), range=Range(Position(0, 0), Position(0, 1))))
    return diags


def _current_prefix(text: str, position: Position) -> str:
    lines = text.splitlines()
    if position.line >= len(lines):
        return ""
    line = lines[position.line]
    col = min(position.character, len(line))
    prefix = re.findall(r"[A-Za-z_][A-Za-z0-9_]*$", line[:col])
    return prefix[0] if prefix else ""


def _text_before_cursor(text: str, position: Position) -> str:
    lines = text.splitlines()
    if not lines:
        return ""
    head = lines[: position.line]
    head.append(lines[position.line][: position.character])
    return "\n".join(head)


def _tokens_before_cursor(text: str, position: Position) -> List[str]:
    before = _text_before_cursor(text, position)
    return [t for t in _SEP.split(before) if t]


def _prev_token(tokens: List[str], n: int = 1) -> Optional[str]:
    return tokens[-n] if len(tokens) >= n else None


def _primary_select_event(tokens: List[str]) -> Optional[str]:
    """Heuristic: first token after 'select' that is not a selector."""
    for i, tok in enumerate(tokens):
        if tok.lower() == "select":
            for t in tokens[i + 1 :]:
                if t.lower() in SELECTOR_TOKENS:
                    continue
                return t
    return None


def completions(text: str, position: Position, catalog: Catalog) -> List[CompletionItem]:
    prefix = _current_prefix(text, position).lower()
    tokens = _tokens_before_cursor(text, position)
    prev = (_prev_token(tokens) or "").lower()
    prev2 = (_prev_token(tokens, 2) or "").lower()
    suggestions: List[CompletionItem] = []

    events = getattr(catalog, "_events", {})
    event_names = list(events.keys())

    def add(labels, kind: str):
        for lbl in labels:
            if prefix and not lbl.lower().startswith(prefix):
                continue
            suggestions.append(CompletionItem(label=lbl, kind=kind, detail=kind))

    def all_attrs():
        attrs = set()
        for ev in events.values():
            attrs.update(ev.event_columns.keys())
        return sorted(attrs)

    # Rule 1: start of file/line
    if (position.line == 0 and position.character == 0) or not tokens:
        add(["select"], "keyword")
        return suggestions

    # Rule 2: after 'select'
    if prev == "select":
        add(SELECTOR_TOKENS, "selector")
        add(event_names, "event")
        return suggestions

    # Rule 3: after selector/join dir
    if prev in SELECTOR_TOKENS or prev in JOIN_DIRS:
        add(event_names, "event")
        return suggestions

    # Rule 4: after 'join'
    if prev == "join":
        add(JOIN_DIRS, "join type")
        add(event_names, "event")
        return suggestions

    # Rule 5: inside first select block (after '(' or ',') before any join
    if prev in ("(", ",") and "join" not in [t.lower() for t in tokens]:
        ev_name = _primary_select_event(tokens)
        attrs = events.get(ev_name).event_columns.keys() if ev_name and ev_name in events else []
        add(attrs, "attribute")
        return suggestions

    # Rule 6: after 'using'
    if prev == "using":
        add(all_attrs(), "attribute")
        return suggestions

    # Rule 7: after 'filter(' or token starting with filter
    if prev.startswith("filter") or prev2 == "filter":
        add(AGG_FUNCS, "aggregation")
        add(all_attrs(), "attribute")
        return suggestions

    # Fallback
    add(KEYWORD_COMPLETIONS, "keyword")
    add(event_names, "event")
    return suggestions


def hover(text: str, position: Position, catalog: Catalog) -> Optional[Hover]:
    ident = _current_prefix(text, position)
    if not ident:
        return None
    ev = catalog.get_event(ident)
    if ev:
        return Hover(contents=f"event {ev.event_name}")
    # check entities on any event
    for e in getattr(catalog, "_events", {}).values() if hasattr(catalog, "_events") else []:
        if hasattr(e, "entities") and hasattr(e.entities, ident):
            return Hover(contents=f"entity {ident} on {e.event_name}")
        if ident in e.event_columns:
            attr = e.event_columns[ident]
            return Hover(contents=f"column {ident}: {attr.data_type}")
    return None
