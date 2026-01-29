from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import List, Optional

from lark import Lark, Transformer, Token, v_args

from eeql.ast import (
    Query,
    SelectClause,
    JoinClause,
    ColumnExpr,
    FilterExpr,
    AdditionalJoinExpression,
    OccurrenceSelector,
    SelectorKind,
    JoinQualifier,
    Span,
)


_AGG_FN_PATTERN = re.compile(r"(?i)^(?P<fn>[a-zA-Z_][\w]*)\s*\(")

# keep in sync with vocabulary/aggregations.py
_AGG_FN_ALLOWED = {
    "first_value",
    "last_value",
    "not_null",
    "is_null",
    "sum",
    "median",
    "average",
    "count",
    "count_distinct",
    "percentile",
}


def _span(meta) -> Optional[Span]:
    if meta and hasattr(meta, "line") and meta.line is not None and meta.column is not None:
        return Span(
            start_line=meta.line,
            start_col=meta.column,
            end_line=meta.end_line,
            end_col=meta.end_column,
        )
    return None


@v_args(meta=True)
class _ASTBuilder(Transformer):
    def __init__(self):
        super().__init__(visit_tokens=True)

    @staticmethod
    def _split(meta, children):
        # Lark with v_args(meta=True) may pass (children, meta) or (meta, children)
        if children is None:
            # meta is actually children list, no meta
            return meta, None
        if isinstance(meta, list) and not hasattr(meta, "line"):
            return meta, children
        if isinstance(children, list):
            return children, meta
        return [meta], children

    # tokens → python primitives -------------------------------------------------
    def EVENT_NAME(self, token: Token):
        return str(token)

    def ENTITY_NAME(self, token: Token):
        return str(token)

    def ALIAS(self, token: Token):
        return str(token)

    def alias(self, meta, children=None):
        children, _meta = self._split(meta, children)
        return str(children[-1])

    # selectors ------------------------------------------------------------------
    def first(self, meta, children=None):
        _, m = self._split(meta, children)
        return OccurrenceSelector(kind=SelectorKind.FIRST, span=_span(meta))

    def last(self, meta, children=None):
        return OccurrenceSelector(kind=SelectorKind.LAST, span=_span(meta))

    def all(self, meta, children=None):
        return OccurrenceSelector(kind=SelectorKind.ALL, span=_span(meta))

    def nth(self, meta, children=None):
        children, _ = self._split(meta, children)
        n_token = children[0]
        return OccurrenceSelector(
            kind=SelectorKind.NTH, n=int(n_token), span=_span(meta)
        )

    # qualifiers -----------------------------------------------------------------
    def JOIN_QUALIFIER(self, token: Token):
        return str(token)

    def join_qualifier(self, meta, children=None):
        children, _ = self._split(meta, children)
        value = str(children[0])
        return JoinQualifier(value)

    # block items ----------------------------------------------------------------
    def default_entity(self, meta, children=None):
        children, _ = self._split(meta, children)
        return ("default_entity", str(children[0]), _span(meta))

    def using_entities(self, meta, children=None):
        children, _ = self._split(meta, children)
        entities = [str(i) for i in children]
        return ("using", entities, _span(meta))

    def filter_expr(self, meta, children=None):
        children, _ = self._split(meta, children)
        text = str(children[0]).strip()
        return FilterExpr(text=text, span=_span(meta))

    def additional_join_expr(self, meta, children=None):
        children, _ = self._split(meta, children)
        raw = children[0]
        text = raw[1:-1] if isinstance(raw, str) else str(raw)
        return AdditionalJoinExpression(text=text, span=_span(meta))

    def column_expr(self, meta, children=None):
        children, _ = self._split(meta, children)
        raw_expr = str(children[0]).strip()
        alias = str(children[1]).strip()
        agg_func = None
        is_agg = False
        match = _AGG_FN_PATTERN.match(raw_expr)
        if match:
            agg_func = match.group("fn").lower()
            is_agg = agg_func in _AGG_FN_ALLOWED
        return ColumnExpr(
            expr=raw_expr,
            alias=alias,
            is_aggregated=is_agg,
            agg_func=agg_func,
            span=_span(meta),
        )

    # clauses --------------------------------------------------------------------
    def select_clause(self, meta, children=None):
        children, _ = self._split(meta, children)
        selector = None
        event_name = None
        block_items = []
        for item in children:
            if isinstance(item, OccurrenceSelector):
                selector = item
            elif isinstance(item, str) and event_name is None:
                event_name = item
            elif hasattr(item, "children"):
                for child in item.children:
                    if hasattr(child, "children"):
                        block_items.extend(child.children)
                    else:
                        block_items.append(child)
            else:
                block_items.append(item)

        default_entity = None
        filter_expr = None
        columns: List[ColumnExpr] = []

        for bi in block_items:
            if isinstance(bi, tuple) and bi[0] == "default_entity":
                default_entity = bi[1]
            elif isinstance(bi, FilterExpr):
                filter_expr = bi
            elif isinstance(bi, ColumnExpr):
                columns.append(bi)
            # select clause ignores using/additional join expressions

        if selector is None:
            selector = OccurrenceSelector(kind=SelectorKind.OMITTED, span=_span(meta))

        return SelectClause(
            event_name=event_name,
            columns=columns,
            selector=selector,
            default_entity=default_entity,
            filter=filter_expr,
            span=_span(meta),
        )

    def join_clause(self, meta, children=None):
        children, _ = self._split(meta, children)
        selector = None
        qualifier = None
        event_name = None
        block_items = []
        for item in children:
            if isinstance(item, OccurrenceSelector):
                selector = item
            elif isinstance(item, JoinQualifier):
                qualifier = item
            elif isinstance(item, str) and event_name is None:
                event_name = item
            elif hasattr(item, "children"):
                for child in item.children:
                    if hasattr(child, "children"):
                        block_items.extend(child.children)
                    else:
                        block_items.append(child)
            else:
                block_items.append(item)

        using_entities: List[str] = []
        filter_expr = None
        columns: List[ColumnExpr] = []
        additional_join_expressions: List[AdditionalJoinExpression] = []

        for bi in block_items:
            if isinstance(bi, tuple) and bi[0] == "using":
                using_entities = bi[1]
            elif isinstance(bi, FilterExpr):
                filter_expr = bi
            elif isinstance(bi, AdditionalJoinExpression):
                additional_join_expressions.append(bi)
            elif isinstance(bi, ColumnExpr):
                columns.append(bi)

        if selector is None:
            selector = OccurrenceSelector(kind=SelectorKind.OMITTED, span=_span(meta))

        return JoinClause(
            event_name=event_name,
            qualifier=qualifier,
            columns=columns,
            selector=selector,
            using_entities=using_entities,
            filter=filter_expr,
            additional_join_expressions=additional_join_expressions,
            span=_span(meta),
        )

    def query(self, meta, children=None):
        children, _ = self._split(meta, children)
        select_clause: SelectClause = children[0]
        joins: List[JoinClause] = children[1:]
        return Query(select=select_clause, joins=joins)


@lru_cache(maxsize=1)
def _build_parser() -> Lark:
    grammar_path = Path(__file__).resolve().parents[1] / "grammar" / "eeql.lark"
    grammar_text = grammar_path.read_text()
    return Lark(
        grammar_text,
        parser="lalr",
        propagate_positions=True,
        maybe_placeholders=False,
    )


def parse(text: str) -> Query:
    """Parse EEQL text into an AST without spans."""
    tree = _build_parser().parse(text)
    return _ASTBuilder().transform(tree)


def parse_with_spans(text: str) -> Query:
    """Parse EEQL text into an AST retaining spans for diagnostics."""
    tree = _build_parser().parse(text)
    return _ASTBuilder().transform(tree)
