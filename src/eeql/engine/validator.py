from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Set, Tuple

from eeql.ast import (
    Query,
    SelectClause,
    JoinClause,
    ColumnExpr,
    SelectorKind,
    JoinQualifier,
)
from eeql.catalog.interface import Catalog
from eeql.core.Event import Event


class ValidationError(Exception):
    def __init__(self, message: str, span=None):
        super().__init__(message)
        self.message = message
        self.span = span

    def __str__(self):
        return self.message


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_AGG_CALL_RE = re.compile(r"(?i)^(?P<fn>[a-zA-Z_][\w]*)\s*\(\s*(?P<arg>[a-zA-Z_][\w]*)\s*\)$")


def _extract_identifier(expr: str) -> Optional[str]:
    if _IDENT_RE.match(expr.strip()):
        return expr.strip()
    m = _AGG_CALL_RE.match(expr.strip())
    if m:
        return m.group("arg")
    return None


def validate_query(ast: Query, catalog: Catalog) -> Query:
    _validate_select(ast.select, catalog)
    _validate_joins(ast, catalog)
    _validate_alias_uniqueness(ast)
    _validate_join_uniqueness(ast)
    return ast


def _require_event(catalog: Catalog, event_name: str, span):
    if not catalog.has_event(event_name):
        raise ValidationError(f"Unknown event '{event_name}'", span)
    return catalog.get_event(event_name)


def _validate_select(select: SelectClause, catalog: Catalog):
    event = _require_event(catalog, select.event_name, select.span)
    # select must have an explicit selector
    if select.selector.kind == SelectorKind.OMITTED:
        raise ValidationError("Select clause must specify a selector (first/last/nth/all)", select.span)

        if select.default_entity:
            if not hasattr(event.entities, select.default_entity):
                raise ValidationError(
                    f"default_entity '{select.default_entity}' not on event '{event.event_name}'",
                    select.span,
                )
    else:
        # fallback to first entity name
        select.default_entity = list(event.entities)[0][0]

    _validate_columns(
        select.columns,
        event,
        selector_kind=select.selector.kind,
    )


def _validate_columns(columns: List[ColumnExpr], event: Event, selector_kind):
    for col in columns:
        # Aggregation requirements
        if selector_kind in {SelectorKind.ALL, SelectorKind.OMITTED}:
            if not col.is_aggregated:
                raise ValidationError(
                    f"Selector '{selector_kind.value}' requires aggregated column '{col.alias}'",
                    col.span,
                )
        # Aggregation function validation
        if col.agg_func and not col.is_aggregated:
            raise ValidationError(
                f"Unknown aggregation function '{col.agg_func}'", col.span
            )
        # Column existence (best-effort)
        ident = _extract_identifier(col.expr)
        if ident and ident not in event.event_columns:
            raise ValidationError(
                f"Column '{ident}' not found on event '{event.event_name}'", col.span
            )


def _validate_joins(ast: Query, catalog: Catalog):
    select_default_entity = ast.select.default_entity
    select_event = _require_event(catalog, ast.select.event_name, ast.select.span)

    for join in ast.joins:
        event = _require_event(catalog, join.event_name, join.span)

        if join.using_entities:
            for ent in join.using_entities:
                if not hasattr(event.entities, ent):
                    raise ValidationError(
                        f"Entity '{ent}' not on joined event '{event.event_name}'", join.span
                    )
        else:
            if not select_default_entity:
                raise ValidationError(
                    "Join missing using entities and select has no default_entity",
                    join.span,
                )
            if not hasattr(event.entities, select_default_entity):
                raise ValidationError(
                    f"default_entity '{select_default_entity}' not on joined event '{event.event_name}'",
                    join.span,
                )

        _validate_columns(
            join.columns,
            event,
            selector_kind=join.selector.kind,
        )


def _validate_alias_uniqueness(ast: Query):
    seen: Set[str] = set()

    def check_columns(columns: List[ColumnExpr]):
        for col in columns:
            alias = col.alias
            if not alias:
                raise ValidationError("Column alias is required", col.span)
            if alias in seen:
                raise ValidationError(f"Duplicate alias '{alias}'", col.span)
            seen.add(alias)

    check_columns(ast.select.columns)
    for join in ast.joins:
        check_columns(join.columns)


def _join_signature(join: JoinClause, select_default_entity: Optional[str]) -> Tuple:
    using_entities = tuple(join.using_entities) if join.using_entities else (select_default_entity,)
    filter_text = join.filter.text if join.filter else ""
    additional = tuple(j.text for j in join.additional_join_expressions)
    return (
        join.selector.kind,
        join.qualifier,
        join.event_name,
        using_entities,
        filter_text,
        additional,
    )


def _validate_join_uniqueness(ast: Query):
    signatures = set()
    select_default_entity = ast.select.default_entity
    for join in ast.joins:
        sig = _join_signature(join, select_default_entity)
        if sig in signatures:
            raise ValidationError("Duplicate join signature", join.span)
        signatures.add(sig)
