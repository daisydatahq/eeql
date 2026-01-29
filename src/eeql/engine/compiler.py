from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from eeql.ast import (
    Query,
    SelectorKind,
)
from eeql.catalog.interface import Catalog
from eeql.core.Dataset import Dataset
from eeql.core.Event import Event
from eeql.core.Entity import Entity
from eeql.core.Attribute import Attribute
from eeql.core.Filter import Filter
from eeql.core.Column import JoinedDatasetColumn
from eeql.core.JoinType import JoinType
from eeql.vocabulary import selectors as vocab_selectors
from eeql.vocabulary import join_types as vocab_join_types
from eeql.vocabulary import aggregations as vocab_aggs


class CompileError(Exception):
    def __init__(self, message: str, span=None):
        super().__init__(message)
        self.span = span


_CALL_RE = re.compile(r"^\s*(?P<fn>[A-Za-z_][\w]*)\s*\(\s*(?P<arg>[A-Za-z_][\w]*)\s*\)\s*$")


def _parse_col_expr(expr: str) -> Tuple[Optional[str], str]:
    """Return (fn, arg) if call, else (None, identifier)."""
    m = _CALL_RE.match(expr)
    if m:
        return m.group("fn").lower(), m.group("arg")
    return None, expr.strip()


def _selector_to_obj(kind: SelectorKind, n: Optional[int]) -> vocab_selectors.Selector:
    if kind == SelectorKind.FIRST:
        return vocab_selectors.First()
    if kind == SelectorKind.LAST:
        return vocab_selectors.Last()
    if kind == SelectorKind.NTH:
        return vocab_selectors.Nth(n=n)
    if kind == SelectorKind.ALL:
        return vocab_selectors.All()
    # omitted should not reach select; joins may use All semantics for aggregation-required
    return vocab_selectors.All()


def _join_type_from_qualifier(qual: str) -> JoinType:
    mapping = {
        "before": vocab_join_types.Before(),
        "since": vocab_join_types.Since(),
        "between": vocab_join_types.Between(),
        "after": vocab_join_types.After(),
        "all": vocab_join_types.All(),
    }
    jt = mapping.get(qual)
    if not jt:
        raise CompileError(f"Unsupported join qualifier '{qual}'")
    return jt


def _agg_from_selector(selector_kind: SelectorKind, n: Optional[int], attr: Attribute, timestamp_attr):
    if selector_kind == SelectorKind.FIRST:
        return vocab_aggs.first_value(attribute=attr, timestamp=timestamp_attr)
    if selector_kind == SelectorKind.LAST:
        return vocab_aggs.last_value(attribute=attr, timestamp=timestamp_attr)
    if selector_kind == SelectorKind.NTH:
        return vocab_aggs.nth_value(attribute=attr, n=n or 1, timestamp=timestamp_attr)
    return None


def _agg_from_call(fn: str):
    registry = {
        "first_value": ("ts", vocab_aggs.first_value),
        "last_value": ("ts", vocab_aggs.last_value),
        "nth_value": ("tsn", vocab_aggs.nth_value),
        "sum": ("attr", vocab_aggs.sum),
        "median": ("attr", vocab_aggs.median),
        "average": ("attr", vocab_aggs.average),
        "count": ("attr", vocab_aggs.count),
        "count_distinct": ("attr", vocab_aggs.count_distinct),
        "percentile": ("attrn", vocab_aggs.percentile),
        "not_null": ("attr", vocab_aggs.not_null),
        "is_null": ("attr", vocab_aggs.is_null),
    }
    return registry.get(fn)


def compile_to_dataset(ast: Query, catalog: Catalog) -> Dataset:
    ds = Dataset(dataset_name="compiled_eeql")

    # Base event
    select = ast.select
    event = _require_event(catalog, select.event_name, select.span)
    selector_obj = _selector_to_obj(select.selector.kind, select.selector.n)
    default_entity = _resolve_entity(catalog, event, select.default_entity, select.span)
    base_columns = _build_base_columns(event, select.columns, select.span, catalog)
    base_filters = _build_filters(event, select.filter)
    ds.select(
        event=event,
        default_entity=default_entity,
        selector=selector_obj,
        columns=base_columns,
        filters=base_filters,
    )

    # Joins
    for join in ast.joins:
        join_event = _require_event(catalog, join.event_name, join.span)
        join_type = _join_type_from_qualifier(join.qualifier.value)
        entities = _resolve_join_entities(catalog, join_event, join.using_entities, select.default_entity, join.span)
        join_filters = _build_filters(join_event, join.filter)
        join_columns = _build_join_columns(join_event, join.columns, join.selector, catalog, join.span)

        ds.join(
            event=join_event,
            join_type=join_type,
            columns=join_columns,
            entity=entities,
            filters=join_filters,
            additional_join_expressions=[aje.text for aje in join.additional_join_expressions] or None,
        )

    return ds


def _require_event(catalog: Catalog, name: str, span):
    event = catalog.get_event(name)
    if not event:
        raise CompileError(f"Unknown event '{name}'", span)
    return event


def _resolve_entity(catalog: Catalog, event: Event, entity_name: Optional[str], span):
    if entity_name:
        ent = catalog.get_entity(event.event_name, entity_name)
        if not ent:
            raise CompileError(f"Entity '{entity_name}' not found on event '{event.event_name}'", span)
        return ent
    # fallback to first entity
    return list(event.entities)[0][1]


def _resolve_join_entities(catalog: Catalog, event: Event, using_entities: List[str], fallback_entity: Optional[str], span):
    entities: List[Entity] = []
    if using_entities:
        for ent_name in using_entities:
            ent = catalog.get_entity(event.event_name, ent_name)
            if not ent:
                raise CompileError(f"Entity '{ent_name}' not found on event '{event.event_name}'", span)
            entities.append(ent)
    else:
        if not fallback_entity:
            raise CompileError("Join missing using entity and no default_entity on select", span)
        ent = catalog.get_entity(event.event_name, fallback_entity)
        if not ent:
            raise CompileError(f"default_entity '{fallback_entity}' not on joined event '{event.event_name}'", span)
        entities.append(ent)
    return entities


def _build_base_columns(event: Event, columns_ast, span, catalog: Catalog) -> Dict[str, Attribute]:
    cols: Dict[str, Attribute] = {}
    for col in columns_ast:
        fn, arg = _parse_col_expr(col.expr)
        if fn:
            raise CompileError("Aggregations are not allowed in select clause", col.span)
        attr = catalog.get_attribute(event.event_name, arg)
        if not attr:
            raise CompileError(f"Column '{arg}' not found on event '{event.event_name}'", col.span)
        cols[col.alias] = attr
    return cols


def _build_join_columns(event: Event, columns_ast, selector, catalog: Catalog, span) -> defaultdict:
    cols = defaultdict(JoinedDatasetColumn)
    timestamp_attr = event.event_timestamp
    selector_kind = selector.kind
    for col in columns_ast:
        fn, arg = _parse_col_expr(col.expr)
        attr = catalog.get_attribute(event.event_name, arg if not fn else arg)
        if not attr:
            raise CompileError(f"Column '{arg}' not found on event '{event.event_name}'", col.span)

        agg_col = None
        if selector_kind in {SelectorKind.FIRST, SelectorKind.LAST, SelectorKind.NTH}:
            agg = _agg_from_selector(selector_kind, selector.n, attr, timestamp_attr)
        else:
            if not fn:
                raise CompileError("Join columns must be aggregated when selector is all/omitted", col.span)
            agg_entry = _agg_from_call(fn)
            if not agg_entry:
                raise CompileError(f"Unsupported aggregation function '{fn}'", col.span)
            kind, builder = agg_entry
            if kind == "ts":
                agg = builder(attribute=attr, timestamp=timestamp_attr)
            elif kind == "tsn":
                n = selector.n or 1
                agg = builder(attribute=attr, n=n, timestamp=timestamp_attr)
            elif kind == "attrn":
                agg = builder(attribute=attr, n=50)
            else:
                agg = builder(attribute=attr)
        agg_col = agg

        if not isinstance(agg_col, JoinedDatasetColumn):
            # agg builders in vocab return JoinedDatasetColumn via decorator
            raise CompileError("Aggregation builder did not produce a JoinedDatasetColumn", col.span)

        cols[col.alias] = agg_col
    return cols


def _build_filters(event: Event, filter_ast) -> Optional[List[Filter]]:
    if not filter_ast:
        return None
    text = filter_ast.text.strip()
    if not text:
        return None
    m = re.match(r"^\s*([A-Za-z_][\w]*)\s*(.*)$", text)
    if not m:
        return None
    attr_name, expr = m.group(1), m.group(2)
    attr = event.event_columns.get(attr_name)
    if not attr:
        return None
    expr = expr.strip()
    return [Filter(attribute=attr, expression=expr)]
