from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


@dataclass
class Span:
    """Source-code span for diagnostics (1-based, inclusive start, exclusive end)."""

    start_line: int
    start_col: int
    end_line: int
    end_col: int


class SelectorKind(str, Enum):
    FIRST = "first"
    LAST = "last"
    ALL = "all"
    NTH = "nth"
    OMITTED = "omitted"  # explicit representation of no selector


@dataclass
class OccurrenceSelector:
    kind: SelectorKind
    n: Optional[int] = None
    span: Optional[Span] = None

    @property
    def is_aggregating_required(self) -> bool:
        return self.kind in {SelectorKind.ALL, SelectorKind.OMITTED}


class JoinQualifier(str, Enum):
    BEFORE = "before"
    SINCE = "since"
    BETWEEN = "between"
    AFTER = "after"
    ALL = "all"


@dataclass
class FilterExpr:
    text: str
    span: Optional[Span] = None


@dataclass
class AdditionalJoinExpression:
    text: str
    span: Optional[Span] = None


@dataclass
class ColumnExpr:
    expr: str
    alias: str
    is_aggregated: bool
    agg_func: Optional[str] = None
    span: Optional[Span] = None


@dataclass
class SelectClause:
    event_name: str
    columns: List[ColumnExpr]
    selector: Optional[OccurrenceSelector] = None
    default_entity: Optional[str] = None
    filter: Optional[FilterExpr] = None
    span: Optional[Span] = None


@dataclass
class JoinClause:
    event_name: str
    qualifier: JoinQualifier
    columns: List[ColumnExpr]
    selector: Optional[OccurrenceSelector] = None
    using_entities: List[str] = field(default_factory=list)
    filter: Optional[FilterExpr] = None
    additional_join_expressions: List[AdditionalJoinExpression] = field(
        default_factory=list
    )
    span: Optional[Span] = None


@dataclass
class Query:
    select: SelectClause
    joins: List[JoinClause] = field(default_factory=list)
