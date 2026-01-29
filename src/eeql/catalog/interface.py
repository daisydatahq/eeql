from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from eeql.core.Event import Event
from eeql.core.Attribute import Attribute
from eeql.core.Entity import Entity


class Catalog:
    def has_event(self, event_name: str) -> bool: ...
    def get_event(self, event_name: str) -> Optional[Event]: ...
    def get_entity(self, event_name: str, entity_name: str) -> Optional[Entity]: ...
    def get_attribute(self, event_name: str, attribute_name: str) -> Optional[Attribute]: ...
    def default_entity(self, event_name: str) -> Optional[Entity]: ...


class InMemoryCatalog(Catalog):
    """Lightweight catalog backed by Event objects."""

    def __init__(self, events: Dict[str, Event]):
        self._events = events

    def has_event(self, event_name: str) -> bool:
        return event_name in self._events

    def get_event(self, event_name: str) -> Optional[Event]:
        return self._events.get(event_name)

    def get_entity(self, event_name: str, entity_name: str) -> Optional[Entity]:
        event = self._events.get(event_name)
        if not event:
            return None
        return getattr(event.entities, entity_name, None)

    def get_attribute(self, event_name: str, attribute_name: str) -> Optional[Attribute]:
        event = self._events.get(event_name)
        if not event:
            return None
        cols = event.event_columns
        return cols.get(attribute_name)

    def default_entity(self, event_name: str) -> Optional[Entity]:
        event = self._events.get(event_name)
        if not event or not event.entities:
            return None
        for name, ent in event.entities:
            if getattr(ent, "is_default", False):
                return ent
        # fall back to first entity
        first = list(event.entities)[0][1] if event.entities else None
        return first
