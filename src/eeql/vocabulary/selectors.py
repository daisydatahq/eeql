from typing import Optional
from pydantic import (
    Field,
    model_validator
)
from eeql.vocabulary import attributes as att
from eeql.utils import _check_fields
from eeql.vocabulary import data_types as dty
from eeql.core.Selector import Selector


class Last(Selector):
    selector_name: str = Field(default="last")

    def qualify_filter(
        self,
        entity_id: att.EntityId,
        event_id: att.EventId,
        event_timestamp: att.EventTimestamp,
        alias: str = None,
        **kwargs,
    ) -> str:
        if alias:
            alias += "."
        else:
            alias = ""
        return f"qualify lead({alias}{event_timestamp.event_alias}, 1) over (partition by {alias}{entity_id.event_alias} order by {alias}{event_timestamp.event_alias}, {alias}{event_id.event_alias}) is null"

class All(Selector):
    selector_name: str = Field(default="all")

    def qualify_filter(
        self,
        alias: str = None,
        **kwargs
    ) -> str:
        if alias:
            alias += "."
        else:
            alias = ""
        return ""


class Nth(Selector):
    selector_name: str = Field(default="nth")
    n: int

    def qualify_filter(
        self,
        entity_id: att.EntityId,
        event_id: att.EventId,
        event_timestamp: att.EventTimestamp,
        alias: str = None,
        **kwargs
    ) -> str:
        if alias:
            alias += "."
        else:
            alias = ""
        return f"qualify row_number() over (partition by {alias}{entity_id.event_alias} order by {alias}{event_timestamp.event_alias}, {alias}{event_id.event_alias}) = {self.n}"


Nth(n=3).qualify_filter(
    entity_id = att.EntityId(event_alias="test_id"),
    event_id = att.EventId(event_alias="event_id"),
    event_timestamp = att.EventTimestamp(event_alias="event_ts"),
)


class First(Nth):
    selector_name: str = Field(default="first")
    n: int = Field(default=1, frozen=True)

    @model_validator(mode="before")
    def check_n(cls, values):
        return _check_fields(cls=cls, values=values, properties=["n"])



f = First()
# f.n = 3
f.n

f.qualify_filter(
    entity_id = att.EntityId(event_alias="test_id"),
    event_id = att.EventId(event_alias="event_id"),
    event_timestamp = att.EventTimestamp(event_alias="event_ts"),
)

