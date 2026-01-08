from eeql.utils import _check_fields
from eeql.vocabulary import attributes as att
from typing import Optional
from pydantic import BaseModel, Field, model_validator


class Selector(BaseModel):
    selector_name: str
    entity_id: Optional[att.EntityId] = Field(default=None)
    event_id: Optional[att.EventId] = Field(default=None)
    event_timestamp: Optional[att.EventTimestamp] = Field(default=None)

    @model_validator(mode="before")
    def check_attribute_fields(cls, values):
        return _check_fields(cls=cls, values=values, properties=["selector"])

    def qualify_filter(self):
        raise NotImplementedError("Method must be explicitly set by each subclass.")