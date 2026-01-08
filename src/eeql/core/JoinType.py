from eeql.utils import _check_fields
from eeql.vocabulary.attributes import EventPrecededAt, EventRepeatedAt, EventTimestamp
from typing import Optional

from pydantic import BaseModel, model_validator


class JoinType(BaseModel):
    join_type_name: str
    # primary_entity_id: Optional[EntityId] = None
    primary_timestamp: Optional[EventTimestamp] = None
    # joined_entity_id: Optional[EntityId] = None
    joined_timestamp: Optional[EventTimestamp] = None
    primary_preceded_at: Optional[EventPrecededAt] = None
    primary_repeated_at: Optional[EventRepeatedAt] = None


    def join_statement(self, **kwargs) -> str:
        raise NotImplementedError("Must be defined in all subclasses")


    @model_validator(mode="before")
    def check_attribute_fields(cls, values):
        return _check_fields(cls=cls, values=values, properties=["join_type_name"])

    @property
    def _p(self):
        return "p"

    @property
    def _j(self):
        return "j"