from typing import Optional, Union, Literal, Dict, Type
from pydantic import (
    Field,
    model_validator
)
from eeql.vocabulary import data_types as dty
from eeql.utils import _check_fields
from eeql.core.Attribute import Attribute


class DefaultAttribute(Attribute):
    attribute_name: str
    data_type: str

    @model_validator(mode="before")
    def check_attribute_fields(cls, values):
        return _check_fields(cls=cls, values=values, properties=["name", "data_type"])


@Attribute.register
class EventId(DefaultAttribute):
    data_type: dty.TypeString = Field(default=dty.TypeString())
    attribute_name: str = Field(default="event_id")

@Attribute.register
class EventTimestamp(DefaultAttribute):
    data_type: dty.TypeTimestamp = Field(default=dty.TypeTimestamp())
    attribute_name: str = Field(default="event_timestamp")

@Attribute.register
class EventIdSource(DefaultAttribute):
    data_type: dty.TypeString = Field(default=dty.TypeString())
    attribute_name: str = Field(default="event_id_source")

@Attribute.register
class EntityId(DefaultAttribute):
    data_type: dty.TypeString = Field(default=dty.TypeString())
    attribute_name: str = Field(default="entity_id")

@Attribute.register
class AnonymousEntityId(DefaultAttribute):
    data_type: dty.TypeString = Field(default=dty.TypeString())
    attribute_name: str = Field(default="anonymous_entity_id")

@Attribute.register
class EntityIdSource(DefaultAttribute):
    data_type: dty.TypeString = Field(default=dty.TypeString())
    attribute_name: str = Field(default="entity_id_source")

@Attribute.register
class EntityIdUpdatedAt(DefaultAttribute):
    data_type: dty.TypeTimestamp = Field(default=dty.TypeTimestamp())
    attribute_name: str = Field(default="entity_id_updated_at")

@Attribute.register
class EventInstance(DefaultAttribute):
    data_type: dty.TypeString = Field(default=dty.TypeInteger())
    attribute_name: str = Field(default="event_instance")

@Attribute.register
class EventRepeatedAt(DefaultAttribute):
    data_type: dty.TypeTimestamp = Field(default=dty.TypeTimestamp())
    attribute_name: str = Field(default="event_repeated_at")

@Attribute.register
class EventPrecededAt(DefaultAttribute):
    data_type: dty.TypeTimestamp = Field(default=dty.TypeTimestamp())
    attribute_name: str = Field(default="event_preceded_at")

@Attribute.register
class EventLoadedAt(DefaultAttribute):
    data_type: dty.TypeTimestamp = Field(default=dty.TypeTimestamp())
    attribute_name: str = Field(default="event_loaded_at")

@Attribute.register
class EventDeletedAt(DefaultAttribute):
    data_type: dty.TypeTimestamp = Field(default=dty.TypeTimestamp())
    attribute_name: str = Field(default="event_deleted_at")

