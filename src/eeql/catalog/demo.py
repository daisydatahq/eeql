from eeql.catalog.interface import InMemoryCatalog
from eeql.core.Event import Event
from eeql.core.Entity import Entity
from eeql.core.Attribute import Attribute
from eeql.vocabulary import attributes as att
from eeql.vocabulary import data_types as dty


def _event(name: str, attrs: dict, entities: list[str], default_entity: str):
    entity_objs = []
    for ent in entities:
        is_default = ent == default_entity
        entity_objs.append(
            Entity(entity_name=ent, entity_id=att.EntityId(event_alias=ent), is_default=is_default)
        )
    attr_objs = [Attribute(attribute_name=k, data_type=v, event_alias=k) for k, v in attrs.items()]
    return Event(
        event_name=name,
        event_id=att.EventId(event_alias="event_id"),
        event_timestamp=att.EventTimestamp(event_alias="ts"),
        entities=entity_objs,
        attributes=attr_objs,
        table=name,
    )


def build():
    signup = _event(
        "user_signed_up",
        {"user_id": dty.TypeString(), "ts": dty.TypeTimestamp()},
        ["user_id"],
        default_entity="user_id",
    )
    login = _event(
        "user_logged_in",
        {"event_id": dty.TypeString(), "ts": dty.TypeTimestamp(), "status": dty.TypeString(), "user_id": dty.TypeString()},
        ["user_id"],
        default_entity="user_id",
    )
    return InMemoryCatalog({"user_signed_up": signup, "user_logged_in": login})
