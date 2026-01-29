import pytest

from eeql.catalog.interface import InMemoryCatalog
from eeql.core.Event import Event
from eeql.core.Entity import Entity
from eeql.core.Attribute import Attribute
from eeql.vocabulary import attributes as att
from eeql.vocabulary import data_types as dty


def _make_event(name: str, attrs: dict, entities: list[str], default_entity: str):
    entity_objs = [Entity(entity_name=e, entity_id=att.EntityId(event_alias=e), is_default=(e == default_entity)) for e in entities]
    attr_objs = [Attribute(attribute_name=k, data_type=v, event_alias=k) for k, v in attrs.items()]
    event = Event(
        event_name=name,
        event_id=att.EventId(event_alias="event_id"),
        event_timestamp=att.EventTimestamp(event_alias="ts"),
        entities=entity_objs,
        attributes=attr_objs,
        table=name,
    )
    return event


@pytest.fixture
def sample_catalog():
    signup = _make_event(
        "user_signed_up",
        {"user_id": dty.TypeString(), "ts": dty.TypeTimestamp()},
        ["user_id"],
        default_entity="user_id",
    )
    login = _make_event(
        "user_logged_in",
        {"event_id": dty.TypeString(), "ts": dty.TypeTimestamp(), "entity_1": dty.TypeString(), "entity_2": dty.TypeString(), "user_id": dty.TypeString()},
        ["entity_1", "entity_2", "user_id"],
        default_entity="user_id",
    )
    return InMemoryCatalog({"user_signed_up": signup, "user_logged_in": login})
