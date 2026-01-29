import pytest

from eeql.engine import parser, validator, compiler
from eeql.catalog.interface import InMemoryCatalog
from eeql.core.Event import Event
from eeql.core.Entity import Entity
from eeql.core.Attribute import Attribute
from eeql.vocabulary import attributes as att
from eeql.vocabulary import data_types as dty


def make_event(name: str, attrs: dict, entities: list[str]):
    entity_objs = [Entity(entity_name=e, entity_id=att.EntityId(event_alias=e)) for e in entities]
    attr_objs = [Attribute(attribute_name=k, data_type=v, event_alias=k) for k, v in attrs.items()]
    event = Event(
        event_name=name,
        event_id=att.EventId(event_alias="event_id"),
        event_timestamp=att.EventTimestamp(event_alias="ts"),
        entities=entity_objs,
        attributes=attr_objs,
        table=name,
    )
    # mark first entity default for simplicity
    setattr(event.entities, entity_objs[0].entity_name, entity_objs[0].model_copy(update={"is_default": True}))
    return event


@pytest.fixture
def catalog():
    signup = make_event(
        "user_signed_up",
        {"user_id": dty.TypeString(), "ts": dty.TypeTimestamp()},
        ["user_id"],
    )
    login = make_event(
        "user_logged_in",
        {"event_id": dty.TypeString(), "ts": dty.TypeTimestamp(), "status": dty.TypeString(), "user_id": dty.TypeString()},
        ["user_id"],
    )
    return InMemoryCatalog({"user_signed_up": signup, "user_logged_in": login})


def test_compile_option_b_sql(catalog):
    text = """
    select first user_signed_up (
      user_id as user_id,
      ts as signup_ts
    )
    join after user_logged_in (
      using user_id
      count(event_id) as login_count,
      last_value(ts) as last_login_ts,
      filter(status = 'ok')
    )
    """
    ast = parser.parse_with_spans(text)
    validator.validate_query(ast, catalog)
    ds = compiler.compile_to_dataset(ast, catalog)

    assert ds.base_event is not None
    assert len(ds.base_event.columns.model_fields) == 2
    assert ds.joined_events is not None
    # ensure SQL renders
    sql = ds.to_sql()
    assert "user_signed_up" in sql
    assert "user_logged_in" in sql
