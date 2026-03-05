import pytest

from eeql.catalog.interface import InMemoryCatalog
from eeql.core.Attribute import Attribute
from eeql.core.DataType import DataType
from eeql.core.Dataset import Dataset
from eeql.core.Entity import Entity
from eeql.core.Event import Event
from eeql.engine import compiler, parser, validator
from eeql.vocabulary import attributes as att
from eeql.vocabulary import data_types as dty
from eeql.vocabulary import selectors as sel


def _make_event(name: str, attrs: dict, entities: list[str]):
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
    setattr(event.entities, entity_objs[0].entity_name, entity_objs[0].model_copy(update={"is_default": True}))
    return event


def _compile_dataset(text: str, catalog: InMemoryCatalog, validate: bool = True) -> Dataset:
    ast = parser.parse_with_spans(text)
    if validate:
        validator.validate_query(ast, catalog)
    return compiler.compile_to_dataset(ast, catalog)


@pytest.fixture
def base_catalog():
    signup = _make_event(
        "user_signed_up",
        {"user_id": dty.TypeString(), "ts": dty.TypeTimestamp()},
        ["user_id"],
    )
    login = _make_event(
        "user_logged_in",
        {
            "event_id": dty.TypeString(),
            "ts": dty.TypeTimestamp(),
            "status": dty.TypeString(),
            "user_id": dty.TypeString(),
            "score": dty.TypeInteger(),
        },
        ["user_id"],
    )
    return InMemoryCatalog({"user_signed_up": signup, "user_logged_in": login})


def test_to_ibis_table_builds_schema_from_base_and_joined(base_catalog):
    text = """
    select first user_signed_up (
      user_id as user_id,
      ts as signup_ts
    )
    join after user_logged_in (
      using user_id
      count(event_id) as login_count,
      last_value(ts) as last_login_ts
    )
    """
    ds = _compile_dataset(text, base_catalog, validate=False)
    table = ds.to_ibis_table()
    schema = table.schema()

    assert set(schema.names) == {"user_id", "signup_ts", "login_count", "last_login_ts"}
    assert str(schema["user_id"]) == "string"
    assert str(schema["login_count"]).startswith("int")
    assert str(schema["last_login_ts"]).startswith("timestamp")


def test_to_ibis_table_uses_aggregation_output_type(base_catalog):
    text = """
    select first user_signed_up (
      user_id as user_id
    )
    join after user_logged_in (
      using user_id
      average(score) as avg_score
    )
    """
    ds = _compile_dataset(text, base_catalog, validate=False)
    table = ds.to_ibis_table()

    assert str(table.schema()["avg_score"]).startswith("float")


def test_to_ibis_table_requires_base_event():
    ds = Dataset(dataset_name="empty_dataset")
    with pytest.raises(ValueError, match="Base Event"):
        ds.to_ibis_table()


def test_to_ibis_table_duplicate_alias_raises(base_catalog):
    text = """
    select first user_signed_up (
      user_id as shared_alias
    )
    join after user_logged_in (
      using user_id
      count(event_id) as shared_alias
    )
    """
    ds = _compile_dataset(text, base_catalog, validate=False)
    with pytest.raises(ValueError, match="Duplicate dataset column alias"):
        ds.to_ibis_table()


def test_to_ibis_table_unknown_datatype_raises():
    class TypeUnsupported(DataType):
        data_type_name: str = "unsupported"
        default_sql: str = "unsupported"

    event = _make_event("test_event", {"x": TypeUnsupported()}, ["user_id"])
    ds = Dataset(dataset_name="unknown_type_dataset")
    ds.select(
        event=event,
        default_entity="user_id",
        selector=sel.First(),
        columns={"x_alias": event.attributes.x},
    )

    with pytest.raises(ValueError, match="Unsupported data type"):
        ds.to_ibis_table()
