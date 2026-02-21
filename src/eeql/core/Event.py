from typing import Optional, Union, List, DefaultDict, Any
from collections import defaultdict
import os
from pydantic import (
    BaseModel,
    create_model,
    Field,
    model_validator,
    field_validator,
    FilePath,
    computed_field,
    ConfigDict,
)
from eeql.utils import _create_group
from eeql.core.Attribute import Attribute, ATTRIBUTE_REGISTRY
from eeql.core.Entity import Entity
from eeql.vocabulary import attributes as att
from eeql.core.DataType import DATA_TYPE_REGISTRY
from eeql.core.sql_metadata_adapter import get_columns_from_sql
import yaml


class Event(BaseModel):
    event_name: str
    event_id: att.EventId
    event_timestamp: att.EventTimestamp
    entities: BaseModel
    table: Optional[str] = None
    attributes: Optional[Union[List[Attribute], Attribute, BaseModel]] = None
    sql: Optional[str] = None
    sql_path: Optional[FilePath] = None
    event_id_source: Optional[att.EventIdSource] = None
    # event_name: Optional[EventName] = None
    event_loaded_at: Optional[att.EventLoadedAt] = None
    event_deleted_at: Optional[att.EventDeletedAt] = None
    project_entities: Optional[List[str]] = Field(default=None)

    model_config = ConfigDict(extra="allow")


    @computed_field
    @property
    def materialized(self) -> bool:
        return self.table is not None

    @computed_field
    @property
    def event_columns(self) -> dict[str, Attribute]:
        # Collect all event columns keyed by their column name/alias
        columns: dict[str, Attribute] = dict()
        if self.attributes:
            columns.update({name: attr for name, attr in self.attributes})
        columns[self.event_id.event_column] = self.event_id
        columns[self.event_timestamp.event_column] = self.event_timestamp
        for _, entity in self.entities:
            columns[entity.entity_id.event_column] = entity.entity_id
        return columns


    @field_validator(
        "event_id",
        "event_timestamp",
        "event_id_source",
        "event_loaded_at",
        "event_deleted_at",
        mode="before",
    )
    def create_default_attribute(cls, v, info):
        # validation helper to enable users to simply pass the default column alias as a string to the argument
        if isinstance(v, str):
            field_map = dict(
                event_id=att.EventId(),
                event_timestamp=att.EventTimestamp(),
                event_id_source=att.EventIdSource(),
                # event_name=att.EventName(),
                event_loaded_at=att.EventLoadedAt(),
                event_deleted_at=att.EventDeletedAt(),
            )
            field = info.field_name
            alias = v
            v = field_map[field]
            v.event_alias = alias
        return v


    @model_validator(mode="before")
    def validate_entities(cls, values):
        # validation helper to check event's registered entities against project's registered entities
        project_entities = values.get("project_entities", None)
        if project_entities:
            registered_entities = values["entity_id_aliases"].keys()
            for entity in registered_entities:
                if entity not in project_entities:
                    event_name = values["event_name"]
                    raise ValueError(f"Event `{event_name}` has invalid entity `{entity}`. Valid options are {registered_entities}")
                if not entity.get("entity_id", None):
                    event_name = values["event_name"]
                    raise ValueError(f"Event `{event_name}` does not specify an entity ID alias for entity `{entity}`.")
        return values


    @field_validator("entities", mode="before")
    def set_entities(cls, v, values, **kwargs):
        # validation helper to standardize entities property
        if isinstance(v, Entity):
            ent = v
            EntityModel = create_model("EntityModel", **{v.entity_name: (Entity, None)})
            v = EntityModel(**{ent.entity_name: ent})
        else:
            if isinstance(v, list):
                EntityModel = create_model("EntityModel", **{ent.entity_name: (Entity, None) for ent in v})
                v = EntityModel(**{ent.entity_name: ent for ent in v})
            for ent_name, ent in v:
                if not isinstance(ent, Entity):
                    event_name = values.data.get("event_name")
                    raise ValueError(f"Incorrectly specified entity `{ent_name}` passed to event `{event_name}`. Entity:\n\n{ent}")
        return v


    @field_validator("attributes", mode="before")
    def set_attributes(cls, v, values, **kwargs):
        # validation helper to standardize attributes property
        if isinstance(v, Attribute):
            AttributeModel = create_model("AttributeModel", **{v.attribute_name: (Attribute, None)})
            v = AttributeModel(**{v.attribute_name: v})
        else:
            if isinstance(v, list):
                AttributeModel = create_model("AttributeModel", **{a.attribute_name: (Attribute, None) for a in v})
                v = AttributeModel(**{a.attribute_name: a for a in v})
            for att_name, a in v:
                if not isinstance(a, Attribute):
                    event_name = values.data.get("event_name")
                    raise ValueError(f"Incorrectly specified attribute `{att_name}` passed to event `{event_name}`. att.Attribute:\n\n{a}")
        return v


    @classmethod
    def from_sql(
        cls,
        event_name: str,
        event_id: str, # alias
        event_timestamp: str, # alias
        sql: Union[str, FilePath], # sql filepath or sql query
        conn: Any,
        entities: Union[Entity, List[Entity]],
        # sql_path: Optional[FilePath]=None,
        table: Optional[str] = None,
        attributes: Optional[Union[DefaultDict[str, str], BaseModel]] = Field(default_factory=lambda: defaultdict(str)),
        event_id_source: Optional[str] = None,
        # event_name: Optional[str] = None,
        event_loaded_at: Optional[str] = None,
        event_deleted_at: Optional[str] = None,
        # default_schema: Optional[Union[ObjectSet, dict]]=None,
        # interactive: Optional[bool]=True,
        **kwargs,
    ):
        
        # get sql
        if sql[-4:] == ".sql":
            sql_path = os.path.abspath(os.path.expanduser(sql))
            file_exists = os.path.exists(sql_path)
            if not file_exists:
                raise ValueError(f"Invalid path provided by {sql}. Inferred full path: {sql_path}")
            with open(sql_path, "r") as f:
                sql = f.read()
                f.close()
        
        # get column information
        columns = get_columns_from_sql(conn=conn, sql=sql)
        # print(columns)


        # column checks
        defaults = dict(
            event_id=event_id,
            event_timestamp=event_timestamp,
        )
        if event_id_source:
            defaults["event_id_source"] = event_id_source
        # if event_name:
        #     defaults["event_name"] = event_name
        if event_loaded_at:
            defaults["event_loaded_at"] = event_loaded_at
        if event_deleted_at:
            defaults["event_deleted_at"] = event_deleted_at
        
        if isinstance(entities, Entity):
            entities = list(entities)
        for entity in entities:
            defaults[f"entity.{entity.entity_name}.entity_id"] = entity.entity_id.event_alias

        # TODO: handle case where event id column is also an entity id/other default column
        for default_name, default_alias in defaults.items():
            if default_alias in columns.keys():
                columns.pop(default_alias)
            else:
                raise ValueError(f"Default column `{default_name}` with configured alias `{default_alias}` not found in query")
        
        attributes = [Attribute(attribute_name=key, data_type=item["data_type"]) for key, item in columns.items()]
        
        return cls(
            event_name=event_name,
            entities=entities,
            attributes=attributes,
            sql=sql,
            event_id=event_id,
            event_timestamp=event_timestamp,
            event_id_source=event_id_source,
            event_loaded_at=event_loaded_at,
            event_deleted_at=event_deleted_at,
            table=table,
            **kwargs
        )
