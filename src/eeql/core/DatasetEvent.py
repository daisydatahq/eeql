from typing import Optional, Literal, List
from pydantic import (
    BaseModel,
    Field,
    model_validator,
    field_validator,
    computed_field,
    create_model
)
from eeql.core import Column as dc
from eeql.core.Event import Event
from eeql.core.Entity import Entity
from eeql.core.Selector import Selector
from eeql.core.JoinType import JoinType
from eeql.core.DatasetColumn import DatasetColumn
from eeql.core.Filter import Filter
from eeql.vocabulary import attributes as att, join_types as jt
import string


class DatasetEventSQL(BaseModel):
    sql: str
    cte_name: str
    dataset_columns: List[str]
    join_statement: str
    from_statement: Optional[str] = Field(default=None)
    event_sql: Optional[str] = Field(default=None) # event sql
    event_cte_name: Optional[str] = Field(default=None) # event sql




class DatasetEvent(BaseModel):
    event: Event
    columns: BaseModel # TODO: add support for passing a DefaultDict[str, str], where the key is the dataset alias and the value is the attribute name
    event_type: Literal["base", "joined"]
    filters: Optional[List[Filter]] = None
    dataset_event_name: Optional[str] = None

    @property
    def _table(self):
        return self.event.table if self.event.materialized else self.event.event_name

    @property
    def _j(self):
        return "j"
    
    @property
    def _p(self):
        return "p"
    
    @property
    def _base(self):
        return "base"
    
    @property
    def _base_event_id(self):
        return "___event_id"
    
    @property
    def _base_event_timestamp(self):
        return "___event_timestamp"


    # TODO: add method for adding a column to an event that's already been added to the dataset
    def add_column(self, column: DatasetColumn):
        pass


    @staticmethod
    def _row_number(entity_id: att.EntityId, event_timestamp: att.EventTimestamp, event_id: att.EventId, alias: str=None):
        if alias:
            alias += "."
        return f"row_number() over (partition by {alias}{entity_id.event_alias} order by {alias}{event_timestamp.event_alias}, {alias}{event_id.event_alias})"

    @staticmethod
    def _preceded_at(entity_id: att.EntityId, event_timestamp: att.EventTimestamp, event_id: att.EventId, alias: str=None):
        if alias:
            alias += "."
        return f"lag({alias}{event_timestamp.event_alias}, 1) over (partition by {alias}{entity_id.event_alias} order by {alias}{event_timestamp.event_alias}, {alias}{event_id.event_alias})"

    @staticmethod
    def _repeated_at(entity_id: att.EntityId, event_timestamp: att.EventTimestamp, event_id: att.EventId, alias: str=None):
        if alias:
            alias += "."
        return f"lead({alias}{event_timestamp.event_alias}, 1) over (partition by {alias}{entity_id.event_alias} order by {alias}{event_timestamp.event_alias}, {alias}{event_id.event_alias})"
    
    def filter_string(self):
        if self.filters:
            if self.event_type == "joined":
                alias = f"{self._j}."
            else:
                alias = ""
            return "and " + "and ".join(f"{alias}{f.attribute.event_column} {f.expression}" for f in self.filters)
        else:
            return "and true"




class BaseEvent(DatasetEvent):
    default_entity: Entity
    selector: Selector
    event_type: str = Field(default="base", init=False)
    roll_up_timestamp: Optional[Literal["date", "week", "month", "quarter", "year"]] = None
    _since_join_entities: Optional[List[str]] = None
    _between_join_entities: Optional[List[str]] = None

    @model_validator(mode="before")
    def validate_entity(cls, values,):
        v = values["default_entity"]
        event = values["event"]
        entities = event.entities
        entity_names = set(list(entities.model_fields.keys()))# + list(getattr(entities, "computed_model_fields", {}).keys()))
        if v.entity_name not in entity_names:
            event_name = event.event_name
            raise ValueError(f"Default entity incorrectly specified for event {event_name}. User passed `{v}`; valid options are {entity_names}")
        default_entity = getattr(event.entities, v.entity_name)
        default_entity.is_default = True
        setattr(event.entities, v.entity_name, default_entity)
        values["event"] = event
        return values
    
    @field_validator("selector", mode="before")
    def enrich_selector(cls, v, values, **kwargs):
        event = values.data["event"]
        event_id = event.event_id
        event_timestamp = event.event_timestamp
        entity_id = getattr(event.entities, values.data["default_entity"].entity_name).entity_id
        v.event_id = event_id
        v.event_timestamp = event_timestamp
        v.entity_id = entity_id
        return v



    def add_column(self, column: dc.BaseDatasetColumn):
        return super().add_column(column)
    
    def event_sql(self) -> str:
        if not self.event.materialized:
            return self.event.sql
        else:
            return ""

    def base_sql(self) -> DatasetEventSQL:
        """Generates the CTE that selects all the columns from the base event needed for aggregating joined events

        Returns:
            str: A sql CTE
        """
        columns = dict()
        event_id = self.event.event_id.event_alias
        columns[self._base_event_id] = event_id
        if self.roll_up_timestamp:
            select_ts = f"date_trunc({self.event.event_timestamp.event_alias}, {self.roll_up_timestamp})"
        columns[self._base_event_timestamp] = self.event.event_timestamp.event_alias
        for entity_name, entity in self.event.entities:
            # entity.ent
            # if entity.join_types:
            #     columns[entity.entity_id.event_alias] = entity.entity_id.event_alias
            if self._since_join_entities:
                if entity.entity_name in self._since_join_entities:
                    if self.filters or not entity.event_preceded_at:
                        columns[f"{entity_name}_preceded_at"] = self._preceded_at(
                            entity_id=entity.entity_id,
                            event_id=self.event.event_id,
                            event_timestamp=self.event.event_timestamp
                        )
                    else:
                        columns[f"{entity_name}_preceded_at"] = entity.event_preceded_at.event_alias
            if self._between_join_entities:
                if entity.entity_name in self._between_join_entities:
                    if self.filters or not entity.event_repeated_at:
                        columns[f"{entity_name}_repeated_at"] = self._repeated_at(
                            entity_id=entity.entity_id,
                            event_id=self.event.event_id,
                            event_timestamp=self.event.event_timestamp
                        )
                    else:
                        columns[f"{entity_name}_repeated_at"] = entity.event_repeated_at.event_alias
        default_entity = getattr(self.event.entities, self.default_entity.entity_name)
        filters = self.filter_string()
        qualify = self.selector.qualify_filter(
            event_id=self.event.event_id,
            event_timestamp=self.event.event_timestamp,
            entity_id=default_entity.entity_id,
        )
        column_string = ",\n".join(f"{value} as {key}" for key, value in columns.items())
        for alias, column in self.columns:
            # add selected columns in case of use in extra joins, but avoid duplicate selection with key base columns
            if alias not in list(columns.keys()):
                column_string += f",\n{column.attribute.event_column} as {alias}"
        # + ",\n" + ",\n".join([f"{column.alias} as {alias}" for alias, column in self.columns])
        event_sql = self.event_sql()
        table = self._table
        base_alias = "base_columns"
        metadata_sql = f"select\n{column_string}\nfrom {table}\nwhere true\n{filters}\n{qualify}"
        final_dataset_columns = [f"{base_alias}.{column.attribute.event_column} as {alias}" for alias, column in self.columns]
        final_join_statement = f"left join {table} {base_alias}\non {self._base}.{self._base_event_id} = {base_alias}.{event_id}"
        final_from_statement = f"from {self._base}"
        if event_sql:
            sql = DatasetEventSQL(
                sql=metadata_sql,
                cte_name=self._base,
                dataset_columns=final_dataset_columns,
                join_statement=final_join_statement,
                from_statement=final_from_statement,
                event_sql=event_sql,
                event_cte_name=table,
            )
        else:
            sql = DatasetEventSQL(
                sql=metadata_sql,
                cte_name=self._base,
                dataset_columns=final_dataset_columns,
                join_statement=final_join_statement,
                from_statement=final_from_statement,
            )
        return sql



class TimeSpine(Event):
    interval: str = Literal["day", "week", "month", "quarter", "year"]

    @field_validator("event_name", mode="before")
    def set_event_name(cls, v, values, **kwargs):
        event = values["event"]
        interval = values["interval"]
        return 

class TimeSpineEvent(BaseEvent):
    default_entity: Entity
    selector: Selector
    event_type: str = Field(default="base", init=False)
    interval: str = Literal["day", "week", "month", "quarter", "year"]


    def base_sql(self) -> DatasetEventSQL:
        """Generates the CTE that selects all the columns from the base event needed for aggregating joined events

        Returns:
            str: A sql CTE
        """
        columns = dict()
        event_id = self.event.event_id.event_alias
        columns[self._base_event_id] = event_id
        columns[self._base_event_timestamp] = self.event.event_timestamp.event_alias
        for entity_name, entity in self.event.entities:
            # entity.ent
            # if entity.join_types:
            #     columns[entity.entity_id.event_alias] = entity.entity_id.event_alias
            if self._since_join_entities:
                if entity_name in self._since_join_entities:
                    if self.filters or not entity.event_preceded_at:
                        columns[f"{entity_name}_preceded_at"] = self._preceded_at(
                            entity_id=entity.entity_id,
                            event_id=self.event.event_id,
                            event_timestamp=self.event.event_timestamp
                        )
                    else:
                        columns[f"{entity_name}_preceded_at"] = entity.event_preceded_at.event_alias
            if self._between_join_entities:
                if entity_name in self._between_join_entities:
                    if self.filters or not entity.event_repeated_at:
                        columns[f"{entity_name}_repeated_at"] = self._repeated_at(
                            entity_id=entity.entity_id,
                            event_id=self.event.event_id,
                            event_timestamp=self.event.event_timestamp
                        )
                    else:
                        columns[f"{entity_name}_repeated_at"] = entity.event_repeated_at.event_alias
        default_entity = getattr(self.event.entities, self.default_entity.entity_name)
        filters = self.filter_string()
        qualify = self.selector.qualify_filter(
            event_id=self.event.event_id,
            event_timestamp=self.event.event_timestamp,
            entity_id=default_entity.entity_id,
        )
        column_string = ",\n".join(f"{value} as {key}" for key, value in columns.items())
        event_sql = self.event_sql()
        table = self._table
        metadata_sql = f"select\n{column_string}\nfrom {table}\nwhere true\n{filters}\n{qualify}"
        self.columns
        final_dataset_columns = [f"{table}.{alias}" for alias, column in self.columns]
        final_join_statement = f"left join {table}\non {self._base}.{self._base_event_id} = {table}.{event_id}"
        final_from_statement = f"from {self._base}"
        if event_sql:
            sql = DatasetEventSQL(
                sql=metadata_sql,
                cte_name=self._base,
                dataset_columns=final_dataset_columns,
                join_statement=final_join_statement,
                from_statement=final_from_statement,
                event_sql=event_sql,
                event_cte_name=table,
            )
        else:
            sql = DatasetEventSQL(
                sql=metadata_sql,
                cte_name=self._base,
                dataset_columns=final_dataset_columns,
                join_statement=final_join_statement,
                from_statement=final_from_statement,
            )
        return sql


class JoinedEvent(DatasetEvent):
    entity: Optional[Entity | List[Entity]]
    join_type: JoinType
    event_type: str = Field(default="joined", init=False)
    additional_join_expressions: Optional[List[str]] = None

    @computed_field
    @property
    def unique_name(self) -> str:
        if self.dataset_event_name:
            return self.dataset_event_name
        else:
            name = f"{self.join_type.join_type_name}__{self.event.event_name}"
            if self.entity:
                entities = self.entity
                if not isinstance(entities, list):
                    entities = [entities]
                for entity in entities:
                    name += f"__{entity.entity_name}"
            if self.filters:
                for filter in self.filters:
                    name += f"__{filter.expression.replace(' ', '_')}"
            if self.additional_join_expressions:
                for expression in self.additional_join_expressions:
                    name += f"__{expression.replace(' ', '_')}"
            translator = str.maketrans(string.punctuation, "_"*len(string.punctuation))
            return name.replace(" ", "_").translate(translator)
    
    @field_validator("columns", mode="before")
    def validate_columns(cls, v, values, **kwargs):
        if isinstance(v, dict):
            for alias, column in v.items():
                if not isinstance(column, dc.JoinedDatasetColumn):
                    raise ValueError(f"Each item passed to columns argument must be a JoinedDatasetColumn class. Passed `{alias}`, which is type {type(column)}")
        DCM = create_model("DCM", **{key: (dc.JoinedDatasetColumn, None) for key in v.keys()})
        v = DCM(**v)
        return v


    def add_column(self, column: dc.JoinedDatasetColumn):
        return super().add_column(column)
    
    def event_sql(self) -> str:
        if not self.event.materialized:
            return self.event.sql
        else:
            return None

    def base_sql(self, base_event: BaseEvent) -> DatasetEventSQL:
        """Generates the CTE that selects all the columns from the base event needed for aggregating joined events

        Returns:
            str: A sql CTE
        """
        column_string = ",\n".join([f"{column.aggregation.aggregation_statement()} as {alias}" for alias, column in self.columns])

        table = self._table
        entity_join_details = dict()
        if self.entity:
            if not isinstance(self.entity, list):
                self.entity = [self.entity]
            for entity in self.entity:
                entity_join_details[entity.entity_name] = {
                    "joined_entity_alias": entity.entity_id.event_alias,
                    "base_entity_alias": getattr(base_event.event.entities, entity.entity_name).entity_id.event_alias
                }
            # else:
            #     joined_entity_alias = self.entity.entity_id.event_alias
            #     base_entity_alias = getattr(base_event.event.entities, self.entity.entity_name).entity_id.event_alias
        else:
            entity_join_details[base_event.default_entity.entity_name] = {
                "joined_entity_alias": getattr(self.event.entities, base_event.default_entity.entity_name).entity_id.event_alias,
                "base_entity_alias": getattr(base_event.event.entities, base_event.default_entity.entity_name).entity_id.event_alias
            }
            # joined_entity_alias = getattr(self.event.entities, base_event.default_entity.entity_name).entity_id.event_alias
            # base_entity_alias = getattr(base_event.event.entities, base_event.default_entity.entity_name).entity_id.event_alias
        base_event_timestamp = base_event.event.event_timestamp.model_copy(deep=True)
        base_event_timestamp.event_alias = self._base_event_timestamp

        join_type_string = self.join_type.join_statement(primary_timestamp=base_event_timestamp, joined_timestamp=self.event.event_timestamp)
        # base_event_id = base_event.event.event_id.event_alias
        if isinstance(self.join_type, jt.All):
            # pkey = base_entity_alias
            pkey: List[str] = [v["base_entity_alias"] for v in entity_join_details.values()]
        else:
            pkey = [self._base_event_id]
        join_sql = f"select\n"
        for k in pkey:
            join_sql += f"{self._p}.{k},"
        join_sql += f"\n{column_string}\n"
        # join_sql += f"{self._p}.{pkey},\n{column_string}\n"
        join_sql += f"from {self._base} {self._p}\nleft join {table} {self._j}\n\ton true\n"
        for v in entity_join_details.values():
            join_sql += f"and {self._p}.{v['base_entity_alias']} = {self._j}.{v['joined_entity_alias']}\n"
        join_sql += f"{join_type_string}\n{self.filter_string()}\n"
        if self.additional_join_expressions:
            join_sql += "and " + "\nand ".join(self.additional_join_expressions) + "\n"
        join_sql += "group by 1\n"


        event_sql = self.event_sql()
        cte_name = self.unique_name
        final_dataset_columns = [f"{cte_name}.{alias}" for alias, column in self.columns]
        final_join_statement = f"left join {cte_name}\non true\n"
        for k in pkey:
            final_join_statement += f"and {self._base}.{k} = {cte_name}.{k}"
        if event_sql:
            sql = DatasetEventSQL(
                sql=join_sql,
                cte_name=cte_name,
                dataset_columns=final_dataset_columns,
                join_statement=final_join_statement,
                event_cte_name=table,
                event_sql=event_sql,
            )
        else:
            sql = DatasetEventSQL(
                sql=join_sql,
                cte_name=cte_name,
                dataset_columns=final_dataset_columns,
                join_statement=final_join_statement,
            )
        return sql

