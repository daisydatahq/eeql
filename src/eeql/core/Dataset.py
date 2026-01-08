from typing import Optional, List, DefaultDict, Union, Dict
from pydantic import (
    BaseModel,
    Field,
    create_model,
    ConfigDict
)
from eeql.core.DatasetColumn import DatasetColumn
from eeql.core.Event import Event
from eeql.core.Filter import Filter
from eeql.core.Entity import Entity
from eeql.core.Selector import Selector
from eeql.core.Attribute import Attribute
from eeql.core.JoinType import JoinType
from eeql.core.DatasetEvent import BaseEvent, JoinedEvent, DatasetEventSQL
from eeql.core.Column import BaseDatasetColumn, JoinedDatasetColumn, DerivedDatasetColumn
from collections import defaultdict




class Dataset(BaseModel):
    dataset_name: str
    base_event: Optional[BaseEvent] = Field(init=False, default=None)
    joined_events: Optional[BaseModel] = Field(init=False, default=None)
    derived_columns: Optional[BaseModel] = Field(init=False, default=None)
    model_config = ConfigDict(extra="allow")
    # graph: nx.DiGraph = Field(default=nx.DiGraph())
    # model_config = ConfigDict(arbitrary_types_allowed=True)

    # @model_validator(mode="before")
    # def check_dataset_fields(cls, values):
    #     return _check_fields(cls=cls, values=values, properties=["graph"])

    @property
    def dataset_columns(self) -> BaseModel:
        columns: DefaultDict[str, DatasetColumn] = dict()
        if self.base_event:
            for alias, column in self.base_event.columns:
                columns[alias] = column
        
        if self.joined_events:
            for joined_event_alias, joined_event in self.joined_events:
                for alias, column in joined_event.columns:
                    columns[alias] = column
        
        # if self.derived_columns:
        #     for alias, column in self.derived_columns:
        #         columns[alias] = column
        
        if len(columns.keys()) != 0:
            DatasetColumnModel = create_model("DatasetColumnModel", **{key: (DatasetColumn, None) for key in columns.keys()})
            dcm = DatasetColumnModel(**columns)
            return dcm
        else:
            return None
    

    def select(
        self,
        event: Event,
        default_entity: Union[Entity, str],
        selector: Selector,
        columns: Dict[str, Attribute] = Field(default_factory=defaultdict(Attribute)),
        filters: Optional[List[Filter]] = None,
    ) -> None:
        if self.base_event:
            raise ValueError("Base event already defined for dataset.")

        # for filter in filters:
        #     if filter.attribute.attribute_name not in event.attributes.model_fields.keys():
        #         raise ValueError(f"Filtered on invalid attribute {filter.attribute.attribute_name} from event {event.event_name}")
        
        # TODO: check to confirm each attribute passed is an attribute
        DatasetColumnModel = create_model("DatasetColumnModel", **{key: (BaseDatasetColumn, None) for key in columns.keys()})
        columns = DatasetColumnModel(**{alias: BaseDatasetColumn(attribute=attribute, alias=alias) for alias, attribute in columns.items()})

        if isinstance(default_entity, str):
            if default_entity not in event.entities.model_fields.keys():
                raise ValueError(
                    f"Invalid entity `{default_entity}` specified for the base dataset event. Valid Options are {list(event.entities.model_fields.keys())}"
                )
            else:
                default_entity = [e[1] for e in event.entities if e[0] == default_entity][0]


        selector.entity_id = default_entity.entity_id
        selector.event_id = event.event_id
        selector.event_timestamp = event.event_timestamp

        self.base_event = BaseEvent(
            event=event,
            default_entity=default_entity,
            selector=selector,
            filters=filters,
            columns=columns,
        )
        for alias, column in self.base_event.columns:
            setattr(column, "table_alias", self.base_event._table)


    def join(
        self,
        event: Event,
        join_type: JoinType,
        columns: DefaultDict[str, JoinedDatasetColumn],
        entity: Optional[Union[Entity, str, List[Entity], List[str]]] = None,
        filters: Optional[List[Filter]] = None,
        join_name: Optional[str] = None,
        additional_join_expressions: Optional[List[str]] = None,
    ) -> None:
        if not self.base_event:
            raise ValueError("Base event must be defined before specifying a joined event.")
        
        if not entity:
            entity: Entity = self.base_event.default_entity
        
        if not isinstance(entity, list):
            entity = [entity]
        for e in entity:
            base_event_entity: Entity = getattr(self.base_event.event.entities, e.entity_name)
        # if base_event_entity.join_types:
        #     base_event_entity.join_types.append(join_type)
        # else:
        #     base_event_entity.join_types = [join_type]
            setattr(self.base_event.event.entities, base_event_entity.entity_name, base_event_entity)

        join_type.primary_timestamp = self.base_event.event.event_timestamp
        join_type.joined_timestamp = event.event_timestamp
        join_type.primary_repeated_at = base_event_entity.event_repeated_at
        join_type.primary_preceded_at = base_event_entity.event_repeated_at

        for alias, column in columns.items():
            setattr(column, "alias", alias)
        

        je = JoinedEvent(
            event=event,
            columns=columns,
            entity=entity,
            join_type=join_type,
            filters=filters,
            dataset_event_name=join_name,
            additional_join_expressions=additional_join_expressions,
        )


        if self.joined_events:
            joined_event_dict = {je.unique_name: je}
            joined_events = list(self.joined_events.model_fields.keys())
            for joined_event in joined_events:
                joined_event_dict[joined_event] = getattr(self.joined_events, joined_event)
            JoinedEventModel = create_model("JoinedEventModel", **{f: (JoinedEvent, None) for f in joined_event_dict.keys()})
            self.joined_events = JoinedEventModel(**joined_event_dict)
    
        else:
            JoinedEventModel = create_model("JoinedEventModel", **{je.unique_name: (JoinedEvent, None)})
            self.joined_events = JoinedEventModel(**{je.unique_name: je})


    def derive(
        self,
        columns: List[DerivedDatasetColumn]
    ):
        derived_columns: DefaultDict[str, DerivedDatasetColumn] = dict()
        if self.derived_columns:
            for alias, dc in self.derived_columns:
                derived_columns[alias] = dc
        for col in columns:
            alias = col.alias
            is_valid_dependency = False
            for dependency_alias, dep in col.dependencies:
                if dependency_alias in self.base_event.columns.model_fields.keys():
                    is_valid_dependency = True
                    break

                if not is_valid_dependency and self.joined_events:
                    for je_alias, je in self.joined_events:
                        if dependency_alias in je.columns.model_fields.keys():
                            is_valid_dependency = True
                            break

                if not is_valid_dependency and len(derived_columns) > 0:
                    for dc_alias, dc in derived_columns.items():
                        if dependency_alias == dc.alias:
                            is_valid_dependency = True
                            break

                if not is_valid_dependency:
                    raise ValueError(f"Specified dependency {dependency_alias} in derived column {alias} does not exist in the dataset.")

            derived_columns[alias] = col

        DerivedColumnModel = create_model("DerivedColumnModel", **{alias: (DerivedDatasetColumn, None) for alias in derived_columns.keys()})
        self.derived_columns = DerivedColumnModel(**derived_columns)
    
    # TODO: implement support for adding predefined dataset columns from semantic layer
    def include(self) -> None:
        pass


    def to_sql(self):
        if not self.base_event:
            raise ValueError("Base Event must be specified for dataset before generating sql")
        
        query = "with\n"
        final_select = "select\n"
        base_event_sql = self.base_event.base_sql()
        comma = ",\n"
        if base_event_sql.event_sql:
            query += f"{base_event_sql.event_cte_name} as (\n{base_event_sql.event_sql}\n){comma}"
        query += f"{base_event_sql.cte_name} as (\n{base_event_sql.sql}\n)"
        final_select += ",\n".join(base_event_sql.dataset_columns)
        final_join = f"\n{base_event_sql.from_statement}\n{base_event_sql.join_statement}\n"

        if self.joined_events:
            for joined_event_name, joined_event in self.joined_events:
                joined_event_sql: DatasetEventSQL = joined_event.base_sql(base_event=self.base_event)
                if joined_event_sql.event_sql and joined_event_sql.event_cte_name != base_event_sql.event_cte_name:
                    query += f"{comma}{joined_event_sql.event_cte_name} as (\n{joined_event_sql.event_sql}\n)"
                query += f"{comma}{joined_event_sql.cte_name} as (\n{joined_event_sql.sql}\n)\n"
                final_select += comma
                final_select += ",\n".join(joined_event_sql.dataset_columns)
                final_join += f"{joined_event_sql.join_statement}\n"
        
        if self.derived_columns:
            for alias, derived_column in self.derived_columns:
                final_select += f"{comma}{derived_column.expression()} as {alias}"
        
        return f"{query}\n{final_select}\n{final_join}"
    
