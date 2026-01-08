from typing import Optional, List, Dict
from pydantic import Field, BaseModel, create_model, model_validator
from eeql.core.Attribute import Attribute
from eeql.core.Aggregation import Aggregation
from eeql.core.DataType import DataType
from eeql.core.Event import Event
from eeql.core.JoinType import JoinType
from eeql.core.Entity import Entity
from eeql.core.Filter import Filter
from eeql.core.DatasetColumn import DatasetColumn


class BaseDatasetColumn(DatasetColumn):
    pass

class JoinedDatasetColumn(DatasetColumn):
    aggregation: Aggregation
    # event: Event
    # join_type: JoinType
    # entity: Entity
    table_alias: str = Field(default="j", init=False)
    # filters: Optional[List[Filter]] = None
    # additional_join_expressions: Optional[List[str]] = None
    


    # def to_yaml(self) -> dict:
    #     filters = None
    #     if self.filters:
    #         filters: Dict[str, List[str]] = dict()
    #         for f in self.filters:
    #             if f.attribute.event_alias not in filters.keys:
    #                 filters[f.attribute.event_alias] = [f.expression]
    #             else:
    #                 filters[f.attribute.event_alias].append(f.expression)
    #     return {
    #         "dataset_column_name": self.alias,
    #         "event_name": self.event.event_name,
    #         "aggregation": self.aggregation.aggregation_name,
    #         "join_type": self.join_type.join_type_name,
    #         "entity": self.entity.entity_name,
    #         "filters": filters,
    #         "additional_join_expressions": self.additional_join_expressions,
    #     }
    
    # @classmethod
    # def from_yaml(cls, yaml_dict: dict, event: Event, entity: Entity):

        



class DerivedDatasetColumn(DatasetColumn):
    alias: str
    data_type: DataType
    attribute: Optional[Attribute] = None
    dependencies: Optional[BaseModel] = None

    # @field_validator("dependencies", mode="before")
    @model_validator(mode="before")
    def validate_dependencies(cls, values):
        v = values.get("dependencies")
        if v is None:
            return v

        alias = values.get("alias")
        if isinstance(v, DatasetColumn):
            v = [v]
        if isinstance(v, list):
            v = {d.alias: d for d in v}
        if isinstance(v, dict):
            DatasetColumnModel = create_model("DatasetColumnModel", **{key: (DatasetColumn, None) for key in v.keys()})
            v = DatasetColumnModel(**v)
        if not isinstance(v, BaseModel):
            raise ValueError(f"Can't process dependencies for derived column `{alias}`. Passed {type(v)} - Must pass a Dataset Column, list, dict, or custom BaseModel instead.")
        
        for alias, dc in v:
            if not isinstance(dc, DatasetColumn):
                raise ValueError(f"All dependencies specified in dataset column `{alias}` should be dataset columns. Found dependency type {type(dc)}")

        values["dependencies"] = v
        return values

    def expression(self) -> str:
        raise NotImplementedError("Must implement expression method for derived column")
    
    # def to_yaml(self) -> dict:
    #     return {
    #         "dataset_column_name": self.alias,
    #         "event_name": self.event.event_name,
    #         "data_type": self.data_type.data_type_name,
    #         "dependencies": {alias: dc.to_yaml() for alias, dc in self.dependencies.items()}
    #     }

