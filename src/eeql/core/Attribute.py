from eeql.core.DataType import DataType, DATA_TYPE_REGISTRY
from typing import Optional, Dict, Type
from pydantic import BaseModel, computed_field, field_validator


class Attribute(BaseModel):
    data_type: DataType
    attribute_name: str
    description: Optional[str] = None
    event_alias: Optional[str] = None

    @field_validator("data_type")
    def set_data_type(cls, v, values, **kwargs):
        if isinstance(v, str):
            if v not in DATA_TYPE_REGISTRY.keys():
                raise ValueError(f"Data type `{v}` not supported. Please provide one of: {list(DATA_TYPE_REGISTRY.keys())}")
            v = DATA_TYPE_REGISTRY[v]
        return v


    @classmethod
    def register(cls, subclass: Type["Attribute"]):
        ATTRIBUTE_REGISTRY[subclass.model_fields["attribute_name"].default] = subclass
        return subclass

    @classmethod
    def from_attribute_name(cls, attribute_name: str) -> "Attribute":
        subclass = ATTRIBUTE_REGISTRY.get(attribute_name)
        if subclass is None:
            raise ValueError(f"Unknown attribute: {attribute_name}")
        return subclass()

    @computed_field
    @property
    def event_column(self) -> str:
        if self.event_alias:
            return self.event_alias
        else:
            return self.attribute_name


ATTRIBUTE_REGISTRY: Dict[str, Type] = dict()
