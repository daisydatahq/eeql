from eeql.core.Attribute import Attribute
from eeql.core.DataType import DataType
from pydantic import BaseModel, PrivateAttr, computed_field, model_validator, Field
from typing import Dict, Type, Any, Optional



class Aggregation(BaseModel):
    aggregation_name: str
    attribute: Attribute
    _output_type: DataType = PrivateAttr(default=None)
    coalesce_value: Optional[Any] = None

    # @model_validator(mode="before")
    # def check_fields(cls, values):
    #     return _check_fields(cls=cls, values=values, properties=["aggregation_name"])

    # @computed_field(return_type=str)

    @property
    def _j(self):
        return "j"

    @property
    def joined_attribute_alias(self) -> str:
        if "event_alias" in self.attribute.model_fields.keys():
            if self.attribute.event_alias:
                col = self.attribute.event_alias
            else:
                col = self.attribute.attribute_name
        else:
            col = self.attribute.attribute_name
        return f"{self._j}.{col}"

    def aggregation_statement(self) -> str:
        raise NotImplementedError("Must be defined in all subclasses")

    def _derive_output_type(self) -> DataType:
        # Subclasses must override and return a DataType
        raise NotImplementedError("Subclasses must implement _derive_output_type()")

    @model_validator(mode="after")
    def _compute_output_type(self):
        ot = self._derive_output_type()
        if not isinstance(ot, DataType):
            raise TypeError("_derive_output_type must return a DataType instance")
        self._output_type = ot
        return self

    @computed_field
    @property
    def output_type(self) -> DataType:
        return self._output_type

    @classmethod
    def register(cls, subclass: Type["Aggregation"]):
        AGGREGATION_REGISTRY[subclass.model_fields["aggregation_name"].default] = subclass
        return subclass

    @classmethod
    def from_type_name(cls, type_name: str) -> "Aggregation":
        subclass = AGGREGATION_REGISTRY.get(type_name)
        if subclass is None:
            raise ValueError(f"Unknown data type: {type_name}")
        return subclass()


AGGREGATION_REGISTRY: Dict[str, Type] = dict()
