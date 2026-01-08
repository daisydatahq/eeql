from eeql.utils import _check_fields
from pydantic import BaseModel, model_validator, ConfigDict, PrivateAttr
from sqlglot import transpile
from typing import Dict, Type


class DataType(BaseModel):
    data_type_name: str
    default_sql: str
    model_config = ConfigDict(arbitrary_types_allowed=True)
    # _registry: Dict[str, Type] = PrivateAttr(default_factory=dict)

    # @model_validator(mode="before")
    # def check_attribute_fields(cls, values):
    #     return _check_fields(cls=cls, values=values, properties=["data_type_name", "default_sql"])

    @classmethod
    def register(cls, subclass: Type["DataType"]):
        DATA_TYPE_REGISTRY[subclass.model_fields["data_type_name"].default] = subclass
        return subclass

    @classmethod
    def from_type_name(cls, type_name: str) -> "DataType":
        subclass = DATA_TYPE_REGISTRY.get(type_name)
        if subclass is None:
            raise ValueError(f"Unknown data type: {type_name}")
        return subclass()


    # def __init_subclass__(cls, **kwargs):
    #     super().__init_subclass__(**kwargs)
    #     if cls is not DataType:
    #         instance = cls.model_construct()  # safe instantiation with default values
    #         DATA_TYPE_REGISTRY[instance.data_type_name.default] = cls


    # @classmethod
    # def from_type_name(cls, type_name: str):
    #     subclass = cls._registry.get(type_name.lower())
    #     if subclass is None:
    #         raise ValueError(f"Unknown data type: {type_name}")
    #     return subclass()



    def transpile(self, engine: str):
        output = transpile(self.default_sql, read="duckdb", write=engine)
        if isinstance(output, list):
            return output[0]
        elif isinstance(output, str):
            return output
        else:
            raise ValueError(f"Unexpected type f{type(output)} returned from sqlglot transpilation of data type `{self.data_type_name}`")



DATA_TYPE_REGISTRY: Dict[str, Type] = dict()
