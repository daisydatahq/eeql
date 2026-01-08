from pydantic import  Field
from eeql.core.DataType import DataType

@DataType.register
class TypeString(DataType):
    data_type_name: str = Field(default="string")
    default_sql: str = Field(default="string")

@DataType.register
class TypeInteger(DataType):
    data_type_name: str = Field(default="integer")
    default_sql: str = Field(default="int")

@DataType.register
class TypeFloat(DataType):
    data_type_name: str = Field(default="float")
    default_sql: str = Field(default="float")

@DataType.register
class TypeBoolean(DataType):
    data_type_name: str = Field(default="boolean")
    default_sql: str = Field(default="bool")

@DataType.register
class TypeTimestamp(DataType):
    data_type_name: str = Field(default="timestamp")
    default_sql: str = Field(default="timestamp")

@DataType.register
class TypeDate(DataType):
    data_type_name: str = Field(default="date")
    default_sql: str = Field(default="date")

@DataType.register
class TypeTime(DataType):
    data_type_name: str = Field(default="time")
    default_sql: str = Field(default="time")
