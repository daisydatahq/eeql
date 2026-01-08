from eeql.core.Column import DerivedDatasetColumn, BaseDatasetColumn
from eeql.core.Attribute import Attribute
from pydantic import field_validator, model_validator, BaseModel, Field, create_model
from eeql.core.DatasetColumn import DatasetColumn
from eeql.vocabulary import data_types as dty
from typing import Union, Optional


class Division(DerivedDatasetColumn):
    numerator: DatasetColumn
    denominator: DatasetColumn
    data_type: Union[dty.TypeInteger, dty.TypeFloat] = Field(default=dty.TypeFloat(), init=False)
    dependencies: Optional[BaseModel] = Field(default=None, validate_default=True)

    @field_validator("numerator", "denominator", mode="before")
    def validate_data_type(cls, v: DatasetColumn, values, **kwargs):
        if isinstance(v, DerivedDatasetColumn):
            data_type = v.data_type
        else:
            data_type = v.attribute.data_type

        valid_types = [dty.TypeInteger, dty.TypeFloat]
        if not any([isinstance(data_type, valid_type) for valid_type in valid_types]):
            raise ValueError(f"{v.alias} has invalid data type {data_type}. Data type should be one of {valid_types}")
        return v
    
    @model_validator(mode="before")
    def validate_dependencies(cls, values):
        numerator: DatasetColumn = values.get("numerator")
        denominator: DatasetColumn = values.get("denominator")
        DatasetColumnModel = create_model("DatasetColumnModel", **{dep.alias: (DatasetColumn, None) for dep in [numerator, denominator]})
        values["dependencies"] = DatasetColumnModel(**{numerator.alias: numerator, denominator.alias: denominator})
        return values
        
    
    def expression(self):
        return f"case when {self.denominator.alias} = 0 then null else {self.numerator.alias}/{self.denominator.alias} end"



a = BaseDatasetColumn(attribute=Attribute(data_type=dty.TypeFloat(), attribute_name="a", event_alias="a"), alias="a")
isinstance(a, DatasetColumn)


div = Division(
    alias="test_division",
    numerator=a,
    denominator=a,
)

div.expression()
div.dependencies




import re

def parse_variables(expression):
    # Regular expression to find variables in the form of $<variable_name>
    variable_pattern = re.compile(r"\$[a-zA-Z_][a-zA-Z0-9_]*")
    
    # Find all occurrences of the pattern in the expression
    variables = variable_pattern.findall(expression)
    
    return variables


class Custom(DerivedDatasetColumn):
    sql: str

    @field_validator("sql", mode="before")
    def validate_expression(cls, v, values, **kwargs):
        alias: str = values.data["alias"]
        dependencies = values.data["dependencies"]

        if "$" not in v:
            raise ValueError(f"No dependencies found in expression for custom derived column `{alias}`. All specified dependencies must be included in the expression and should be specified with a `$` immediately followed by the alias name. Specified dependencies are: {[d.alias for d in dependencies]}")


        vars = parse_variables(v)

        for dep_alias, dep in dependencies:
            if "$" + dep.alias not in vars:
                raise ValueError(f"`{dep.alias}` not specified in the expression for derived dataset column `{alias}`")
        
        return v
    
    def expression(self) -> str:
        return self.sql.replace("$", "")

Custom(
    alias="test_custom_column",
    sql=f"${div.alias}*100",
    dependencies=[div],
    data_type=dty.TypeInteger()
).expression()

