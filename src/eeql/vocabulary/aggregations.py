from pydantic import Field, field_validator
from eeql.core.Attribute import Attribute
from eeql.core import Column as dc
from eeql.core.Aggregation import Aggregation
from eeql.vocabulary import data_types as dty
from eeql.vocabulary.attributes import EventTimestamp



def aggfunc(func):
    def wrapper(attribute: Attribute, **kwargs):
        
        agg = func(attribute, **kwargs)
        if not isinstance(agg, Aggregation):
            raise ValueError("Function decorated with `aggfunc` decorator needs to return a subclass of Aggregation")

        return dc.JoinedDatasetColumn(aggregation=agg, attribute=attribute)
    return wrapper


@aggfunc
def first_value(attribute: Attribute, timestamp: EventTimestamp):

    @Aggregation.register
    class FirstValue(Aggregation):
        aggregation_name: str = Field(default="first_value")
        timestamp: EventTimestamp

        def aggregation_statement(self) -> str:
            delimiter = "';.,;'"
            return f"cast(split_part(min(cast({self._j}.{timestamp.event_column} as varchar) || {delimiter} || cast({self.joined_attribute_alias} as varchar)), {delimiter}, 2) as {self.attribute.data_type.default_sql})"

        def _derive_output_type(self):
            return self.attribute.data_type

    return FirstValue(attribute=attribute, timestamp=timestamp)

first_value(attribute=Attribute(attribute_name="test", data_type=dty.TypeBoolean()), timestamp=EventTimestamp(event_alias="ts", attribute_name="event_timestamp")).aggregation.aggregation_statement()


@aggfunc
def last_value(attribute: Attribute, timestamp: EventTimestamp):

    @Aggregation.register
    class LastValue(Aggregation):
        aggregation_name: str = Field(default="last_value")
        timestamp: EventTimestamp

        def aggregation_statement(self) -> str:
            delimiter = "';.,;'"
            return f"cast(split_part(max(cast({self._j}.{timestamp.event_column} as varchar) || {delimiter} || cast({self.joined_attribute_alias} as varchar)), {delimiter}, 2) as {self.attribute.data_type.default_sql})"

        def _derive_output_type(self):
            return self.attribute.data_type

    return LastValue(attribute=attribute, timestamp=timestamp)

last_value(attribute=Attribute(attribute_name="test", data_type=dty.TypeBoolean()), timestamp=EventTimestamp(event_alias="ts", attribute_name="event_timestamp")).aggregation.aggregation_statement()


@aggfunc
def nth_value(attribute: Attribute, n: int, timestamp: EventTimestamp):

    @Aggregation.register
    class NthValue(Aggregation):
        aggregation_name: str = Field(default="nth_value")
        timestamp: EventTimestamp

        def aggregation_statement(self) -> str:
            delimiter = "';.,;'"
            return f"cast(split_part(string_agg(cast({self.joined_attribute_alias} as varchar), {delimiter} order by {self._j}.{timestamp.event_column}), {delimiter}, {n}) as {self.attribute.data_type.default_sql})"

        def _derive_output_type(self):
            return self.attribute.data_type

    return NthValue(attribute=attribute, timestamp=timestamp)

nth_value(attribute=Attribute(attribute_name="test", data_type=dty.TypeBoolean()), n=3, timestamp=EventTimestamp(event_alias="ts", attribute_name="event_timestamp")).aggregation.aggregation_statement()



@aggfunc
def not_null(attribute: Attribute):

    @Aggregation.register
    class NotNull(Aggregation):
        aggregation_name: str = Field(default="not_null")

        def aggregation_statement(self) -> str:
            return f"max({self.joined_attribute_alias}) is not null"

        def _derive_output_type(self):
            return dty.TypeBoolean()

    return NotNull(attribute=attribute)

not_null(attribute=Attribute(attribute_name="test", data_type=dty.TypeBoolean())).aggregation.aggregation_statement()


@aggfunc
def is_null(attribute: Attribute):

    @Aggregation.register
    class IsNull(Aggregation):
        aggregation_name: str = Field(default="is_null")

        def aggregation_statement(self) -> str:
            return f"max({self.joined_attribute_alias}) is null"

        def _derive_output_type(self):
            return dty.TypeBoolean()

    return IsNull(attribute=attribute)

is_null(attribute=Attribute(attribute_name="test", data_type=dty.TypeBoolean())).aggregation.aggregation_statement()





@aggfunc
def sum(attribute: Attribute):

    @Aggregation.register
    class Sum(Aggregation):
        aggregation_name: str = Field(default="sum")
        coalesce_value: int = 0

        @field_validator("attribute", mode="before")
        def validate_attribute_type(cls, v):
            if not isinstance(v.data_type, (dty.TypeInteger, dty.TypeFloat, dty.TypeBoolean)):
                raise ValueError("Incompatible data type provided")
            return v

        def aggregation_statement(self) -> str:
            if isinstance(self.attribute.data_type, dty.TypeBoolean):
                suffix = "::int"
            else:
                suffix = ""
            return f"sum({self.joined_attribute_alias}{suffix})"

        def _derive_output_type(self):
            if isinstance(self.attribute.data_type, dty.TypeFloat):
                return dty.TypeFloat()
            else:
                return dty.TypeInteger()

    return Sum(attribute=attribute)

sum(attribute=Attribute(attribute_name="test", data_type=dty.TypeBoolean()))

@aggfunc
def median(attribute: Attribute):

    @Aggregation.register
    class Median(Aggregation):
        aggregation_name: str = Field(default="median")

        @field_validator("attribute", mode="before")
        def validate_attribute_type(cls, v):
            if not isinstance(v.data_type, (dty.TypeInteger, dty.TypeFloat)):
                raise ValueError("Incompatible data type provided")
            return v

        def aggregation_statement(self) -> str:
            return f"median({self.joined_attribute_alias})"

        def _derive_output_type(self):
            return dty.TypeFloat()

    return Median(attribute=attribute)

median(attribute=Attribute(attribute_name="test", data_type=dty.TypeInteger()))

@aggfunc
def average(attribute: Attribute):

    @Aggregation.register
    class Average(Aggregation):
        aggregation_name: str = Field(default="average")

        @field_validator("attribute", mode="before")
        def validate_attribute_type(cls, v):
            if not isinstance(v.data_type, (dty.TypeInteger, dty.TypeFloat, dty.TypeBoolean)):
                raise ValueError("Incompatible data type provided")
            return v

        def aggregation_statement(self) -> str:
            if isinstance(self.attribute.data_type, dty.TypeBoolean):
                suffix = "::boolean"
            else:
                suffix = ""
            return f"avg({self.joined_attribute_alias}{suffix})"

        def _derive_output_type(self):
            return dty.TypeFloat()

    return Average(attribute=attribute)

average(attribute=Attribute(attribute_name="test", data_type=dty.TypeInteger()))

@aggfunc
def count(attribute: Attribute):

    @Aggregation.register
    class Count(Aggregation):
        aggregation_name: str = Field(default="count")
        coalesce_value: int = 0


        def aggregation_statement(self) -> str:
            return f"count({self.joined_attribute_alias})"

        def _derive_output_type(self):
            return dty.TypeInteger()

    return Count(attribute=attribute)

count(attribute=Attribute(attribute_name="test", data_type=dty.TypeInteger()))

@aggfunc
def count_distinct(attribute: Attribute):

    @Aggregation.register
    class CountDistinct(Aggregation):
        aggregation_name: str = Field(default="count_distinct")
        coalesce_value: int = 0

        def aggregation_statement(self) -> str:
            return f"count(distinct {self.joined_attribute_alias})"

        def _derive_output_type(self):
            return dty.TypeInteger()

    return CountDistinct(attribute=attribute)

count_distinct(attribute=Attribute(attribute_name="test", data_type=dty.TypeInteger()))

@aggfunc
def percentile(attribute: Attribute, n: int):
    if n < 1 or n > 100:
        raise ValueError("Invalid value `{n}` parameter `n` provided. Must be between 1 and 100")

    @Aggregation.register
    class Percentile(Aggregation):
        aggregation_name: str = Field(default="percentile")
        n: int

        def aggregation_statement(self) -> str:
            return f"percentile_cont({n/100}) within group (order by {self.joined_attribute_alias})"

        def _derive_output_type(self):
            return dty.TypeFloat()

    return Percentile(attribute=attribute, n=n)

percentile(attribute=Attribute(attribute_name="test", data_type=dty.TypeInteger()), n=99).aggregation.aggregation_statement()
