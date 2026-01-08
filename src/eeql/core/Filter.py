from eeql.core.Attribute import Attribute


from pydantic import BaseModel


class Filter(BaseModel):
    attribute: Attribute
    expression: str