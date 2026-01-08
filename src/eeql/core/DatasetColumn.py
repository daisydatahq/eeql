from eeql.core.Attribute import Attribute
from eeql.core.Event import Event
from pydantic import BaseModel
from typing import Optional


class DatasetColumn(BaseModel):
    attribute: Attribute
    table_alias: Optional[str] = None
    alias: Optional[str] = None

