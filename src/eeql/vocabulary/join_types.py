from typing import Optional
from pydantic import (
    Field
)
from eeql.core.JoinType import JoinType
from eeql.vocabulary.attributes import (
    EventTimestamp,
    EventPrecededAt,
    EventRepeatedAt
)


class Before(JoinType):
    join_type_name: str = Field(default="before")

    def join_statement(
        self,
        # primary_entity_id: Optional[EntityId]=None,
        primary_timestamp: Optional[EventTimestamp]=None,
        # joined_entity_id: Optional[EntityId]=None,
        joined_timestamp: Optional[EventTimestamp]=None,
        **kwargs
    ) -> str:
        # if not primary_entity_id:
        #     primary_entity_id = self.primary_entity_id
        if not primary_timestamp:
            primary_timestamp = self.primary_timestamp
        # if not joined_entity_id:
        #     joined_entity_id = self.joined_entity_id
        if not joined_timestamp:
            joined_timestamp = self.joined_timestamp
        # if not all([primary_entity_id, joined_entity_id, primary_timestamp, joined_timestamp]):
        if not all([primary_timestamp, joined_timestamp]):
            raise ValueError(f"Must define all variables before referencing the join_statement property")
        # entity_join = f"{self._p}.{primary_entity_id.event_alias} = {self._j}.{joined_entity_id.event_alias}"
        timestamp_join = f"{self._p}.{primary_timestamp.event_alias} > {self._j}.{joined_timestamp.event_alias}"
        # return f"on {entity_join} and {timestamp_join}"
        return f"and {timestamp_join}"

Before().join_statement(
    # primary_entity_id=EntityId(event_alias="test_id"),
    primary_timestamp=EventTimestamp(event_alias="activity_at"),
    # joined_entity_id=EntityId(event_alias="test_id"),
    joined_timestamp=EventTimestamp(event_alias="activity_at"),
)


class Since(JoinType):
    join_type_name: str = Field(default="since")


    def join_statement(
        self,
        # primary_entity_id: Optional[EntityId],
        primary_timestamp: Optional[EventTimestamp],
        # joined_entity_id: Optional[EntityId],
        joined_timestamp: Optional[EventTimestamp],
        primary_preceded_at: Optional[EventPrecededAt],
        **kwargs
    ) -> str:
        # if not primary_entity_id:
        #     primary_entity_id = self.primary_entity_id
        if not primary_timestamp:
            primary_timestamp = self.primary_timestamp
        # if not joined_entity_id:
        #     joined_entity_id = self.joined_entity_id
        if not joined_timestamp:
            joined_timestamp = self.joined_timestamp
        if not primary_preceded_at:
            primary_preceded_at = self.primary_preceded_at
        # if not all([primary_entity_id, joined_entity_id, primary_timestamp, joined_timestamp, primary_preceded_at]):
        if not all([primary_timestamp, joined_timestamp, primary_preceded_at]):
            raise ValueError(f"Must define all variables before referencing the join_statement property")
        # entity_join = f"{self._p}.{primary_entity_id.event_alias} = {self._j}.{joined_entity_id.event_alias}"
        timestamp_join_start = f"{self._p}.{primary_preceded_at.event_alias} < {self._j}.{joined_timestamp.event_alias}"
        timestamp_join_end = f"{self._p}.{primary_timestamp.event_alias} > {self._j}.{joined_timestamp.event_alias}"
        # return f"on {entity_join} and {timestamp_join_start} and {timestamp_join_end}"
        return f"and {timestamp_join_start} and {timestamp_join_end}"

Since().join_statement(
    # primary_entity_id=EntityId(event_alias="test_id"),
    primary_timestamp=EventTimestamp(event_alias="activity_at"),
    # joined_entity_id=EntityId(event_alias="test_id"),
    joined_timestamp=EventTimestamp(event_alias="activity_at"),
    primary_preceded_at=EventPrecededAt(event_alias="preceded_at"),
)



class Between(JoinType):
    join_type_name: str = Field(default="between")

    def join_statement(
        self,
        # primary_entity_id: Optional[EntityId],
        primary_timestamp: Optional[EventTimestamp],
        # joined_entity_id: Optional[EntityId],
        joined_timestamp: Optional[EventTimestamp],
        primary_repeated_at: Optional[EventRepeatedAt],
        **kwargs
    ) -> str:
        # if not primary_entity_id:
        #     primary_entity_id = self.primary_entity_id
        if not primary_timestamp:
            primary_timestamp = self.primary_timestamp
        # if not joined_entity_id:
        #     joined_entity_id = self.joined_entity_id
        if not joined_timestamp:
            joined_timestamp = self.joined_timestamp
        if not primary_repeated_at:
            primary_repeated_at = self.primary_repeated_at
        # if not all([primary_entity_id, joined_entity_id, primary_timestamp, joined_timestamp, primary_repeated_at]):
        if not all([primary_timestamp, joined_timestamp, primary_repeated_at]):
            raise ValueError(f"Must define all variables before referencing the join_statement property")
        # entity_join = f"{self._p}.{primary_entity_id.event_alias} = {self._j}.{joined_entity_id.event_alias}"
        timestamp_join_start = f"{self._p}.{primary_timestamp.event_alias} < {self._j}.{joined_timestamp.event_alias}"
        timestamp_join_end = f"{self._p}.{primary_repeated_at.event_alias} > {self._j}.{joined_timestamp.event_alias}"
        # return f"on {entity_join} and {timestamp_join_start} and {timestamp_join_end}"
        return f"and {timestamp_join_start} and {timestamp_join_end}"

Between().join_statement(
    # primary_entity_id=EntityId(event_alias="test_id"),
    primary_timestamp=EventTimestamp(event_alias="activity_at"),
    # joined_entity_id=EntityId(event_alias="test_id"),
    joined_timestamp=EventTimestamp(event_alias="activity_at"),
    primary_repeated_at=EventRepeatedAt(event_alias="preceded_at"),
)



class After(JoinType):
    join_type_name: str = Field(default="after")

    def join_statement(
        self,
        # primary_entity_id: Optional[EntityId],
        primary_timestamp: Optional[EventTimestamp],
        # joined_entity_id: Optional[EntityId],
        joined_timestamp: Optional[EventTimestamp],
        **kwargs
    ) -> str:
        # if not primary_entity_id:
        #     primary_entity_id = self.primary_entity_id
        if not primary_timestamp:
            primary_timestamp = self.primary_timestamp
        # if not joined_entity_id:
        #     joined_entity_id = self.joined_entity_id
        if not joined_timestamp:
            joined_timestamp = self.joined_timestamp
        # if not all([primary_entity_id, joined_entity_id, primary_timestamp, joined_timestamp]):
        if not all([primary_timestamp, joined_timestamp]):
            raise ValueError(f"Must define all variables before referencing the join_statement property")
        # entity_join = f"{self._p}.{primary_entity_id.event_alias} = {self._j}.{joined_entity_id.event_alias}"
        timestamp_join = f"{self._p}.{primary_timestamp.event_alias} < {self._j}.{joined_timestamp.event_alias}"
        # return f"on {entity_join} and {timestamp_join}"
        return f"and {timestamp_join}"

After().join_statement(
    # primary_entity_id=EntityId(event_alias="test_id"),
    primary_timestamp=EventTimestamp(event_alias="activity_at"),
    # joined_entity_id=EntityId(event_alias="test_id"),
    joined_timestamp=EventTimestamp(event_alias="activity_at"),
)




class All(JoinType):
    join_type_name: str = Field(default="all")

    def join_statement(
        self,
        # primary_entity_id: Optional[EntityId],
        # joined_entity_id: Optional[EntityId],
        **kwargs
    ) -> str:
        # if not primary_entity_id:
        #     primary_entity_id = self.primary_entity_id
        # if not joined_entity_id:
        #     joined_entity_id = self.joined_entity_id
        # if not all([primary_entity_id, joined_entity_id]):
        #     raise ValueError(f"Must define all variables before referencing the join_statement property")
        # entity_join = f"{self._p}.{primary_entity_id.event_alias} = {self._j}.{joined_entity_id.event_alias}"
        # return f"on {entity_join}"
        return ""

All().join_statement(
    # primary_entity_id=EntityId(event_alias="test_id"),
    # joined_entity_id=EntityId(event_alias="test_id"),
)


