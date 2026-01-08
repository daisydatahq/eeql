from typing import Optional, List
from pydantic import (
    BaseModel,
    field_validator,
    computed_field,
    ConfigDict
)
from eeql.vocabulary import attributes as att

# from eeql.vocabulary import join_types as jt
# from eeql.core.JoinType import JoinType


class Entity(BaseModel):

    entity_name: str
    entity_id: att.EntityId
    description: Optional[str] = None
    event_table_prefix: Optional[str] = None
    entity_id_source: Optional[att.EntityIdSource] = None
    entity_id_updated_at: Optional[att.EntityIdUpdatedAt] = None
    event_instance: Optional[att.EventInstance] = None
    event_repeated_at: Optional[att.EventRepeatedAt] = None
    event_preceded_at: Optional[att.EventPrecededAt] = None
    is_used_for_join: Optional[bool] = None
    is_default: Optional[bool] = None
    # join_types: Optional[List[JoinType]] = None
    
    model_config = ConfigDict(extra="allow")


    @field_validator(
        "entity_id",
        "entity_id_source",
        "entity_id_updated_at",
        "event_instance",
        "event_repeated_at",
        "event_preceded_at",
        mode="before",
    )
    def create_default_attribute(cls, v, info):
        from eeql.vocabulary import attributes as att
        if isinstance(v, str):
            field_map = dict(
                entity_id=att.EntityId(),
                entity_id_source=att.EntityIdSource(),
                entity_id_updated_at=att.EntityIdUpdatedAt(),
                event_instance=att.EventInstance(),
                event_repeated_at=att.EventRepeatedAt(),
                event_preceded_at=att.EventPrecededAt(),
            )
            field = info.field_name
            alias = v
            v = field_map[field]
            v.event_alias = alias
        return v

    # @computed_field
    # @property
    # def has_since_join(self) -> bool:
    #     if self.join_types:
    #         return any([isinstance(join_type, jt.Since) for join_type in self.join_types])
    #     else:
    #         return None

    # @computed_field
    # @property
    # def has_between_join(self) -> bool:
    #     if self.join_types:
    #         return any([isinstance(join_type, jt.Between) for join_type in self.join_types])
    #     else:
    #         return None

