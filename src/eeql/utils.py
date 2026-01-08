from typing import List, DefaultDict
from collections import defaultdict
from pydantic import BaseModel, create_model
from pydantic._internal._model_construction import ModelMetaclass


def _check_fields(cls, values, properties: List):
    class_name = cls.__name__
    if not isinstance(properties, ModelMetaclass):
        for property in properties:
            if property in values:
                raise ValueError(f"Cannot specify `{property}` for {class_name} class instances. That property is fixed.")
    return values


def _create_group(object_type: BaseModel, objects: DefaultDict[str, BaseModel]):
    object_type_name = object_type.__name__
    for object in objects.values():
        if not isinstance(object, object_type):
            raise ValueError(f"All objects must be type {object_type_name}. Passed type {object.__class__.__name__}")
    M = create_model(f"{object_type_name.title()}Model", **{o: (object_type, None) for o in objects.keys()})
    return M(**objects)
