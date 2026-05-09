"""Build JSON Schema and Pydantic models from a flat FieldDef list."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, create_model

# Types that map directly to a JSON Schema leaf node
_LEAF_TYPE_MAP: dict[str, dict[str, Any]] = {
    "string": {"type": "string"},
    "integer": {"type": "integer"},
    "float": {"type": "number"},
    "boolean": {"type": "boolean"},
    "list[string]": {"type": "array", "items": {"type": "string"}},
    "list[integer]": {"type": "array", "items": {"type": "integer"}},
}

_ALL_TYPES = set(_LEAF_TYPE_MAP) | {"enum", "object", "list[object]"}


@dataclass
class FieldDef:
    """Definition of one extraction field as provided by the user in YAML."""

    name: str
    type: str
    description: str
    required: bool = True
    parent: str | None = None
    values: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_fields(fields: list[FieldDef]) -> None:
    """Raise ValueError if the field list is inconsistent."""
    names = [f.name for f in fields]
    # Unique names
    seen: set[str] = set()
    for n in names:
        if n in seen:
            raise ValueError(f"Duplicate field name: '{n}'")
        seen.add(n)
    name_set = set(names)
    # Valid types
    for f in fields:
        if f.type not in _ALL_TYPES:
            raise ValueError(f"Field '{f.name}': unknown type '{f.type}'. Valid: {sorted(_ALL_TYPES)}")
        if f.type == "enum" and not f.values:
            raise ValueError(f"Field '{f.name}': enum type requires 'values' list")
        if f.parent and f.parent not in name_set:
            raise ValueError(f"Field '{f.name}': parent '{f.parent}' does not exist")
    # No circular references (parent must not reference descendant)
    for f in fields:
        visited: set[str] = {f.name}
        cursor = f.parent
        while cursor:
            if cursor in visited:
                raise ValueError(f"Circular parent reference detected involving '{f.name}'")
            visited.add(cursor)
            parent_field = next((x for x in fields if x.name == cursor), None)
            cursor = parent_field.parent if parent_field else None


# ---------------------------------------------------------------------------
# JSON Schema builder
# ---------------------------------------------------------------------------


def build_json_schema(fields: list[FieldDef], schema_name: str) -> dict[str, Any]:
    """Return the full response_format envelope for OpenRouter.

    Result shape:
        {"type": "json_schema", "json_schema": {"name": ..., "strict": True, "schema": {...}}}
    """
    validate_fields(fields)
    schema = _build_object_schema(fields, parent=None)
    return {
        "type": "json_schema",
        "json_schema": {
            "name": schema_name,
            "strict": True,
            "schema": schema,
        },
    }


def _build_object_schema(fields: list[FieldDef], parent: str | None) -> dict[str, Any]:
    """Recursively build a JSON Schema 'object' node for children of `parent`."""
    children = [f for f in fields if f.parent == parent]
    properties: dict[str, Any] = {}
    required_list: list[str] = []

    for f in children:
        if f.type in _LEAF_TYPE_MAP:
            prop = dict(_LEAF_TYPE_MAP[f.type])
        elif f.type == "enum":
            prop = {"type": "string", "enum": f.values}
        elif f.type == "object":
            prop = _build_object_schema(fields, parent=f.name)
        elif f.type == "list[object]":
            prop = {
                "type": "array",
                "items": _build_object_schema(fields, parent=f.name),
            }
        else:
            prop = {"type": "string"}

        prop["description"] = f.description
        properties[f.name] = prop
        if f.required:
            required_list.append(f.name)

    schema: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }
    if required_list:
        schema["required"] = required_list
    return schema


# ---------------------------------------------------------------------------
# Pydantic model builder
# ---------------------------------------------------------------------------


def build_pydantic_model(fields: list[FieldDef], schema_name: str) -> type[BaseModel]:
    """Dynamically build a Pydantic v2 BaseModel from the field list."""
    validate_fields(fields)
    return _build_pydantic_node(fields, parent=None, model_name=schema_name)


def _build_pydantic_node(
    fields: list[FieldDef], parent: str | None, model_name: str
) -> type[BaseModel]:
    """Recursively create a Pydantic model for children of `parent`."""
    from typing import Optional

    children = [f for f in fields if f.parent == parent]
    field_definitions: dict[str, Any] = {}

    for f in children:
        if f.type == "string" or f.type == "enum":
            py_type: Any = str
        elif f.type == "integer":
            py_type = int
        elif f.type == "float":
            py_type = float
        elif f.type == "boolean":
            py_type = bool
        elif f.type == "list[string]":
            py_type = list[str]
        elif f.type == "list[integer]":
            py_type = list[int]
        elif f.type == "object":
            py_type = _build_pydantic_node(fields, parent=f.name, model_name=f"{model_name}_{f.name}")
        elif f.type == "list[object]":
            child_model = _build_pydantic_node(fields, parent=f.name, model_name=f"{model_name}_{f.name}")
            py_type = list[child_model]  # type: ignore[valid-type]
        else:
            py_type = str

        if f.required:
            field_definitions[f.name] = (py_type, ...)
        else:
            field_definitions[f.name] = (Optional[py_type], None)

    return create_model(model_name, **field_definitions)
