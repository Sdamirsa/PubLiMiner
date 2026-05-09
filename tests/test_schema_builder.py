"""Tests for steps/extract/schema_builder.py."""

from __future__ import annotations

import pytest

from publiminer.steps.extract.schema_builder import (
    FieldDef,
    build_json_schema,
    build_pydantic_model,
    validate_fields,
)


def _f(name: str, ftype: str, parent: str | None = None, required: bool = True, values: list | None = None) -> FieldDef:
    return FieldDef(name=name, type=ftype, description=f"{name} desc", required=required, parent=parent, values=values or [])


class TestValidateFields:
    def test_passes_valid_flat(self) -> None:
        validate_fields([_f("a", "string"), _f("b", "integer")])

    def test_duplicate_name_raises(self) -> None:
        with pytest.raises(ValueError, match="Duplicate"):
            validate_fields([_f("a", "string"), _f("a", "string")])

    def test_unknown_type_raises(self) -> None:
        with pytest.raises(ValueError, match="unknown type"):
            validate_fields([_f("a", "badtype")])

    def test_enum_without_values_raises(self) -> None:
        with pytest.raises(ValueError, match="enum type requires"):
            validate_fields([FieldDef(name="x", type="enum", description="x")])

    def test_missing_parent_raises(self) -> None:
        with pytest.raises(ValueError, match="parent .* does not exist"):
            validate_fields([_f("child", "string", parent="nonexistent")])

    def test_circular_parent_raises(self) -> None:
        fields = [
            FieldDef(name="a", type="object", description="a", parent=None),
            FieldDef(name="b", type="string", description="b", parent="a"),
        ]
        # Make a circular: a → b → a (hack parent field)
        fields[0].parent = "b"
        with pytest.raises(ValueError, match="[Cc]ircular"):
            validate_fields(fields)


class TestBuildJsonSchema:
    def test_flat_schema(self) -> None:
        fields = [_f("title", "string"), _f("year", "integer")]
        result = build_json_schema(fields, "my_schema")
        assert result["type"] == "json_schema"
        schema = result["json_schema"]["schema"]
        assert schema["type"] == "object"
        assert "title" in schema["properties"]
        assert schema["properties"]["year"]["type"] == "integer"
        assert schema["additionalProperties"] is False
        assert result["json_schema"]["strict"] is True

    def test_enum_field(self) -> None:
        fields = [_f("status", "enum", values=["yes", "no", "unclear"])]
        result = build_json_schema(fields, "s")
        prop = result["json_schema"]["schema"]["properties"]["status"]
        assert prop["type"] == "string"
        assert prop["enum"] == ["yes", "no", "unclear"]

    def test_required_vs_optional(self) -> None:
        fields = [_f("a", "string", required=True), _f("b", "string", required=False)]
        schema = build_json_schema(fields, "s")["json_schema"]["schema"]
        assert "a" in schema["required"]
        assert "b" not in schema.get("required", [])

    def test_nested_object(self) -> None:
        fields = [
            FieldDef(name="meta", type="object", description="meta obj"),
            FieldDef(name="year", type="integer", description="year", parent="meta"),
        ]
        schema = build_json_schema(fields, "s")["json_schema"]["schema"]
        meta_prop = schema["properties"]["meta"]
        assert meta_prop["type"] == "object"
        assert "year" in meta_prop["properties"]

    def test_list_of_objects(self) -> None:
        fields = [
            FieldDef(name="authors", type="list[object]", description="authors"),
            FieldDef(name="name", type="string", description="name", parent="authors"),
        ]
        schema = build_json_schema(fields, "s")["json_schema"]["schema"]
        authors_prop = schema["properties"]["authors"]
        assert authors_prop["type"] == "array"
        assert "name" in authors_prop["items"]["properties"]

    def test_leaf_types(self) -> None:
        type_map = {
            "string": "string",
            "integer": "integer",
            "float": "number",
            "boolean": "boolean",
        }
        for ftype, expected_json_type in type_map.items():
            fields = [_f("x", ftype)]
            schema = build_json_schema(fields, "s")["json_schema"]["schema"]
            assert schema["properties"]["x"]["type"] == expected_json_type

    def test_list_string_and_integer(self) -> None:
        fields = [_f("tags", "list[string]"), _f("counts", "list[integer]")]
        schema = build_json_schema(fields, "s")["json_schema"]["schema"]
        assert schema["properties"]["tags"] == {"type": "array", "items": {"type": "string"}, "description": "tags desc"}
        assert schema["properties"]["counts"]["items"]["type"] == "integer"


class TestBuildPydanticModel:
    def test_flat_model(self) -> None:
        fields = [_f("title", "string"), _f("year", "integer")]
        Model = build_pydantic_model(fields, "Test")
        obj = Model(title="hello", year=2024)
        assert obj.title == "hello"
        assert obj.year == 2024

    def test_optional_field(self) -> None:
        fields = [_f("a", "string", required=False)]
        Model = build_pydantic_model(fields, "Opt")
        obj = Model()
        assert obj.a is None

    def test_nested_object(self) -> None:
        fields = [
            FieldDef(name="meta", type="object", description="m"),
            FieldDef(name="year", type="integer", description="y", parent="meta"),
        ]
        Model = build_pydantic_model(fields, "Nested")
        obj = Model(meta={"year": 2024})
        assert obj.meta.year == 2024

    def test_enum_field(self) -> None:
        fields = [_f("status", "enum", values=["yes", "no"])]
        Model = build_pydantic_model(fields, "EnumTest")
        obj = Model(status="yes")
        assert obj.status == "yes"
