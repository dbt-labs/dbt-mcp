from collections.abc import Mapping
from dataclasses import dataclass

import pytest
from pydantic import BaseModel

from dbt_mcp.tracking.token_estimation import (
    EMPTY_MEASUREMENT,
    TOKEN_ESTIMATION_METHOD,
    PayloadMeasurement,
    measure_payload,
)


class TestMeasurePayload:
    def test_none_returns_zeroed(self):
        assert measure_payload(None) == EMPTY_MEASUREMENT

    def test_empty_string(self):
        result = measure_payload("")
        assert result == PayloadMeasurement(char_count=0, token_estimate=0)

    def test_plain_string(self):
        text = "metric list result"
        result = measure_payload(text)
        assert result.char_count == len(text)
        assert result.token_estimate == len(text) // 4

    def test_dict_serialized_as_sorted_json(self):
        args = {"z_key": "val", "a_key": 42}
        result = measure_payload(args)
        expected = '{"a_key": 42, "z_key": "val"}'
        assert result.char_count == len(expected)
        assert result.token_estimate == len(expected) // 4

    def test_text_content_like_object(self):
        """Objects with a .text attribute (e.g. MCP TextContent) use .text."""

        @dataclass
        class TextContent:
            type: str
            text: str

        obj = TextContent(type="text", text="hello world")
        result = measure_payload(obj)
        assert result.char_count == len("hello world")

    def test_content_attribute_delegates(self):
        """Objects with .content recurse into that attribute."""

        @dataclass
        class Inner:
            text: str

        @dataclass
        class Outer:
            content: Inner

        obj = Outer(content=Inner(text="nested"))
        result = measure_payload(obj)
        assert result.char_count == len("nested")

    def test_sequence_of_text_blocks(self):
        """A list of TextContent-like blocks is concatenated."""

        @dataclass
        class Block:
            text: str

        blocks = [Block(text="aaa"), Block(text="bbb")]
        result = measure_payload(blocks)
        assert result.char_count == 6
        assert result.token_estimate == 1

    def test_sequence_with_mixed_types(self):
        """Sequences can contain a mix of strings and objects."""

        @dataclass
        class Block:
            text: str

        items: list = ["raw", Block(text="obj")]
        result = measure_payload(items)
        assert result.char_count == len("raw") + len("obj")

    def test_pydantic_model(self):
        class MyModel(BaseModel):
            name: str
            count: int

        obj = MyModel(name="test", count=5)
        result = measure_payload(obj)
        assert result.char_count == len(obj.model_dump_json())
        assert result.char_count > 0

    def test_fallback_to_str(self):
        result = measure_payload(42)
        assert result.char_count == len("42")

    def test_serialization_error_returns_empty(self):
        """If _to_text raises, measure_payload returns EMPTY_MEASUREMENT."""

        class Bomb:
            """An object that defeats every serialization path."""

            @property
            def text(self):
                raise RuntimeError("boom")

            @property
            def content(self):
                raise RuntimeError("boom")

            def model_dump_json(self):
                raise RuntimeError("boom")

            def json(self):
                raise RuntimeError("boom")

            def __str__(self):
                raise RuntimeError("boom")

            def __iter__(self):
                raise RuntimeError("boom")

        result = measure_payload(Bomb())
        assert result == EMPTY_MEASUREMENT

    def test_token_estimation_method_constant(self):
        assert TOKEN_ESTIMATION_METHOD == "char_div_4"

    def test_token_estimate_rounds_down(self):
        result = measure_payload("abc")
        assert result.char_count == 3
        assert result.token_estimate == 0

        result = measure_payload("abcd")
        assert result.char_count == 4
        assert result.token_estimate == 1

    def test_nested_dict_values_use_default_str(self):
        """Non-JSON-serializable dict values use str() via default=str."""

        @dataclass
        class Custom:
            val: int

        args = {"key": Custom(val=99)}
        result = measure_payload(args)
        assert result.char_count > 0
