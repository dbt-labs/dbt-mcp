"""Unit tests for _AttrDict lenient fallback wrapper."""

import pytest

from dbt_mcp.dbt_admin.run_artifacts.artifacts.lenient import _AttrDict, _wrap


class TestAttrDict:
    def test_attribute_access_present_key(self) -> None:
        d = _AttrDict({"foo": "bar"})
        assert d.foo == "bar"

    def test_attribute_access_missing_key_raises(self) -> None:
        d = _AttrDict({"foo": "bar"})
        with pytest.raises(AttributeError):
            _ = d.missing

    def test_hasattr_false_for_missing_key(self) -> None:
        d = _AttrDict({"foo": "bar"})
        assert not hasattr(d, "missing")

    def test_hasattr_true_for_present_key(self) -> None:
        d = _AttrDict({"foo": "bar"})
        assert hasattr(d, "foo")

    def test_schema_alias_maps_schema_underscore(self) -> None:
        """schema_ attribute should resolve to the 'schema' JSON key."""
        d = _AttrDict({"schema": "my_schema"})
        assert d.schema_ == "my_schema"

    def test_nested_dict_wrapped_recursively(self) -> None:
        d = _AttrDict({"node": {"name": "test"}})
        assert isinstance(d.node, _AttrDict)
        assert d.node.name == "test"

    def test_nested_list_items_wrapped(self) -> None:
        d = _AttrDict({"results": [{"status": "error"}]})
        results = d.results
        assert isinstance(results, list)
        assert isinstance(results[0], _AttrDict)
        assert results[0].status == "error"

    def test_get_returns_wrapped_value(self) -> None:
        d = _AttrDict({"key": {"nested": 1}})
        val = d.get("key")
        assert isinstance(val, _AttrDict)
        assert val.nested == 1

    def test_get_returns_default_for_missing(self) -> None:
        d = _AttrDict({})
        assert d.get("missing", "default") == "default"

    def test_contains(self) -> None:
        d = _AttrDict({"a": 1})
        assert "a" in d
        assert "b" not in d

    def test_len(self) -> None:
        d = _AttrDict({"a": 1, "b": 2})
        assert len(d) == 2

    def test_bool_true_for_nonempty(self) -> None:
        assert bool(_AttrDict({"a": 1}))

    def test_bool_false_for_empty(self) -> None:
        assert not bool(_AttrDict({}))

    def test_iter_yields_keys(self) -> None:
        d = _AttrDict({"x": 1, "y": 2})
        assert set(d) == {"x", "y"}

    def test_items_is_reusable(self) -> None:
        """items() must be re-iterable (list), not a single-use generator."""
        d = _AttrDict({"a": 1})
        items = d.items()
        first_pass = list(items)
        second_pass = list(items)
        assert first_pass == second_pass

    def test_values_is_reusable(self) -> None:
        """values() must be re-iterable (list), not a single-use generator."""
        d = _AttrDict({"a": 1})
        vals = d.values()
        first_pass = list(vals)
        second_pass = list(vals)
        assert first_pass == second_pass

    def test_items_wraps_values(self) -> None:
        d = _AttrDict({"node": {"name": "test"}})
        pairs = dict(d.items())
        assert isinstance(pairs["node"], _AttrDict)

    def test_keys(self) -> None:
        d = _AttrDict({"a": 1, "b": 2})
        assert set(d.keys()) == {"a", "b"}


class TestWrap:
    def test_dict_becomes_attrdict(self) -> None:
        result = _wrap({"key": "val"})
        assert isinstance(result, _AttrDict)

    def test_list_items_wrapped_recursively(self) -> None:
        result = _wrap([{"key": "val"}, 42])
        assert isinstance(result[0], _AttrDict)
        assert result[1] == 42

    def test_scalar_returned_as_is(self) -> None:
        assert _wrap(42) == 42
        assert _wrap("hello") == "hello"
        assert _wrap(None) is None

    def test_nested_list_of_dicts(self) -> None:
        result = _wrap([{"a": {"b": 1}}])
        assert isinstance(result[0], _AttrDict)
        assert isinstance(result[0].a, _AttrDict)
        assert result[0].a.b == 1
