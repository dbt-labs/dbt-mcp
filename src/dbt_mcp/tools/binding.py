from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from mcp.types import Tool


@dataclass(frozen=True)
class InputBinding:
    """A host's input selection, shared by schema projection and invocation."""

    values: dict[str, Any] = field(default_factory=dict)
    hidden: frozenset[str] = frozenset()
    choices: dict[str, tuple[Any, ...]] = field(default_factory=dict)

    @property
    def hidden_parameters(self) -> frozenset[str]:
        return self.hidden | self.values.keys()

    def input_schema(self, original: dict[str, Any]) -> dict[str, Any]:
        schema = dict(original)
        schema["properties"] = {
            name: dict(value)
            for name, value in original.get("properties", {}).items()
            if name not in self.hidden_parameters
        }
        required = [
            name
            for name in original.get("required", [])
            if name not in self.hidden_parameters
        ]
        for name, choices in self.choices.items():
            parameter = schema["properties"][name]
            non_nullable = [
                variant
                for variant in parameter.get("anyOf", [])
                if variant.get("type") != "null"
            ]
            if len(non_nullable) == 1 and None not in choices:
                parameter.pop("anyOf")
                parameter.update(non_nullable[0])
            parameter.pop("default", None)
            parameter["enum"] = list(choices)
            required.append(name)
        if required:
            schema["required"] = list(dict.fromkeys(required))
        else:
            schema.pop("required", None)
        return schema

    def schema(self, tool: Tool) -> Tool:
        return tool.model_copy(
            update={"inputSchema": self.input_schema(tool.inputSchema)}
        )

    def bind_arguments(
        self, arguments: Mapping[str, Any], *, parameters: set[str]
    ) -> dict[str, Any]:
        unknown = arguments.keys() - parameters - self.hidden_parameters
        if unknown:
            raise ValueError(f"Unknown tool arguments: {', '.join(sorted(unknown))}")
        for name in self.hidden_parameters & arguments.keys():
            raise ValueError(f"{name} is bound by the tool schema; omit this argument")
        for name, choices in self.choices.items():
            selected = arguments.get(name)
            if not any(
                type(selected) is type(choice) and selected == choice
                for choice in choices
            ):
                raise ValueError(f"{name} is required; choose one of {list(choices)}")
        return dict(arguments) | self.values
