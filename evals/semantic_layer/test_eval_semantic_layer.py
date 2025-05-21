import json
from typing import Any

import pytest
from dbtsl.api.shared.query_params import GroupByParam
from openai import OpenAI
from openai.types.responses import (
    FunctionToolParam,
    ResponseFunctionToolCall,
    ResponseInputParam,
    ResponseOutputItem,
)
from openai.types.responses.response_input_param import FunctionCallOutput

from client.tools import get_tools
from dbt_mcp.config.config import load_config
from dbt_mcp.mcp.server import dbt_mcp
from dbt_mcp.semantic_layer.client import get_semantic_layer_fetcher
from dbt_mcp.semantic_layer.types import OrderByParam, QueryMetricsSuccess

LLM_MODEL = "gpt-4o-mini"
llm_client = OpenAI()
config = load_config()
assert config.semantic_layer_config
semantic_layer_fetcher = get_semantic_layer_fetcher(config.semantic_layer_config)


async def expect_metadata_tool_call(
    messages: list,
    tools: list[FunctionToolParam],
    expected_tool: str,
    expected_arguments: str | None = None,
) -> ResponseOutputItem:
    response = llm_client.responses.create(
        model=LLM_MODEL,
        input=messages,
        tools=tools,
        parallel_tool_calls=False,
    )
    assert len(response.output) == 1
    tool_call = response.output[0]
    assert isinstance(tool_call, ResponseFunctionToolCall)
    function_name = tool_call.name
    function_arguments = tool_call.arguments
    assert tool_call.type == "function_call"
    assert function_name == expected_tool
    assert expected_arguments is None or function_arguments == expected_arguments
    tool_response = await dbt_mcp.call_tool(
        function_name,
        json.loads(function_arguments),
    )
    messages.append(tool_call)
    messages.append(
        FunctionCallOutput(
            type="function_call_output",
            call_id=tool_call.call_id,
            output=str(tool_response),
        )
    )
    return tool_call


def deep_equal(dict1: Any, dict2: Any) -> bool:
    if isinstance(dict1, dict) and isinstance(dict2, dict):
        return dict1.keys() == dict2.keys() and all(
            deep_equal(dict1[k], dict2[k]) for k in dict1
        )
    elif isinstance(dict1, list) and isinstance(dict2, list):
        return len(dict1) == len(dict2) and all(
            deep_equal(x, y) for x, y in zip(dict1, dict2, strict=False)
        )
    else:
        return dict1 == dict2


def expect_query_metrics_tool_call(
    messages: list,
    tools: list[FunctionToolParam],
    expected_metrics: list[str],
    expected_group_by: list[dict] | None = None,
    expected_order_by: list[dict] | None = None,
    expected_where: str | None = None,
    expected_limit: int | None = None,
):
    response = llm_client.responses.create(
        model=LLM_MODEL,
        input=messages,
        tools=tools,
        parallel_tool_calls=False,
    )
    assert len(response.output) == 1
    tool_call = response.output[0]
    assert isinstance(tool_call, ResponseFunctionToolCall)
    assert tool_call.name == "query_metrics"
    args_dict = json.loads(tool_call.arguments)
    assert set(args_dict["metrics"]) == set(expected_metrics)
    if expected_group_by is not None:
        assert deep_equal(args_dict["group_by"], expected_group_by)
    else:
        assert args_dict.get("group_by", []) == []
    if expected_order_by is not None:
        assert deep_equal(args_dict["order_by"], expected_order_by)
    else:
        assert args_dict.get("order_by", []) == []
    if expected_where is not None:
        assert args_dict["where"] == expected_where
    else:
        assert args_dict.get("where", None) is None
    if expected_limit is not None:
        assert args_dict["limit"] == expected_limit
    else:
        assert args_dict.get("limit", None) is None

    tool_response = semantic_layer_fetcher.query_metrics(
        metrics=args_dict["metrics"],
        group_by=[
            GroupByParam(name=g["name"], type=g["type"], grain=g.get("grain"))
            for g in args_dict.get("group_by", [])
        ],
        order_by=[
            OrderByParam(name=o["name"], descending=o["descending"])
            for o in args_dict.get("order_by", [])
        ],
        where=args_dict.get("where"),
        limit=args_dict.get("limit"),
    )
    assert isinstance(tool_response, QueryMetricsSuccess)


def initial_messages(content: str) -> ResponseInputParam:
    return [
        {
            "role": "user",
            "content": content,
        }
    ]


@pytest.mark.parametrize(
    "content, expected_tool",
    [
        (
            "What metrics are available? Use the list_metrics tool",
            "list_metrics",
        ),
        (
            "What dimensions are available for the order metric? Use the get_dimensions tool",
            "get_dimensions",
        ),
        (
            "What entities are available for the order metric? Use the get_entities tool",
            "get_entities",
        ),
    ],
)
async def test_explicit_tool_request(content: str, expected_tool: str):
    response = llm_client.responses.create(
        model=LLM_MODEL,
        input=initial_messages(content),
        tools=await get_tools(),
        parallel_tool_calls=False,
    )
    assert len(response.output) == 1
    assert response.output[0].type == "function_call"
    assert response.output[0].name == expected_tool


async def test_semantic_layer_fulfillment_query():
    tools = await get_tools()
    messages = initial_messages(
        "How many orders did we fulfill this month last year?",
    )
    await expect_metadata_tool_call(
        messages,
        tools,
        "list_metrics",
        "{}",
    )
    await expect_metadata_tool_call(
        messages,
        tools,
        "get_dimensions",
        '{"metrics":["orders"]}',
    )
    expect_query_metrics_tool_call(
        messages,
        tools,
    )


async def test_semantic_layer_food_revenue_per_month():
    tools = await get_tools()
    messages = initial_messages(
        "What is our food revenue per location per month?",
    )
    await expect_metadata_tool_call(
        messages,
        tools,
        "list_metrics",
        "{}",
    )
    await expect_metadata_tool_call(
        messages,
        tools,
        "get_dimensions",
        '{"metrics":["food_revenue"]}',
    )
    await expect_metadata_tool_call(
        messages,
        tools,
        "get_entities",
        '{"metrics":["food_revenue"]}',
    )
    expect_query_metrics_tool_call(
        messages=messages,
        tools=tools,
        expected_metrics=["food_revenue"],
        expected_group_by=[
            {
                "name": "order_id__location__location_name",
                "type": "entity",
            },
            {
                "name": "metric_time",
                "type": "time_dimension",
                "grain": "MONTH",
            },
        ],
        expected_order_by=[
            {
                "name": "metric_time",
                "descending": True,
            },
        ],
        expected_limit=5,
    )


async def test_semantic_layer_what_percentage_of_orders_were_large():
    tools = await get_tools()
    messages = initial_messages(
        "What percentage of orders were large this year?",
    )
    await expect_metadata_tool_call(
        messages,
        tools,
        "list_metrics",
        "{}",
    )
    expect_query_metrics_tool_call(
        messages=messages,
        tools=tools,
        expected_metrics=["orders", "large_orders"],
        expected_where="metric_time >= '2024-01-01' and metric_time < '2025-01-01'",
    )
