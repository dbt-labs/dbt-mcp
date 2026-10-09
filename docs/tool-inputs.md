# Tool inputs and resolved context

Declare all model inputs on the tool function. Target annotations describe
resolution and permission requirements; context mappers use those inputs
to build resolved context:

```python
@dbt_mcp_tool(
    title="Get All Models",
    description="List the project's models.",
)
async def get_all_models(
    context: DiscoveryToolContext,
    limit: int = 50,
    after: str | None = None,
    *,
    project_id: Annotated[
        int,
        ProjectTarget(
            requires=Permission.METADATA_READ,
            environment=EnvironmentRole.PRODUCTION,
        ),
    ] = Field(description="Project ID."),
):
    return await context.models_fetcher.fetch_models(limit=limit, after=after)
```

`ProjectTarget` declares resolution and permission requirements. It does not
change how arguments reach the body. Selector arguments pass through the
context adapter normally, but the body reads resolved context instead of the
selector argument. Ordinary
arguments, such as `limit` or a job ID used directly by the body, pass through.
Discovery fetchers already hold the resolved config, so bodies need not retrieve
or forward it on each call.

`Field(...)` declares a required schema input and provides a Python declaration
default. It is not a usable project ID. Hosts with fixed runtime context can
omit an unused selector while supplying resolved context. The argument's
type remains `int`: it is required whenever exposed, or removed completely
when bound. Access declarations survive adaptation through `tool.targets`.

Use named mappers for context injection and input selection:

```python
async def build_context(project_id: int | None = None) -> DiscoveryToolContext:
    config = await config_provider.get_config(project_id=project_id)
    return DiscoveryToolContext(config=config)

tool = get_all_models.adapt_with_mappers(
    context=build_context,
)

# The host knows the selected project; the model sees limit and after.
def selected_project() -> int:
    return 42

bound = tool.adapt_with_mappers(project_id=selected_project)
result = await bound.fastmcp_tool.run({"limit": 10})

# Fixed context can omit an unused selector without fabricating a value.
from dbt_mcp.tools.injection import HIDE

fixed = get_all_models.adapt_with_mappers(
    context=build_context,
    project_id=HIDE,
)
```

1. Context mapper inputs form the framework callable's signature. Existing
   arguments retain the tool's declared types, defaults and annotations. A mapper
   can accept omission internally without making an exposed selector nullable.
2. A zero-argument mapper hides its destination and injects its result. `HIDE`
   removes an input from the exposed signature without supplying a value; any
   context mapper that uses it must allow omission. Apply known-value mappers
   after context adaptation so their values reach the context mapper.
3. FastMCP generates its schema and argument validator from the adapted callable.
   Its generated Pydantic model forbids extra inputs, so unknown arguments and
   attempts to override hidden selectors fail before mapping or execution.
4. The context mapper resolves and authorizes the inputs it declares, then builds
   the implementation context. A host can capture the particular tool definition
   and request-local authorizer in the mapper. Mapper inputs also reach the body
   when it declares them: inspecting `sql` or a job ID does not consume it. Only
   mapper destination parameters are replaced.

For multiple projects, leave `project_id` exposed as a required integer. For
alternative project/environment selectors, hide the unused selector and let the
context mapper derive the target using the host's shared resolver. The marker
does not perform resolution or permission checks. Adaptations return new tool
views and never modify canonical definitions or registered tools.

The local dispatcher adapts callables from the current credential selection for
listing and invocation. Remote hosts first batch authorization of available
targets, then select argument mappers for listing and calls. Private agents can
hide selectors supplied by their fixed context before generating model schemas.

Named mappers are checked at adaptation time: their return annotations must fit
the destination's value type, including nullability. For example, a mapper
returning `int | None` cannot supply an implementation argument declared `int`.
Context mappers must also accept the selectors' declared value types.
