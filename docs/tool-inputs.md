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
bound = tool.bind_inputs(InputBinding(values={"project_id": 42}))
result = await bound.to_fastmcp_internal_tool().run({"limit": 10})
```

1. `adapt_with_mappers` builds the framework callable. Its mapper can accept
   omission internally, while the declared model input remains a required
   `int` on the adapted callable's signature. FastMCP reads that signature
   directly. Framework context parameters are excluded from model inputs.
2. `InputBinding` returns a callable with the selected inputs removed from its
   signature. FastMCP generates its schema and argument validator from that
   callable. Its generated Pydantic model forbids extra inputs, so unknown
   arguments and attempts to override hidden selectors fail before execution.
3. After validation, the callable supplies bound values and runs its mappers.
   A host can capture request-local resolution and authorization state in a
   mapper, then pass authorized context directly to an inner context builder.
   Compose adaptations to run selection/authorization before context injection;
   mappers in the same adaptation independently read the caller's inputs.

For multiple projects, leave `project_id` exposed as a required integer and
validate the selected ID in a mapper. Known values can also be injected by
zero-argument mappers. `InputBinding` remains useful for fixed context whose
unused selector has no known value, or to hide one selector while exposing
another. Bindings return new tool views; they never modify canonical definitions
or registered tools. Permission decisions remain the host's responsibility.

The local dispatcher binds callables from the current credential selection
for listing and invocation. Remote hosts first batch authorization of available
targets, then bind the callables used for listing and calls. Private agents can
bind selectors from their fixed runtime context before generating model schemas.

Named mappers are checked at adaptation time: their return annotations must fit
the destination's value type, including nullability. For example, a mapper
returning `int | None` cannot supply an implementation argument declared `int`.
Context mappers must also accept the selectors' declared value types.
