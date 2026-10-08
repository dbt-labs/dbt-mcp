# Tool inputs and resolved context

Declare all model inputs on the tool function. Target annotations describe
resolution and permission requirements; registration controls which inputs
are consumed by context building:

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
change how arguments reach the body. When a selector contributes to resolved
context, remove it from body invocation explicitly before adapting the tool.
The body reads resolved context instead of the selector argument. Ordinary
arguments, such as `limit` or a job ID used directly by the body, pass through.
Discovery fetchers already hold the resolved config, so bodies need not retrieve
or forward it on each call.

`Field(...)` declares a required schema input and provides a Python declaration
default. It is not a usable project ID. It lets the body omit an unused selector
while the host supplies resolved context. The argument's
type remains `int`: it is required whenever exposed, or removed completely
when bound. Access declarations survive adaptation through `tool.targets`.

Keep context injection and input binding separate:

```python
async def build_context(project_id: int | None = None) -> DiscoveryToolContext:
    config = await config_provider.get_config(project_id=project_id)
    return DiscoveryToolContext(config=config)

tool = get_all_models.remove_body_parameters("project_id").adapt_with_mappers(
    context=build_context,
)

# The host knows the selected project; the model sees limit and after.
bound = tool.bind_inputs(InputBinding(values={"project_id": 42}))
inputs = bound.validate_and_bind({"limit": 10})
```

1. `adapt_with_mappers` builds the framework callable. Its mapper can accept
   omission internally, while the declared model input remains a required
   `int`. Framework context parameters are excluded from model inputs.
2. `remove_body_parameters` consumes selectors at invocation without changing
   their declarations. The body receives context and ordinary arguments.
3. `InputBinding` describes host selections and available choices. The same
   binding hides known values in the schema, rejects overrides, and supplies
   them on calls. `validate_and_bind` also validates ordinary declared inputs.
   A host resolves and authorizes these values before building tool context.

For multiple projects, use
`InputBinding(choices={"project_id": (10, 42)})`: the model sees a required
integer selector restricted to those choices. Bindings return new tool views;
they never modify the canonical definition. Validation models are cached by
input shape, independently of request values and choices.

The local dispatcher projects schemas from the current credential selection
and uses the same binding on calls. Remote hosts first authorize available
targets, then bind the shared input contract. Private agents can bind selectors
from their fixed runtime context before generating model schemas.

Named mappers are checked at adaptation time: their return annotations must fit
the destination's value type, including nullability. For example, a mapper
returning `int | None` cannot supply an implementation argument declared `int`.
Context mappers must also accept the selectors' declared value types.
