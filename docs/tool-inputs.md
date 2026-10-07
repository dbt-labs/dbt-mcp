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
default. It is not a usable project ID. It lets a configured adapter omit an
unused selector without injecting `None` or inventing an ID. The argument's
type remains `int`: it is required whenever exposed, or removed completely
when bound. Access declarations survive adaptation through `tool.targets`.

Use the existing named mapper API to build context from selectors:

```python
async def build_context(project_id: int) -> DiscoveryToolContext:
    config = await config_provider.get_config(project_id=project_id)
    return DiscoveryToolContext(config=config)

tool = get_all_models.remove_body_parameters("project_id").adapt_with_mappers(
    context=build_context,
)
```

`remove_body_parameters` removes the selector from body invocation while
preserving its canonical declaration for schema binding and authorization.
The mapper receives `project_id` and supplies `context`. The body receives
context and ordinary arguments; the selector keeps its unused declaration
default. A configured mapper with no selector arguments exposes no selectors. Local
providers supporting both project selection and configured environments may
accept omission internally; the canonical declaration still defines the
model's type and requiredness. The dispatcher binds known selections and
validates exposed selections before invocation. Credential refresh can change
that projection without changing tool definitions.

Remote hosts use `tool.input_signature` to prepare the complete unbound input
contract, resolve and authorize targets, then build context.

Named mappers are checked at adaptation time: their return annotations must fit
the destination's value type, including nullability. For example, a mapper
returning `int | None` cannot supply an implementation argument declared `int`.
Context mappers must also accept the selectors' declared value types.
