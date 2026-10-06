# Tool inputs and resolved context

Declare all model inputs on the tool function. Mark selectors consumed by
context building with `ContextInput()` alongside their target metadata:

```python
@dbt_mcp_tool(
    title="Get All Models",
    description="List the project's models.",
)
async def get_all_models(
    context: DiscoveryToolContext,
    project_id: Annotated[
        int,
        ProjectTarget(
            requires=Permission.METADATA_READ,
            environment=EnvironmentRole.PRODUCTION,
        ),
        ContextInput(),
    ] = Field(description="Project ID."),
    limit: int = 50,
):
    config = await context.config_provider.get_config()
    return await context.models_fetcher.fetch_models(config=config, limit=limit)
```

`ProjectTarget` declares resolution and permission requirements. `ContextInput`
declares that the context adapter consumes the input. The body must read the
resolved configuration instead of the selector argument. Ordinary arguments,
such as `limit` or a job ID used directly by the body, have no `ContextInput`.

`Field(...)` declares a required schema input and provides a Python declaration
default. It is not a usable project ID. It lets a configured adapter omit an
unused selector without injecting `None` or inventing an ID. The argument's
type remains `int`: it is required whenever exposed, or removed completely
when bound. Access declarations survive adaptation through `tool.targets`.

Use the existing named mapper API to build context from selectors:

```python
async def build_context(project_id: int) -> DiscoveryToolContext:
    config = await config_provider.get_config(project_id=project_id)
    return DiscoveryToolContext(StaticConfigProvider(config))

tool = get_all_models.adapt_with_mappers(context=build_context)
```

The mapper receives `project_id` and supplies `context`. The body receives
context and ordinary arguments; the consumed selector is left unused. A
configured mapper with no selector arguments exposes no selectors. Local
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
