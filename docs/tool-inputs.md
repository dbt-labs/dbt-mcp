# Tool inputs and resolved context

Keep ordinary arguments on the implementation function. Declare selectors that
are consumed by context building in the tool's `inputs` dictionary:

```python
@dbt_mcp_tool(
    title="Get All Models",
    description="List the project's models.",
    inputs={
        "project_id": Annotated[
            int,
            ProjectTarget(
                requires=Permission.METADATA_READ,
                environment=EnvironmentRole.PRODUCTION,
            ),
            Field(description="Project ID."),
        ],
    },
)
async def get_all_models(context: DiscoveryToolContext, limit: int = 50):
    config = await context.config_provider.get_config()
    return await context.models_fetcher.fetch_models(config=config, limit=limit)
```

`inputs` describes the unbound model contract. An `int` selector is required
whenever the model sees it; binding removes the parameter entirely. It does not
turn the selector into `int | None`. Access declarations remain available through
`tool.targets`, even after configured context mappers hide all selectors.

Use the existing named mapper API to build context from selectors:

```python
async def build_context(project_id: int) -> DiscoveryToolContext:
    config = await config_provider.get_config(project_id=project_id)
    return DiscoveryToolContext(StaticConfigProvider(config))

tool = get_all_models.adapt_with_mappers(context=build_context)
```

The adapter consumes `project_id` and supplies `context`; the implementation
never receives a replacement or placeholder project ID. A configured mapper
with no selector arguments exposes no selectors. Local providers that support
both project selection and configured environments may accept an omitted
selector internally, while the declaration still defines the public schema.
The dispatcher binds known selections and validates exposed selections before
invocation. Credential refresh can change that projection without changing the
implementation contract.

Remote hosts use `tool.input_signature` to prepare the full model contract,
resolve and authorize the requested targets, then build context. Pass only the
implementation's ordinary arguments and mapped context to its function.

Named mappers are checked at adaptation time: their return annotations must fit
the destination's value type, including nullability. For example, a mapper
returning `int | None` cannot supply an implementation argument declared `int`.
Context mappers must also accept the selectors' declared value types.
