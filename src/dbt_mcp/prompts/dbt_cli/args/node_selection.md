Node selection string passed to dbt's `--select` flag (like `my_model`, `tag:nightly`, `path:models/staging`).

This parameter accepts **inline node selection syntax only**. It does **NOT** accept named
selectors from `selectors.yml` — use the `yml_selector` parameter for those instead.

A selection should be used when we need to select specific nodes or are asking to do actions on specific nodes. A node can be a model, a test, a seed or a snapshot. It is strongly preferred to provide a selection, especially on large projects. Always provide a selection initially.

- to select all models, just do not provide a selection
- to select a particular model, use the selection `<model_name>`

## Graph operators

- to select a particular model and all the downstream ones (also known as children), use the selection `<model_name>+`
- to select a particular model and all the upstream ones (also known as parents), use the selection `+<model_name>`
- to select a particular model and all the downstream and upstream ones, use the selection `+<model_name>+`
- to select the union of different selections, separate them with a space like `selection1 selection2`
- to select the intersection of different selections, separate them with a comma like `selection1,selection2`

## Matching nodes based on parts of their name

When looking to select nodes based on parts of their names, the selection needs to be `fqn:<pattern>`. The fqn is the fully qualified name, it starts with the project or package name followed by the subfolder names and finally the node name.

### Examples

- to select a node from any package that contains `stg_`, we would have the selection `fqn:*stg_*`
- to select a node from the current project that contains `stg_`, we would have the selection `fqn:project_name.*stg_*`
