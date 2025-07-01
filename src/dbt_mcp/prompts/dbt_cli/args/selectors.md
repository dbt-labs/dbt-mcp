A selector needs be used when we need to select specific nodes or are asking to do actions on specific nodes. A node can be a model, a test, a seed or a snapshot. It is strongly preferred to provide a selector, especially on large projects. Always provide a selector initially.

- to select all models, just do not provide a selector
- to select a particular model, use the selector `<model_name>`

## Graph operators

- to select a particular model and all the downstream ones (also known as children), use the selector `<model_name>+`
- to select a particular model and all the upstream ones (also known as parents), use the selector `+<model_name>`
- to select a particular model and all the downstream and upstream ones, use the selector `+<model_name>+`
- to select the union of different selectors, separate them with a space like `selector1 selector2`
- to select the intersection of different selectors, separate them with a comma like `selector1,selector2`

## Matching nodes based on parts of their name

When looking to select nodes based on parts of their names, the selector needs to be `fqn:<pattern>`. The fqn is the fully qualified name, it starts with the project or package name followed by the subfolder names and finally the node name.

### Examples

- to select a node from any package that contains `stg_`, we would have the selector `fqn:*stg_*`
- to select a node from the current project that contains `stg_`, we would have the selector `fqn:project_name.*stg_*`
