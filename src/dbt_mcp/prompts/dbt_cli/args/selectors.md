A selector needs be used to select specific nodes in the DAG or to do actions on one particular nodes. A node can be a model, a test, a seed or a snapshot. It is strongly preferred to provide a selector, especially on large projects. Always provide a selector initially.

- To select all models, just do not provide a selector
- To select one particular model, use the selector `<model_name>`

## Union and Intersection
- To select the union of different selectors, separate them with a space like `selector1 selector2`
- To select the intersection of different selectors, separate them with a comma like `selector1,selector2`

## Downstream and Upstream

Downstream and upstream models are selected with a `+` and the number of children or parents to select.

- To select a particular model and the downstream ones (also known as children), use the selector `<model_name>+<number of models>`.
- To select a particular model and the upstream ones (also known as parents), use the selector `<number of models>+<model_name>`.
- To select a particular model and the downstream and upstream ones, use the selector `<number of models>+<model_name>+<number of models>`.

If the user asks for all downstream or upstream models, use a sensible default like 5 and then iteratively traverse the DAG to collect the models. If a timeout occurs, reduce this number.

<example>
User: give me all models downstream of orders
Think step-by-step:
- The user is asking for downstream models so I should use the `<model_name>+<number of models>` syntax
- The user is asking about the orders model, so the selection will look like this: `orders+<number of models>`
- The user is asking for all models, so I should start with a sensible default like 5 and iterate from there: `orders+5`
Final answer: `orders+5`
</example>