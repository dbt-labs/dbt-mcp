It is important to add the `--select` before the selector if any selector needs to be provided.
If you provide a selector, DO NOT forget adding `--select` in front!

- to select all models, just do not provide a selector.
- to select a particular model, use the selector `--select <model_name>`
- to select a particular model and all the downstream ones (also known as children), use the selector `--select <model_name>+`
- to select a particular model and all the upstream ones (also known as parents), use the selector `--select +<model_name>`
- to select a particular model and all the downstream and upstream ones, use the selector `--select +<model_name>+`
- to select the union of different selectors, separate them with a space like `--select selector1 selector2`
- to select the intersection of different selectors, separate them with a comma like `--select selector1,selector2`