Generate lightweight YAML for dbt sources by introspecting database schemas. This tool queries your warehouse to discover tables in a schema and produces the YAML structure for a source that can be pasted into a schema.yml file. Supports selective table generation and can optionally include column definitions with data types. Perfect for bootstrapping new sources or documenting existing database schemas in your dbt project.

Note: This is one of three dbt-codegen tools (generate_source, generate_model_yaml, generate_staging_model) that are available when dbt-codegen is enabled. These tools are opt-in and must be explicitly enabled in the configuration.

Always return the generated YAML output to the user.