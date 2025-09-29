Generate YAML documentation for dbt models by introspecting their compiled SQL. This tool analyzes one or more models to produce complete schema.yml entries with all column names and optionally their data types. Can intelligently inherit column descriptions from upstream models and sources for consistent documentation. Essential for maintaining comprehensive model documentation and enabling dbt's testing framework.

Note: This is one of three dbt-codegen tools (generate_source, generate_model_yaml, generate_staging_model) that are available when dbt-codegen is enabled. These tools are opt-in and must be explicitly enabled in the configuration.

Always return the generated YAML output to the user.