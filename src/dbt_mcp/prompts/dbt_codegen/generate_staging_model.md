Generate SQL for a staging model from a source table. This creates a complete SELECT statement with all columns from the source, properly aliased and formatted according to dbt best practices. Includes optional model configuration block for materialization, column name casing preferences, and comma placement style. The generated SQL serves as the foundation for your staging layer and can be directly saved as a new model file.

Note: This is one of three dbt-codegen tools (generate_source, generate_model_yaml, generate_staging_model) that are available when dbt-codegen is enabled. These tools are opt-in and must be explicitly enabled in the configuration.

Always return the generated SQL output to the user.