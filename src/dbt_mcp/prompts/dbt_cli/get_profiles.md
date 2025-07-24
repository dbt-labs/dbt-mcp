Parses the dbt project's profiles.yml and return detailed information about all available targets.

This tool reads the profiles.yml configuration file to extract comprehensive information about all configured targets for the project. Each target represents a different environment or connection configuration (e.g., dev, staging, prod) that dbt can use to connect to your data warehouse.

For each target, the tool returns a JSON object containing:

- Target name
- Database adapter type (e.g., postgres, snowflake, bigquery, redshift)
- Database name/catalog
