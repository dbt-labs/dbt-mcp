Parse the profiles.yml file of the dbt project and return detailed information about all available targets.

This tool reads the profiles.yml configuration file to extract comprehensive information about all configured targets for the project. Each target represents a different environment or connection configuration (e.g., dev, staging, prod) that dbt can use to connect to your data warehouse.

For each target, the tool returns:
- Target name
- Database adapter type (e.g., postgres, snowflake, bigquery, redshift)
- Database name/catalog

The profiles.yml file is typically located in:

- The project directory (as specified by DBT_PROJECT_DIR)
- The user's home directory at ~/.dbt/profiles.yml
- Or as specified by the DBT_PROFILES_DIR environment variable

Returns a JSON structure with detailed information about all profiles and their targets, including connection details that can be used with the --target flag in other dbt commands.
