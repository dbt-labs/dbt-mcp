dbt compile generates executable SQL from source model, test, and analysis files.

The compile command is useful for visually inspecting the compiled output of model files. This is useful for validating complex jinja logic or macro usage.

By default, non-error logs are suppressed. Set suppress_non_error_logs=false to receive the full compiled SQL output.
