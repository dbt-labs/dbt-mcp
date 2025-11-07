# Custom Tools

This module provides functionality for discovering and registering custom tools from dbt models.

## Overview

The custom tools system allows you to:
1. Discover dbt models in a `models/tools` directory
2. Parse Jinja variables from model SQL templates
3. Automatically create MCP tools from these models
4. Execute the models with user-provided parameters

## File System Abstraction

The custom tools module uses a pluggable file system abstraction to support different storage backends.

### Using the Default Local File System

By default, the system uses `LocalFileSystemProvider` which reads from the local filesystem:

```python
from dbt_mcp.custom_tools import register_custom_tools

# Uses local filesystem by default
register_custom_tools(dbt_mcp, config_provider, exclude_tools)
```

### Creating a Custom File System Provider

To support alternative storage backends (S3, Azure Blob Storage, remote APIs, etc.), implement the `FileSystemProvider` interface:

```python
from dbt_mcp.custom_tools.filesystem import FileSystemProvider

class S3FileSystemProvider(FileSystemProvider):
    """Example S3-backed file system provider."""

    def __init__(self, s3_client, bucket_name):
        self.s3_client = s3_client
        self.bucket_name = bucket_name

    def exists(self, path: str) -> bool:
        """Check if a file exists in S3."""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=path)
            return True
        except:
            return False

    def read_text(self, path: str) -> str:
        """Read file contents from S3."""
        response = self.s3_client.get_object(
            Bucket=self.bucket_name,
            Key=path
        )
        return response['Body'].read().decode('utf-8')

    def join_path(self, base: str, *parts: str) -> str:
        """Join S3 paths."""
        path = base.rstrip('/')
        for part in parts:
            path = f"{path}/{part.lstrip('/')}"
        return path

# Use the custom provider
s3_provider = S3FileSystemProvider(s3_client, 'my-bucket')
register_custom_tools(
    dbt_mcp,
    config_provider,
    exclude_tools,
    fs_provider=s3_provider
)
```

### Example: In-Memory File System (for Testing)

```python
from dbt_mcp.custom_tools.filesystem import FileSystemProvider

class InMemoryFileSystemProvider(FileSystemProvider):
    """In-memory file system for testing."""

    def __init__(self):
        self.files = {}

    def add_file(self, path: str, content: str):
        """Add a file to the in-memory filesystem."""
        self.files[path] = content

    def exists(self, path: str) -> bool:
        return path in self.files

    def read_text(self, path: str) -> str:
        if path not in self.files:
            raise FileNotFoundError(f"File not found: {path}")
        return self.files[path]

    def join_path(self, base: str, *parts: str) -> str:
        from pathlib import Path
        path = Path(base)
        for part in parts:
            path = path / part
        return str(path)

# Use in tests
fs = InMemoryFileSystemProvider()
fs.add_file("/project/models/tools/my_tool.sql", """
    SELECT * FROM {{ ref('users') }}
    WHERE status = '{{ var('status') }}'
""")

models = discover_tool_models(
    "/project",
    "models/tools",
    "dbt",
    parser,
    fs_provider=fs
)
```

## Architecture

### Components

1. **FileSystemProvider**: Abstract interface for file operations
   - `exists(path)`: Check if file exists
   - `read_text(path)`: Read file contents
   - `join_path(base, *parts)`: Join path components

2. **JinjaTemplateParser**: Parses Jinja templates to extract variables
   - Extracts `var()` calls from SQL templates
   - Supports default values
   - Marks variables as required/optional

3. **discover_tool_models()**: Discovers models from dbt project
   - Runs `dbt ls` to find models
   - Uses FileSystemProvider to read SQL files
   - Parses Jinja variables from templates

4. **register_custom_tools()**: Registers discovered models as MCP tools
   - Creates tool definitions with proper signatures
   - Handles parameter validation
   - Renders SQL with user-provided parameters

## Model Discovery Process

1. Run `dbt ls --select path:models/tools` to find models
2. Parse JSON output to get model metadata
3. Use FileSystemProvider to check if SQL file exists
4. Use FileSystemProvider to read SQL template
5. Parse Jinja variables from template
6. Create CustomToolModel with all metadata

## Example Tool Model

Create a model in `models/tools/customer_lookup.sql`:

```sql
{{
  config(
    materialized='ephemeral'
  )
}}

SELECT
  customer_id,
  customer_name,
  email,
  created_at
FROM {{ ref('customers') }}
WHERE customer_id = {{ var('customer_id') }}
{% if var('include_orders', false) %}
  AND EXISTS (
    SELECT 1
    FROM {{ ref('orders') }}
    WHERE orders.customer_id = customers.customer_id
  )
{% endif %}
```

This will automatically create a tool with:
- Name: `customer_lookup`
- Parameters: `customer_id` (required), `include_orders` (optional)
- Description: From model's dbt documentation

## Testing with Custom Providers

```python
import pytest
from dbt_mcp.custom_tools import discover_tool_models
from dbt_mcp.custom_tools.filesystem import FileSystemProvider

class MockFileSystemProvider(FileSystemProvider):
    def __init__(self, files: dict[str, str]):
        self.files = files

    def exists(self, path: str) -> bool:
        return path in self.files

    def read_text(self, path: str) -> str:
        return self.files[path]

    def join_path(self, base: str, *parts: str) -> str:
        return f"{base}/{'/'.join(parts)}"

def test_discover_models_with_mock_fs():
    fs = MockFileSystemProvider({
        "/project/models/tools/test.sql": "SELECT * FROM {{ ref('users') }}"
    })

    # Mock dbt ls output would be needed here
    models = discover_tool_models(
        "/project",
        "models/tools",
        "dbt",
        parser,
        fs_provider=fs
    )

    assert len(models) > 0
```

## Benefits of File System Abstraction

1. **Testability**: Mock file system for unit tests
2. **Flexibility**: Support cloud storage, remote APIs, etc.
3. **Portability**: Same code works across different environments
4. **Security**: Control file access through provider implementation
5. **Performance**: Implement caching in custom providers
