from dbt_mcp.config.config import load_config
from dbt_mcp.mcp.server import create_dbt_mcp

config = load_config()
app = create_dbt_mcp(config)
