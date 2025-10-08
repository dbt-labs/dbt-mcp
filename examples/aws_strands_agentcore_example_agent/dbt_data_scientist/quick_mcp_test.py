#!/usr/bin/env python3
"""Quick MCP connection test - minimal version."""

import os
import sys
from dotenv import load_dotenv

def quick_test():
    """Quick test of MCP connectivity."""
    print("🧪 Quick MCP Connection Test")
    print("-" * 30)
    
    # Load environment
    load_dotenv()
    
    # Check basic env vars
    url = os.environ.get("DBT_MCP_URL")
    token = os.environ.get("DBT_TOKEN")
    
    if not url or not token:
        print("❌ Missing DBT_MCP_URL or DBT_TOKEN")
        return False
    
    print(f"✅ URL: {url}")
    print(f"✅ Token: {'*' * len(token)}")
    
    try:
        # Import and test
        from tools.dbt_mcp import dbt_mcp_client
        
        print("🔌 Testing connection...")
        with dbt_mcp_client:
            tools = dbt_mcp_client.list_tools_sync()
            print(f"✅ Connected! Found {len(tools)} tools")
            
            if tools:
                print("📋 Available tools:")
                for tool in tools[:5]:  # Show first 3 tools
                    print(f"  - {tool.tool_name}")
                if len(tools) > 3:
                    print(f"  ... and {len(tools) - 3} more")
        
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    success = quick_test()
    if success:
        print("\n🎉 MCP connection is working!")
    else:
        print("\n💥 MCP connection failed!")
    sys.exit(0 if success else 1)
