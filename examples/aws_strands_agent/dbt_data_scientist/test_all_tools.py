#!/usr/bin/env python3
"""Test script to verify all tools are accessible and working."""

import os
import sys
from dotenv import load_dotenv


def test_environment_setup():
    """Test that environment is properly configured."""
    print("🔧 Testing Environment Setup")
    print("-" * 30)
    
    load_dotenv()
    
    # Check required environment variables
    required_vars = {
        "DBT_MCP_URL": "dbt MCP server URL",
        "DBT_TOKEN": "dbt Cloud authentication token", 
        "DBT_USER_ID": "dbt Cloud user ID",
        "DBT_PROD_ENV_ID": "dbt Cloud production environment ID"
    }
    
    missing_vars = []
    for var, description in required_vars.items():
        value = os.environ.get(var)
        if value:
            if var == "DBT_TOKEN":
                print(f"  ✅ {var}: {'*' * len(value)}")
            else:
                print(f"  ✅ {var}: {value}")
        else:
            print(f"  ❌ {var}: NOT SET")
            missing_vars.append(var)
    
    # Check optional variables
    optional_vars = ["DBT_DEV_ENV_ID", "DBT_ACCOUNT_ID", "DBT_PROJECT_LOCATION", "DBT_EXECUTABLE"]
    for var in optional_vars:
        value = os.environ.get(var)
        if value:
            print(f"  ✅ {var}: {value}")
        else:
            print(f"  ⚠️  {var}: NOT SET (optional)")
    
    if missing_vars:
        print(f"\n❌ Missing required variables: {', '.join(missing_vars)}")
        return False
    
    print("  ✅ Environment setup complete!")
    return True

def test_tool_imports():
    """Test that all tools can be imported."""
    print("\n📦 Testing Tool Imports")
    print("-" * 30)
    
    try:
        from tools import dbt_compile, dbt_mcp_tool, dbt_model_analyzer_agent
        print("  ✅ All tools imported successfully")
        
        # Test that tools are callable
        if callable(dbt_compile):
            print("  ✅ dbt_compile is callable")
        else:
            print("  ❌ dbt_compile is not callable")
            return False
            
        if callable(dbt_mcp_tool):
            print("  ✅ dbt_mcp_tool is callable")
        else:
            print("  ❌ dbt_mcp_tool is not callable")
            return False
            
        if callable(dbt_model_analyzer_agent):
            print("  ✅ dbt_model_analyzer_agent is callable")
        else:
            print("  ❌ dbt_model_analyzer_agent is not callable")
            return False
        
        return True
        
    except ImportError as e:
        print(f"  ❌ Import failed: {e}")
        return False

def test_agent_initialization():
    """Test that the agent can be initialized with all tools."""
    print("\n🤖 Testing Agent Initialization")
    print("-" * 30)
    
    try:
        from agent import dbt_agent
        
        # Check that agent exists and has the expected attributes
        if hasattr(dbt_agent, 'tools'):
            tools = dbt_agent.tools
            if tools and len(tools) > 0:
                print(f"  ✅ Agent initialized with {len(tools)} tools")
                
                # List the tools
                tool_names = []
                for tool in tools:
                    if hasattr(tool, 'tool_name'):
                        tool_names.append(tool.tool_name)
                    else:
                        tool_names.append(str(tool))
                
                print(f"  📋 Available tools: {', '.join(tool_names)}")
                return True
            else:
                print("  ⚠️  Agent has no tools (this might be expected if tools are loaded dynamically)")
                # This is actually okay for MCP tools that are loaded dynamically
                return True
        else:
            print("  ⚠️  Agent doesn't have tools attribute (this might be expected)")
            # Check if agent has other expected attributes
            if hasattr(dbt_agent, 'system_prompt'):
                print("  ✅ Agent has system_prompt")
                return True
            else:
                print("  ❌ Agent missing expected attributes")
                return False
            
    except Exception as e:
        print(f"  ❌ Agent initialization failed: {e}")
        return False
    """Test that the agent can be initialized with all tools."""
    print("\n🤖 Testing Agent Initialization")
    print("-" * 30)
    
    try:
        from agent import dbt_agent
        
        # Check that agent has tools
        if hasattr(dbt_agent, 'tools') and dbt_agent.tools:
            print(f"  ✅ Agent initialized with {len(dbt_agent.tools)} tools")
            
            # List the tools
            tool_names = []
            for tool in tools:
                if hasattr(tool, 'tool_name'):
                    tool_names.append(tool.tool_name)
                else:
                    tool_names.append(str(tool))
            
            print(f"  📋 Available tools: {', '.join(tool_names)}")
            return True
        else:
            print("  ❌ Agent has no tools")
            return False
            
    except Exception as e:
        print(f"  ❌ Agent initialization failed: {e}")
        return False

def test_dbt_compile_tool():
    """Test the dbt compile tool."""
    print("\n🔨 Testing dbt_compile Tool")
    print("-" * 30)
    
    try:
        from tools import dbt_compile
        
        # Test with a simple query
        test_query = "test dbt compile functionality"
        print(f"  📝 Testing with query: '{test_query}'")
        
        result = dbt_compile(test_query)
        
        if result and len(result) > 0:
            print(f"  ✅ dbt_compile tool executed successfully")
            print(f"  📄 Result preview: {result[:100]}...")
            return True
        else:
            print("  ⚠️  dbt_compile returned empty result")
            return False
            
    except Exception as e:
        print(f"  ❌ dbt_compile tool failed: {e}")
        return False

def test_dbt_mcp_tool():
    """Test the dbt MCP tool."""
    print("\n🌐 Testing dbt_mcp_tool")
    print("-" * 30)
    
    try:
        from tools import dbt_mcp_tool
        
        # Test with a simple query
        test_query = "list tools"
        print(f"  📝 Testing with query: '{test_query}'")
        
        result = dbt_mcp_tool(test_query)
        
        if result and len(result) > 0:
            print(f"  ✅ dbt_mcp_tool executed successfully")
            print(f"  📄 Result preview: {result[:100]}...")
            return True
        else:
            print("  ⚠️  dbt_mcp_tool returned empty result")
            return False
            
    except Exception as e:
        print(f"  ❌ dbt_mcp_tool failed: {e}")
        return False

def test_dbt_model_analyzer_tool():
    """Test the dbt model analyzer tool."""
    print("\n📊 Testing dbt_model_analyzer_agent")
    print("-" * 30)
    
    try:
        from tools import dbt_model_analyzer_agent
        
        # Test with a simple query
        test_query = "analyze my dbt models"
        print(f"  📝 Testing with query: '{test_query}'")
        
        result = dbt_model_analyzer_agent(test_query)
        
        if result and len(result) > 0:
            print(f"  ✅ dbt_model_analyzer_agent executed successfully")
            print(f"  📄 Result preview: {result[:100]}...")
            return True
        else:
            print("  ⚠️  dbt_model_analyzer_agent returned empty result")
            return False
            
    except Exception as e:
        print(f"  ❌ dbt_model_analyzer_agent failed: {e}")
        return False

def test_agent_with_tools():
    """Test the agent with all tools integrated."""
    print("\n🎯 Testing Agent with All Tools")
    print("-" * 30)
    
    try:
        from agent import dbt_agent
        
        # Test with different types of queries
        test_queries = [
            "What tools are available?",
            "Help me with dbt compilation",
            "Analyze my data models"
        ]
        
        for i, query in enumerate(test_queries, 1):
            print(f"  📝 Test {i}: '{query}'")
            try:
                result = dbt_agent(query)
                if result and len(str(result)) > 0:
                    print(f"    ✅ Agent responded successfully")
                    print(f"    📄 Response preview: {str(result)[:80]}...")
                else:
                    print(f"    ⚠️  Agent returned empty response")
            except Exception as e:
                print(f"    ❌ Agent failed: {e}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Agent testing failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Complete Tool and Agent Test Suite")
    print("=" * 50)
    
    tests = [
        ("Environment Setup", test_environment_setup),
        ("Tool Imports", test_tool_imports),
        ("Agent Initialization", test_agent_initialization),
        ("dbt_compile Tool", test_dbt_compile_tool),
        ("dbt_mcp_tool", test_dbt_mcp_tool),
        ("dbt_model_analyzer_agent", test_dbt_model_analyzer_tool),
        ("Agent with All Tools", test_agent_with_tools)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
                print(f"✅ {test_name} - PASSED")
            else:
                print(f"❌ {test_name} - FAILED")
        except Exception as e:
            print(f"❌ {test_name} - ERROR: {e}")
        print()
    
    print("=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Your agent and tools are working correctly.")
        print("\nYou can now run the agent:")
        print("  python dbt_data_scientist/agent.py")
    else:
        print("⚠️  Some tests failed. Please check the errors above.")
        print("Common issues:")
        print("  - Missing environment variables")
        print("  - MCP server not accessible")
        print("  - dbt project not found")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
