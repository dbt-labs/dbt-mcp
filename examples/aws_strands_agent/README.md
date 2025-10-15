# dbt AWS Agentcore Multi-Agent

A multi-agent system built with AWS Bedrock Agent Core that provides intelligent dbt project management and analysis capabilities. 

## Architecture

This project implements a multi-agent architecture with three specialized tools:

1. **dbt Compile Tool** - Local dbt compilation functionality
2. **dbt Model Analyzer** - Data model analysis and recommendations  
3. **dbt MCP Server Tool** - Remote dbt MCP server connection 

## 📋 Prerequisites

- Python 3.10+
- dbt CLI installed and configured
- dbt Fusion installed
- AWS Agentcore setup

## 🛠️ Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd dbt-aws-agent
   ```

2. **Create a virtual environment**:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

5. **Run**:
   ```bash
   cd dbt_data_scientist
   python agent.py
   ```

## Project Structure

```
dbt-aws-agent/
├── dbt_data_scientist/            # Main application package
│   ├── __init__.py               # Package initialization
│   ├── agent.py                  # Main agent with Bedrock Agent Core integration
│   ├── prompts.py                # Agent prompts and instructions
│   ├── test_all_tools.py         # Comprehensive test suite
│   ├── quick_mcp_test.py         # Quick MCP connectivity test
│   └── tools/                    # Tool implementations
│       ├── __init__.py
│       ├── dbt_compile.py        # Local dbt compilation tool
│       ├── dbt_mcp.py            # Remote dbt MCP server tool (translated from Google ADK)
│       └── dbt_model_analyzer.py # Data model analysis tool
├── requirements.txt              # Python dependencies
├── env.example                   # Environment configuration template
└── README.md                     # This documentation
```

## Tools Overview

### 1. dbt Compile Tool (`dbt_compile.py`)
- **Purpose**: Local dbt project compilation and troubleshooting
- **Features**:
  - Runs `dbt compile --log-format json` locally
  - Parses JSON logs for structured analysis
  - Provides compilation error analysis and recommendations
  - Routes to specialized dbt compile agent for intelligent responses

### 2. dbt Model Analyzer Tool (`dbt_model_analyzer.py`)
- **Purpose**: Data model analysis and recommendations
- **Features**:
  - Analyzes model structure and dependencies
  - Assesses data quality patterns and test coverage
  - Reviews adherence to dbt best practices
  - Provides optimization recommendations
  - Generates model documentation suggestions

### 3. dbt MCP Server Tool (`dbt_mcp.py`) 
- **Purpose**: Remote dbt MCP server connection using AWS Bedrock Agent Core
- **Features**:
  - Connects to remote dbt MCP server using streamable HTTP client
  - Supports dbt Cloud authentication with headers
  - Lists available MCP tools dynamically
  - Executes dbt MCP tool functions
  - Provides intelligent query routing to appropriate tools
  - Built-in connection testing and error handling
```

### 3. Test the Setup

Before running the full application, test that everything is working:

```bash
# Quick MCP test
python dbt_data_scientist/quick_mcp_test.py

# Full test suite
python dbt_data_scientist/test_all_tools.py
```

### 4. Run the Application

#### For AWS Bedrock Agent Core:
```bash
python -m dbt_data_scientist.agent
```

#### For Local Testing:
```bash
python -m dbt_data_scientist.agent
```

## Usage Examples

### dbt Compile Tool
```
> "Compile my dbt project and find any issues"
> "What's wrong with my models in the staging folder?"
```

### dbt Model Analyzer Tool
```
> "Analyze my data modeling approach for best practices"
> "Review the dependencies in my dbt project"
> "Check the data quality patterns in my models"
```

### dbt MCP Server Tool
```
> "List all available dbt MCP tools"
> "Show me the catalog from dbt Cloud"
> "Run my models in dbt Cloud"
> "What tests are available in my dbt project?"
```

## Testing

The project includes comprehensive testing capabilities to verify all components are working correctly.

### Quick Tests

#### Test MCP Connection Only
```bash
python dbt_data_scientist/quick_mcp_test.py
```
- Fast, minimal test of MCP connectivity
- Verifies environment variables and connection
- Lists available MCP tools

#### Test MCP Tool Directly
```bash
python dbt_data_scientist/tools/dbt_mcp.py
```
- Tests the MCP module directly
- Built-in connection testing
- Shows detailed error messages

### Comprehensive Testing

#### Full Test Suite
```bash
python dbt_data_scientist/test_all_tools.py
```
- Tests all tools individually
- Verifies agent initialization
- Tests tool integration
- Comprehensive error reporting

### What Tests Verify

1. **Environment Variables** - All required variables are set
2. **Tool Imports** - All tools can be imported successfully
3. **Agent Initialization** - Agent loads with all tools
4. **Individual Tool Testing** - Each tool executes correctly
5. **Agent Integration** - Tools work together in the agent
6. **MCP Connectivity** - Remote MCP server connection works

### Test Output Example
```
🚀 Complete Tool and Agent Test Suite
==================================================
🔧 Testing Environment Setup
------------------------------
  ✅ DBT_MCP_URL: https://your-mcp-server.com
  ✅ DBT_TOKEN: ****************
  ✅ DBT_USER_ID: your_user_id
  ✅ DBT_PROD_ENV_ID: your_env_id
  ✅ Environment setup complete!

📦 Testing Tool Imports
------------------------------
  ✅ All tools imported successfully
  ✅ dbt_compile is callable
  ✅ dbt_mcp_tool is callable
  ✅ dbt_model_analyzer_agent is callable

... (more tests)

🎉 All tests passed! Your agent and tools are working correctly.
```

## Key Features

### 🔄 **Intelligent Routing**
The main agent automatically routes queries to the appropriate specialized tool based on keywords and context.

### 🌐 **MCP Server Integration**
Seamless connection to remote dbt MCP servers with proper authentication and error handling.

### 📊 **Comprehensive Analysis**
Multi-faceted analysis including compilation, modeling best practices, and data quality assessment.

### ⚡ **Async Support**
Full async/await support for MCP operations while maintaining compatibility with Bedrock Agent Core.

### 🛡️ **Error Handling**
Robust error handling and fallback mechanisms for all tool operations.

## Development

### Adding New Tools
1. Create a new tool file in `dbt_data_scientist/tools/`
2. Use the `@tool` decorator from strands
3. Add the tool to the main agent's tools list in `agent.py`
4. Update the routing logic in the main agent's system prompt

## Troubleshooting

### Testing First

Before troubleshooting, run the test suite to identify issues:

```bash
# Quick test for MCP issues
python dbt_data_scientist/quick_mcp_test.py

# Comprehensive test for all issues
python dbt_data_scientist/test_all_tools.py
```

### Common Issues

1. **MCP Connection Failed**
   - Run `python dbt_data_scientist/quick_mcp_test.py` to diagnose
   - Verify `DBT_MCP_URL` is correct
   - Check authentication headers
   - Ensure dbt MCP server is accessible
   - Check network connectivity

2. **dbt Compile Errors**
   - Verify `DBT_PROJECT_LOCATION` path exists
   - Check `DBT_EXECUTABLE` is in PATH
   - Ensure dbt project is valid
   - Run `dbt compile` manually to test

3. **Environment Variable Issues**
   - Copy `env.example` to `.env`
   - Verify all required variables are set
   - Check variable values are correct
   - Use the test suite to validate configuration

4. **Agent Initialization Issues**
   - Check that all tools can be imported
   - Verify MCP server is accessible
   - Ensure all dependencies are installed
   - Run individual tool tests

### Debug Mode

For detailed debugging, you can run individual components:

```bash
# Test MCP tool directly
python dbt_data_scientist/tools/dbt_mcp.py

# Test individual tools
python -c "from dbt_data_scientist.tools import dbt_compile; print(dbt_compile('test'))"
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
