"""dbt Model Analyzer Tool - Data model analysis and recommendations."""

import os
import json
import subprocess
from typing import Dict, Any, List, Optional
from strands import Agent, tool
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DBT_MODEL_ANALYZER_SYSTEM_PROMPT = """
You are a dbt data modeling expert and analyst. Your capabilities include:

1. **Model Structure Analysis**: Analyze dbt model structure, dependencies, and relationships
2. **Data Quality Assessment**: Evaluate data quality patterns, test coverage, and validation rules
3. **Performance Optimization**: Identify performance bottlenecks and optimization opportunities
4. **Best Practices Review**: Check adherence to dbt best practices and naming conventions
5. **Dependency Analysis**: Map model dependencies and identify circular dependencies or issues
6. **Documentation Review**: Assess model documentation completeness and quality

When analyzing models, provide:
- Clear summary of findings
- Specific recommendations for improvement
- Priority levels for each recommendation
- Code examples where applicable
- Impact assessment for suggested changes

Focus on actionable insights that help improve data modeling practices and model quality.
"""

@tool
def dbt_model_analyzer_agent(query: str) -> str:
    """
    Analyzes dbt models and provides recommendations for data modeling improvements.
    
    This tool can:
    - Analyze model structure and dependencies
    - Assess data quality patterns and test coverage
    - Review adherence to dbt best practices
    - Provide optimization recommendations
    - Generate model documentation suggestions
    
    Args:
        query: The user's question about data modeling analysis or specific model to analyze
        
    Returns:
        String response with analysis results and recommendations
    """
    try:
        # Load environment variables
        load_dotenv()
        
        # Get dbt project location
        dbt_project_location = os.getenv("DBT_PROJECT_LOCATION", os.getcwd())
        dbt_executable = os.getenv("DBT_EXECUTABLE", "dbt")
        
        print(f"Analyzing dbt models in: {dbt_project_location}")
        
        # Parse the query to determine analysis type
        query_lower = query.lower()
        
        # Determine what type of analysis to perform
        analysis_type = "comprehensive"
        if "dependency" in query_lower:
            analysis_type = "dependencies"
        elif "quality" in query_lower or "test" in query_lower:
            analysis_type = "data_quality"
        elif "performance" in query_lower or "optimize" in query_lower:
            analysis_type = "performance"
        elif "documentation" in query_lower or "docs" in query_lower:
            analysis_type = "documentation"
        elif "best practice" in query_lower or "convention" in query_lower:
            analysis_type = "best_practices"
        
        # Gather dbt project information
        project_info = gather_dbt_project_info(dbt_project_location, dbt_executable)
        
        # Format the analysis query
        formatted_query = f"""
        User wants to analyze their dbt data modeling approach. Analysis type: {analysis_type}
        
        Project information:
        - Project location: {dbt_project_location}
        - Models count: {project_info.get('models_count', 'Unknown')}
        - Tests count: {project_info.get('tests_count', 'Unknown')}
        - Sources count: {project_info.get('sources_count', 'Unknown')}
        
        User's specific question: {query}
        
        Please provide a comprehensive analysis focusing on {analysis_type} and give actionable recommendations.
        """
        
        # Create the model analyzer agent
        model_analyzer_agent = Agent(
            system_prompt=DBT_MODEL_ANALYZER_SYSTEM_PROMPT,
            tools=[],
        )
        
        # Get analysis from the agent
        agent_response = model_analyzer_agent(formatted_query)
        text_response = str(agent_response)
        
        if len(text_response) > 0:
            return text_response
        
        return "I apologize, but I couldn't process your dbt model analysis request. Please try rephrasing or providing more specific details about what you'd like to analyze."
    
    except Exception as e:
        return f"Error processing your dbt model analysis query: {str(e)}"

def gather_dbt_project_info(project_location: str, dbt_executable: str) -> Dict[str, Any]:
    """
    Gather basic information about the dbt project.
    
    Args:
        project_location: Path to dbt project
        dbt_executable: Path to dbt executable
        
    Returns:
        Dictionary with project information
    """
    info = {
        "models_count": 0,
        "tests_count": 0,
        "sources_count": 0,
        "project_name": "Unknown"
    }
    
    try:
        # Try to get project name from dbt_project.yml
        dbt_project_file = os.path.join(project_location, "dbt_project.yml")
        if os.path.exists(dbt_project_file):
            with open(dbt_project_file, 'r') as f:
                content = f.read()
                if 'name:' in content:
                    # Simple extraction of project name
                    for line in content.split('\n'):
                        if line.strip().startswith('name:'):
                            info["project_name"] = line.split(':')[1].strip().strip('"\'')
                            break
        
        # Try to count models, tests, and sources by running dbt commands
        try:
            # List models
            result = subprocess.run(
                [dbt_executable, "list", "--resource-type", "model"],
                cwd=project_location,
                text=True,
                capture_output=True,
                timeout=30
            )
            if result.returncode == 0:
                info["models_count"] = len([line for line in result.stdout.split('\n') if line.strip()])
        except:
            pass
        
        try:
            # List tests
            result = subprocess.run(
                [dbt_executable, "list", "--resource-type", "test"],
                cwd=project_location,
                text=True,
                capture_output=True,
                timeout=30
            )
            if result.returncode == 0:
                info["tests_count"] = len([line for line in result.stdout.split('\n') if line.strip()])
        except:
            pass
        
        try:
            # List sources
            result = subprocess.run(
                [dbt_executable, "list", "--resource-type", "source"],
                cwd=project_location,
                text=True,
                capture_output=True,
                timeout=30
            )
            if result.returncode == 0:
                info["sources_count"] = len([line for line in result.stdout.split('\n') if line.strip()])
        except:
            pass
            
    except Exception as e:
        print(f"Error gathering project info: {e}")
    
    return info
