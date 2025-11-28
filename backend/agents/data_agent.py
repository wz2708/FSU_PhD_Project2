"""
Data Analysis Agent: Specialized agent for querying and analyzing paper data.
Can be used as a tool by the orchestrator.
"""

from langchain.agents import AgentExecutor, create_react_agent
from langchain.memory import ConversationBufferWindowMemory
from langchain_core.prompts import PromptTemplate
from langchain_core.tools import BaseTool
from langchain_core.callbacks import CallbackManagerForToolRun
from utils.llm_client import create_llm
from agents.tools import DataAnalysisTools
from processors.query_executor import QueryExecutor
from typing import Dict, Any, Optional
import json


class DataAnalysisAgent:
    """Intelligent agent that uses ReAct pattern for data analysis."""
    
    def __init__(self, query_executor: QueryExecutor):
        self.llm = create_llm()
        self.tools_wrapper = DataAnalysisTools(query_executor)
        self.tools = self.tools_wrapper.get_tools()
        
        template = """You are a specialized data analysis agent for scientific paper database queries.

Your role is to:
1. Understand user queries about paper data
2. Select appropriate tools to query the database
3. Return results in JSON format with success status, data, stats, and chart_type

Available tools: {tools}

CRITICAL: All tools accept a JSON STRING as input. When calling a tool, the Action Input MUST be a valid JSON string.

Use the following format:
Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action (MUST be a JSON string, e.g., '{{"limit": 5}}' or '{{"year": 2023}}' or '{{"field": "machine learning", "start_year": 2020, "end_year": 2024}}')
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: Return the analysis result as JSON with this structure:
{{
    "success": true,
    "data": [...],
    "stats": {{...}},
    "chart_type": "bar" or "line" or "table"
}}

IMPORTANT EXAMPLES:
- For query_papers_by_field: Action Input should be '{{"limit": 5, "field_name": "machine learning"}}'
- For query_papers_by_year: Action Input should be '{{"year": 2023}}' or '{{"start_year": 2020, "end_year": 2024}}'
- For analyze_citation_patterns: Action Input should be '{{"year": 2023, "field": "machine learning"}}'
- For query_papers_advanced: Action Input should be '{{"year": 2023, "field": "ML", "limit": 20}}'

Begin!

Question: {input}
Thought: {agent_scratchpad}"""

        prompt = PromptTemplate.from_template(template)
        
        agent = create_react_agent(self.llm, self.tools, prompt)
        
        memory = ConversationBufferWindowMemory(
            k=5,
            return_messages=True,
            memory_key="chat_history"
        )
        
        self.agent_executor = AgentExecutor(
            agent=agent,
            tools=self.tools,
            memory=memory,
            verbose=True,
            handle_parsing_errors=True,
            max_iterations=15,
            max_execution_time=60
        )
    
    def invoke(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Invoke the agent (for use as a tool)."""
        user_query = inputs.get("input", "")
        result = self.agent_executor.invoke({"input": user_query})
        return result
    
    def process_query(self, user_query: str, conversation_id: Optional[str] = None) -> Dict[str, Any]:
        """Process user query using ReAct agent."""
        print(f"\n{'='*80}")
        print(f"[DATA_AGENT] process_query called")
        print(f"[DATA_AGENT] User query: {user_query}")
        
        try:
            print(f"[DATA_AGENT] Invoking agent_executor.invoke...")
            result = self.agent_executor.invoke({"input": user_query})
            print(f"[DATA_AGENT] Agent executor returned result type: {type(result)}")
            print(f"[DATA_AGENT] Result keys: {result.keys() if isinstance(result, dict) else 'Not a dict'}")
            
            output = result.get("output", "")
            print(f"[DATA_AGENT] Extracted output: {output[:500] if len(output) > 500 else output}")
            
            analysis_result = self._extract_analysis_result(output)
            print(f"[DATA_AGENT] Extracted analysis_result: {analysis_result}")
            
            if analysis_result and analysis_result.get("success"):
                return {
                    "success": True,
                    "message": self._generate_summary(analysis_result, user_query),
                    "analysis_result": analysis_result,
                    "raw_output": output
                }
            else:
                return {
                    "success": True,
                    "message": output,
                    "analysis_result": analysis_result,
                    "raw_output": output
                }
        
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            print(f"\n[DATA_AGENT ERROR] Exception in process_query:")
            print(f"[DATA_AGENT ERROR] Error type: {type(e).__name__}")
            print(f"[DATA_AGENT ERROR] Error message: {str(e)}")
            print(f"[DATA_AGENT ERROR] Full traceback:\n{error_trace}")
            return {
                "success": False,
                "message": f"Error processing query: {str(e)}",
                "error": str(e),
                "traceback": error_trace,
                "analysis_result": None
            }
    
    def _extract_analysis_result(self, output: str) -> Optional[Dict[str, Any]]:
        """Extract JSON analysis result from agent output."""
        try:
            if "{" in output and "}" in output:
                start = output.find("{")
                end = output.rfind("}") + 1
                json_str = output[start:end]
                result = json.loads(json_str)
                if result.get("success"):
                    return result
        except:
            pass
        return None
    
    def _generate_summary(self, analysis_result: Dict[str, Any], user_query: str) -> str:
        """Generate human-readable summary from analysis result."""
        stats = analysis_result.get("stats", {})
        chart_type = analysis_result.get("chart_type", "")
        
        if chart_type == "bar" and "total_fields" in stats:
            top_field = stats.get("top_field", {})
            return f"Found {stats.get('total_fields', 0)} research fields with {stats.get('total_papers', 0)} total papers. Top field: {top_field.get('display_name', 'N/A')} with {top_field.get('paper_count', 0)} papers."
        
        elif chart_type == "line" and "total_years" in stats:
            return f"Analyzed {stats.get('total_years', 0)} years with {stats.get('total_papers', 0)} total papers. Average: {stats.get('avg_per_year', 0):.1f} papers per year."
        
        elif chart_type == "bar" and "total_papers" in stats:
            return f"Patent distribution: {stats.get('total_papers', 0)} papers analyzed. {stats.get('papers_with_patents', 0)} papers have patents. Average: {stats.get('avg_patents', 0):.2f} patents per paper."
        
        return "Analysis complete. Data retrieved successfully."


def create_data_analysis_agent_tool(data_agent: DataAnalysisAgent) -> BaseTool:
    """Create a tool wrapper for DataAnalysisAgent."""
    
    class DataAnalysisAgentTool(BaseTool):
        """Tool wrapper for DataAnalysisAgent to be used by orchestrator."""
        
        name: str = "query_paper_data"
        description: str = """Query and analyze scientific paper data from Columbia University CS database (2020-2024).
        
Use this tool when the user asks about:
- Paper counts by field, year, citations, or patents
- Research field statistics and trends
- Citation patterns and distributions
- Top authors or fields
- Complex multi-field queries

Input: Natural language query about paper data (e.g., "show me top 5 research fields", "papers by year from 2020 to 2024")
Output: JSON with data, stats, and suggested chart type"""
        
        def _run(self, query: str, run_manager: Optional[CallbackManagerForToolRun] = None) -> str:
            """Execute data analysis query."""
            print(f"\n[DATA_AGENT_TOOL] Called with query: {query}")
            try:
                result = data_agent.process_query(query)
                output = result.get("raw_output", result.get("message", ""))
                
                # Try to extract JSON result
                analysis_result = result.get("analysis_result")
                if analysis_result:
                    return json.dumps(analysis_result, indent=2)
                else:
                    return output
            except Exception as e:
                import traceback
                return json.dumps({
                    "success": False,
                    "error": str(e),
                    "traceback": traceback.format_exc()
                })
    
    return DataAnalysisAgentTool()
