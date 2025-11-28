"""
Orchestrator: Main intelligent agent that coordinates specialized agents and tools.
Uses ReAct pattern to intelligently handle queries, including conversational questions.
"""

from langchain.agents import AgentExecutor, create_react_agent
from langchain.memory import ConversationBufferWindowMemory
from langchain_core.prompts import PromptTemplate
from langchain_core.tools import BaseTool
from langchain_core.callbacks import CallbackManagerForToolRun
from utils.llm_client import create_llm
from agents.data_agent import DataAnalysisAgent, create_data_analysis_agent_tool
from agents.viz_agent import VisualizationAgent, create_visualization_agent_tool, create_visualization_code_execution_tool
from processors.query_executor import QueryExecutor
from typing import Dict, Any, Optional
import json


class Orchestrator:
    """Intelligent orchestrator agent that coordinates specialized agents and tools."""
    
    def __init__(self, query_executor: QueryExecutor):
        self.llm = create_llm()
        
        # Initialize specialized agents
        self.data_agent = DataAnalysisAgent(query_executor)
        self.viz_agent = VisualizationAgent()
        
        # Track visualization spec from tool calls
        self.last_viz_spec = None
        
        # Create tools from specialized agents with tracking
        all_tools = [
            create_data_analysis_agent_tool(self.data_agent),
            self._create_tracking_viz_tool(),
            self._create_tracking_viz_code_tool()
        ]
        
        self.tools = all_tools
        
        # Create orchestrator prompt
        template = """You are an intelligent scientific paper data analysis orchestrator for Columbia University Computer Science research (2020-2024).

PRIMARY ROLE:
Intelligently coordinate data analysis and visualization workflows by understanding user queries, selecting appropriate specialized agents, and providing comprehensive answers.

CORE CAPABILITIES:
1. INTELLIGENT QUERY UNDERSTANDING: Understand both data queries and conversational questions
2. ADAPTIVE AGENT SELECTION: Choose the right specialized agent or tool based on query type
3. CONVERSATIONAL ANSWERS: Answer questions about trends, patterns, and insights (e.g., "why did paper count decrease?")
4. VISUALIZATION COORDINATION: Generate appropriate visualizations when needed
5. MULTI-STEP WORKFLOWS: Coordinate complex queries requiring multiple steps

AVAILABLE SPECIALIZED AGENTS AND TOOLS:

**DATA ANALYSIS AGENT:**
- query_paper_data: Query and analyze paper data (fields, years, citations, patents, authors, trends)
  Use when user asks about: paper counts, field statistics, citation patterns, top authors/fields, trends over time

**VISUALIZATION TOOLS:**
- generate_visualization: Generate standard Vega-Lite charts from analysis results
  Use when user asks to visualize data or create charts from query results
- execute_visualization_code: Execute custom Python code for advanced visualizations
  Use when standard charts are insufficient or custom visualization is needed

INTELLIGENT WORKFLOW GUIDELINES:

**For Data Queries:**
- User asks "show me top 5 fields" → Use query_paper_data with "top 5 research fields"
- User asks "papers by year 2020-2024" → Use query_paper_data with "papers by year from 2020 to 2024"
- User asks "citation patterns in 2023" → Use query_paper_data with "citation patterns in 2023"

**For Visualization Requests:**
- User asks "visualize the data" or "show me a chart" → First query data, then use generate_visualization
- User asks "create a custom visualization" → Use execute_visualization_code with custom code

**For Conversational Questions:**
- User asks "why did paper count decrease?" → First query data to get trends, then analyze and explain
- User asks "what's the trend in machine learning?" → Query data for ML field trends, analyze, and explain
- User asks "which field is growing fastest?" → Query data, compare trends, and provide insights

**For Multi-Step Queries:**
- User asks "show me top 5 fields and visualize them" → 
  1. Use query_paper_data to get top 5 fields
  2. Use generate_visualization to create chart
  3. Provide summary with insights

FLEXIBLE WORKFLOW PHILOSOPHY:
- NOT every query needs all agents - be selective and efficient
- ADAPT your approach based on what the user actually needs
- START with the most relevant agent for the specific question
- BUILD workflows dynamically based on query requirements
- PRIORITIZE understanding user intent over following rigid patterns
- ANSWER conversational questions intelligently using data insights

CONVERSATIONAL ANSWER EXAMPLES:
- "Why did paper count decrease?" → Query data for trends, identify decrease, explain possible reasons
- "What's happening with machine learning papers?" → Query ML field data, analyze trends, provide insights
- "Which field is most active?" → Query field statistics, identify top field, explain why

**IMPORTANT: CONTEXT AND MEMORY AWARENESS:**
- REMEMBER previous visualizations and analyses from the conversation history (chat_history)
- When user says "make this figure more beautiful", "improve the chart", "make it prettier", or similar phrases, they ALWAYS refer to the MOST RECENT visualization generated in this conversation
- CRITICAL: Look in the conversation history for previous tool calls to "generate_visualization" or "execute_visualization_code" - these contain the data and chart specifications you need
- To improve a visualization, you should:
  1. Search conversation history for the most recent visualization tool call result
  2. Extract the data and chart type from that result
  3. Use execute_visualization_code to create an enhanced version with:
     - Better colors and styling (use professional color palettes)
     - Improved layout and formatting
     - Professional appearance suitable for reports
     - Enhanced visual appeal
     - Better titles and labels
- ALWAYS check conversation history (chat_history) to understand what "this figure", "the chart", "it", or similar references mean
- The conversation history contains all previous tool calls and their results - use this information!
- When generating improved visualizations, make them publication-ready with:
  - Professional color schemes (avoid bright colors, use muted professional palettes)
  - Clear labels and titles
  - Appropriate sizing
  - Clean, modern aesthetics
  - Proper axis formatting

You have access to the following tools:

{tools}

Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
  - For query_paper_data: Use natural language query (e.g., "top 5 research fields" or "citation patterns in 2023")
  - For generate_visualization: Use JSON string with analysis results (e.g., '{{"success": true, "data": [...], "chart_type": "bar"}}')
  - For execute_visualization_code: Use JSON string with code and data (e.g., '{{"code": "...", "data": [...]}}')
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: Provide a comprehensive answer. If visualization was generated, mention it. If data was analyzed, provide insights and explanations.

Begin!


Question: {input}
Thought: {agent_scratchpad}"""

        prompt = PromptTemplate.from_template(template)
        
        agent = create_react_agent(self.llm, self.tools, prompt)
        
        memory = ConversationBufferWindowMemory(
            k=300,  # Increased from 10 to 15 to remember more context
            return_messages=True,
            memory_key="chat_history",
            input_key="input",  # Key for user input
            output_key="output"  # Key for agent output
        )
        
        self.agent_executor = AgentExecutor(
            agent=agent,
            tools=self.tools,
            memory=memory,
            verbose=True,
            handle_parsing_errors=True,
            max_iterations=20,
            max_execution_time=120
        )
    
    def _create_tracking_viz_tool(self):
        """Create a wrapper for visualization tool that tracks the spec."""
        original_tool = create_visualization_agent_tool(self.viz_agent)
        orchestrator_ref = self
        
        class TrackingVizTool(BaseTool):
            name: str = original_tool.name
            description: str = original_tool.description
            
            def _run(self, analysis_results_json: str, run_manager: Optional[CallbackManagerForToolRun] = None) -> str:
                result = original_tool._run(analysis_results_json, run_manager)
                # Extract and track spec
                try:
                    parsed = json.loads(result)
                    if parsed.get("success") and parsed.get("spec"):
                        orchestrator_ref.last_viz_spec = parsed["spec"]
                        print(f"[ORCHESTRATOR] Tracked visualization spec from generate_visualization tool")
                except Exception as e:
                    print(f"[ORCHESTRATOR] Error tracking viz spec: {str(e)}")
                return result
        
        return TrackingVizTool()
    
    def _create_tracking_viz_code_tool(self):
        """Create a wrapper for visualization code execution tool that tracks the spec."""
        original_tool = create_visualization_code_execution_tool(self.viz_agent)
        orchestrator_ref = self
        
        class TrackingVizCodeTool(BaseTool):
            name: str = original_tool.name
            description: str = original_tool.description
            
            def _run(self, input_json: str, run_manager: Optional[CallbackManagerForToolRun] = None) -> str:
                result = original_tool._run(input_json, run_manager)
                # Extract and track spec
                try:
                    parsed = json.loads(result)
                    if parsed.get("success") and parsed.get("spec"):
                        orchestrator_ref.last_viz_spec = parsed["spec"]
                        print(f"[ORCHESTRATOR] Tracked visualization spec from execute_visualization_code tool")
                except Exception as e:
                    print(f"[ORCHESTRATOR] Error tracking viz code spec: {str(e)}")
                return result
        
        return TrackingVizCodeTool()
    
    def process_query(self, user_query: str) -> Dict[str, Any]:
        """Process user query through the orchestrator agent."""
        improvement_keywords = ["beautiful", "pretty", "better", "improve", "enhance", "polish", "refine"]
        is_improvement_request = any(keyword in user_query.lower() for keyword in improvement_keywords)
        
        enhanced_query = user_query
        if is_improvement_request and self.last_viz_spec:
            viz_info = f"\n[CONTEXT: The most recent visualization generated was a {self.last_viz_spec.get('mark', {}).get('type', 'chart')} chart. You can find the full specification in the conversation history or use the last generated visualization data to create an improved version.]"
            enhanced_query = user_query + viz_info
        
        try:
            result = self.agent_executor.invoke({"input": enhanced_query})
            output = result.get("output", "")
            
            viz_spec = self.last_viz_spec
            if not viz_spec:
                viz_spec = self._extract_visualization_spec(output)
            
            analysis_result = self._extract_analysis_result(output)
            
            if not viz_spec and analysis_result:
                if "spec" in analysis_result:
                    viz_spec = analysis_result["spec"]
                elif analysis_result.get("data") and analysis_result.get("chart_type"):
                    viz_result = self.viz_agent.process(analysis_result)
                    if viz_result.get("success"):
                        viz_spec = viz_result.get("spec")
            
            if not is_improvement_request and viz_spec:
                self.last_viz_spec = viz_spec
            elif is_improvement_request and viz_spec:
                self.last_viz_spec = viz_spec
            
            return {
                "success": True,
                "message": output,
                "chart_spec": viz_spec,
                "stats": analysis_result.get("stats", {}) if analysis_result else {},
                "query_type": analysis_result.get("chart_type") if analysis_result else None
            }
        
        except Exception as e:
            import traceback
            return {
                "success": False,
                "message": "An error occurred processing your query",
                "error": str(e),
                "traceback": traceback.format_exc(),
                "chart_spec": None,
                "stats": {}
            }
    
    def _extract_visualization_spec(self, output: str) -> Optional[Dict[str, Any]]:
        """Extract Vega-Lite specification from output."""
        try:
            if '"$schema"' in output and 'vega-lite' in output:
                start = output.find('{', output.find('"$schema"'))
                if start != -1:
                    brace_count = 0
                    for i in range(start, len(output)):
                        if output[i] == '{':
                            brace_count += 1
                        elif output[i] == '}':
                            brace_count -= 1
                            if brace_count == 0:
                                json_str = output[start:i+1]
                                spec = json.loads(json_str)
                                if spec.get("$schema") and "vega-lite" in spec.get("$schema", ""):
                                    return spec
            
            if '"spec"' in output:
                import re
                json_pattern = r'\{[^{}]*"spec"[^{}]*\}'
                matches = re.finditer(json_pattern, output)
                for match in matches:
                    try:
                        start = output.rfind('{', 0, match.start())
                        if start != -1:
                            brace_count = 0
                            for i in range(start, len(output)):
                                if output[i] == '{':
                                    brace_count += 1
                                elif output[i] == '}':
                                    brace_count -= 1
                                    if brace_count == 0:
                                        full_json = output[start:i+1]
                                        result = json.loads(full_json)
                                        if "spec" in result and isinstance(result["spec"], dict):
                                            spec = result["spec"]
                                            if spec.get("$schema") and "vega-lite" in spec.get("$schema", ""):
                                                return spec
                    except:
                        continue
            
            json_objects = []
            start = 0
            while True:
                start = output.find('{', start)
                if start == -1:
                    break
                brace_count = 0
                for i in range(start, len(output)):
                    if output[i] == '{':
                        brace_count += 1
                    elif output[i] == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            try:
                                json_str = output[start:i+1]
                                obj = json.loads(json_str)
                                if isinstance(obj, dict) and "$schema" in obj:
                                    json_objects.append(obj)
                            except:
                                pass
                            break
                start += 1
            
            for obj in json_objects:
                if obj.get("$schema") and "vega-lite" in obj.get("$schema", ""):
                    return obj
                if "spec" in obj and isinstance(obj["spec"], dict):
                    spec = obj["spec"]
                    if spec.get("$schema") and "vega-lite" in spec.get("$schema", ""):
                        return spec
            
        except Exception:
            pass
        
        return None
    
    def _extract_analysis_result(self, output: str) -> Optional[Dict[str, Any]]:
        """Extract analysis result JSON from output."""
        try:
            if "{" in output and "}" in output:
                # Look for JSON with "success", "data", "stats" keys
                start = output.find("{")
                end = output.rfind("}") + 1
                json_str = output[start:end]
                result = json.loads(json_str)
                if result.get("success") and ("data" in result or "stats" in result):
                    return result
        except:
            pass
        return None
