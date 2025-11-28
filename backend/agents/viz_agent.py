"""
Visualization Agent: Generates visualizations from analysis results.
Can be used as a tool by the orchestrator, with code execution capability.
"""

from typing import Dict, Any, List, Optional
from langchain_core.tools import BaseTool
from langchain_core.callbacks import CallbackManagerForToolRun
from utils.llm_client import create_llm, create_prompt_template
from utils.vega_spec_generator import create_bar_chart, create_line_chart, create_histogram
import json
import io
import sys
from contextlib import redirect_stdout, redirect_stderr


class VisualizationAgent:
    """Agent that generates Vega-Lite chart specifications and can execute visualization code."""
    
    def __init__(self):
        self.llm = create_llm()
        self.prompt_template = create_prompt_template("""
You are a data visualization agent. Given analysis results, generate an appropriate Vega-Lite chart specification.

Analysis results:
{analysis_results}

Chart type suggested: {chart_type}

Generate a Vega-Lite v5 JSON specification. The chart should:
1. Be appropriate for the data type and analysis
2. Have clear titles and labels
3. Use appropriate colors
4. Include interactive features if possible

Return ONLY the JSON specification, no other text. Use this structure:
{{
    "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
    "description": "chart description",
    "data": {{"values": [...]}},
    "mark": "...",
    "encoding": {{...}},
    "width": 600,
    "height": 400
}}

Response (JSON only):
""")
    
    def process(self, analysis_results: Dict[str, Any], filter_description: str = "") -> Dict[str, Any]:
        """Generate Vega-Lite specification from analysis results."""
        if not analysis_results.get("success"):
            return {
                "success": False,
                "error": analysis_results.get("error", "Analysis failed"),
                "spec": None
            }
        
        data = analysis_results.get("data", [])
        chart_type = analysis_results.get("chart_type", "bar")
        stats = analysis_results.get("stats", {})
        
        if not data:
            return {
                "success": False,
                "error": "No data to visualize",
                "spec": None
            }
        
        try:
            if chart_type == "bar":
                if "fieldid" in data[0] or "display_name" in data[0]:
                    x_field = "display_name" if "display_name" in data[0] else "fieldid"
                    y_field = "paper_count"
                    title = f"Papers by Field - {filter_description}" if filter_description else "Papers by Field"
                    spec = create_bar_chart(data, x_field, y_field, title, "#4A90E2")
                elif "patent_count" in data[0]:
                    x_field = "patent_count"
                    y_field = "paper_count"
                    title = f"Patent Citation Distribution - {filter_description}" if filter_description else "Patent Citation Distribution"
                    spec = create_bar_chart(data, x_field, y_field, title, "#50C878")
                else:
                    spec = self._generate_with_llm(analysis_results, chart_type, filter_description)
            elif chart_type == "line":
                x_field = "year"
                y_field = "count"
                title = f"Papers by Year - {filter_description}" if filter_description else "Papers by Year"
                spec = create_line_chart(data, x_field, y_field, title)
            else:
                spec = self._generate_with_llm(analysis_results, chart_type, filter_description)
            
            return {
                "success": True,
                "spec": spec,
                "stats": stats
            }
        
        except Exception as e:
            return {
                "success": False,
                "error": f"Visualization generation failed: {str(e)}",
                "spec": None
            }
    
    def _generate_with_llm(self, analysis_results: Dict[str, Any], chart_type: str, filter_description: str) -> Dict:
        """Use LLM to generate chart specification for complex cases."""
        try:
            chain = self.prompt_template | self.llm
            response = chain.invoke({
                "analysis_results": json.dumps(analysis_results, indent=2),
                "chart_type": chart_type
            })
            
            content = response.content.strip()
            if content.startswith("```json"):
                content = content[7:]
            if content.startswith("```"):
                content = content[3:]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()
            
            spec = json.loads(content)
            return spec
        except Exception as e:
            return {
                "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                "description": "Chart",
                "data": {"values": analysis_results.get("data", [])},
                "mark": "bar",
                "encoding": {
                    "x": {"field": "x", "type": "nominal"},
                    "y": {"field": "y", "type": "quantitative"}
                }
            }
    
    def execute_visualization_code(self, code: str, data: List[Dict]) -> Dict[str, Any]:
        """Execute Python code to generate custom visualizations."""
        print(f"\n[VIZ_AGENT] Executing visualization code")
        print(f"[VIZ_AGENT] Code length: {len(code)}")
        print(f"[VIZ_AGENT] Data points: {len(data)}")
        
        try:
            # Create execution namespace
            exec_namespace = {
                '__builtins__': __builtins__,
                'data': data,
                'json': json,
                'pd': None,
                'plt': None,
                'np': None,
                'vega_spec': None
            }
            
            # Try to import common libraries
            try:
                import pandas as pd
                exec_namespace['pd'] = pd
            except ImportError:
                pass
            
            try:
                import matplotlib.pyplot as plt
                exec_namespace['plt'] = plt
            except ImportError:
                pass
            
            try:
                import numpy as np
                exec_namespace['np'] = np
            except ImportError:
                pass
            
            # Capture output
            stdout_capture = io.StringIO()
            stderr_capture = io.StringIO()
            
            with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                exec(code, exec_namespace)
            
            stdout_output = stdout_capture.getvalue()
            stderr_output = stderr_capture.getvalue()
            
            # Check if vega_spec was created
            vega_spec = exec_namespace.get('vega_spec')
            
            if vega_spec:
                return {
                    "success": True,
                    "spec": vega_spec,
                    "stdout": stdout_output,
                    "stderr": stderr_output
                }
            else:
                return {
                    "success": False,
                    "error": "Code executed but did not create 'vega_spec' variable",
                    "stdout": stdout_output,
                    "stderr": stderr_output
                }
        
        except Exception as e:
            import traceback
            return {
                "success": False,
                "error": str(e),
                "traceback": traceback.format_exc()
            }


def create_visualization_agent_tool(viz_agent: VisualizationAgent) -> BaseTool:
    """Create a tool wrapper for VisualizationAgent."""
    
    class VisualizationAgentTool(BaseTool):
        """Tool wrapper for VisualizationAgent to be used by orchestrator."""
        
        name: str = "generate_visualization"
        description: str = """Generate visualizations from analysis data.
        
Use this tool when:
- User asks to visualize data or create charts
- Analysis results need to be displayed graphically
- Custom visualization is needed

Input: JSON string with analysis results containing 'data', 'stats', and 'chart_type' fields
Output: Vega-Lite JSON specification for the chart"""
        
        def _run(self, analysis_results_json: str, run_manager: Optional[CallbackManagerForToolRun] = None) -> str:
            """Generate visualization from analysis results."""
            print(f"\n[VIZ_AGENT_TOOL] Called")
            print(f"[VIZ_AGENT_TOOL] Input type: {type(analysis_results_json)}")
            print(f"[VIZ_AGENT_TOOL] Input (first 500 chars): {str(analysis_results_json)[:500]}")
            
            try:
                # Parse input
                if isinstance(analysis_results_json, str):
                    analysis_results = json.loads(analysis_results_json)
                else:
                    analysis_results = analysis_results_json
                
                result = viz_agent.process(analysis_results)
                
                if result.get("success"):
                    return json.dumps({
                        "success": True,
                        "spec": result.get("spec"),
                        "stats": result.get("stats")
                    }, indent=2)
                else:
                    return json.dumps(result, indent=2)
            except Exception as e:
                import traceback
                return json.dumps({
                    "success": False,
                    "error": str(e),
                    "traceback": traceback.format_exc()
                }, indent=2)
    
    return VisualizationAgentTool()


def create_visualization_code_execution_tool(viz_agent: VisualizationAgent) -> BaseTool:
    """Create a tool for executing custom Python code to generate visualizations."""
    
    class VisualizationCodeExecutionTool(BaseTool):
        """Tool for executing custom Python code to generate visualizations."""
        
        name: str = "execute_visualization_code"
        description: str = """Execute custom Python code to generate advanced visualizations.
        
Use this when:
- Standard Vega-Lite charts are not sufficient
- User requests custom visualization types
- Complex data transformations are needed before visualization

Input: JSON string with 'code' (Python code) and 'data' (list of dicts) fields.
The code should create a 'vega_spec' variable with Vega-Lite JSON specification.

Example input: '{{"code": "import pandas as pd\\ndf = pd.DataFrame(data)\\nvega_spec = {{...}}", "data": [...]}}'
Output: Vega-Lite JSON specification"""
        
        def _run(self, input_json: str, run_manager: Optional[CallbackManagerForToolRun] = None) -> str:
            """Execute visualization code."""
            print(f"\n[VIZ_CODE_TOOL] Called")
            print(f"[VIZ_CODE_TOOL] Input: {input_json[:500]}")
            
            try:
                # Parse input
                if isinstance(input_json, str):
                    params = json.loads(input_json)
                else:
                    params = input_json
                
                code = params.get('code', '')
                data = params.get('data', [])
                
                result = viz_agent.execute_visualization_code(code, data)
                return json.dumps(result, indent=2)
            except Exception as e:
                import traceback
                return json.dumps({
                    "success": False,
                    "error": str(e),
                    "traceback": traceback.format_exc()
                }, indent=2)
    
    return VisualizationCodeExecutionTool()
