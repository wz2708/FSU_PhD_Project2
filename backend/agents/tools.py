"""
Comprehensive tools for LangChain Agent to interact with the database.
"""

from langchain_core.tools import tool, StructuredTool, BaseTool
from pydantic import BaseModel, Field
from processors.query_executor import QueryExecutor
from typing import Optional, Dict, Any, List
import pandas as pd
import json




class DataAnalysisTools:
    """Wrapper class to hold query executor and provide comprehensive tools."""
    
    def __init__(self, query_executor: QueryExecutor):
        self.executor = query_executor
    
    def get_tools(self):
        """Return list of all tools for LangChain Agent."""
        executor = self.executor
        
        @tool
        def query_papers_by_field(tool_input: str) -> str:
            """
            Get paper count grouped by research field.
            
            Args:
                tool_input: JSON string with parameters. Example: '{"limit": 5, "field_name": "machine learning"}'
            
            Returns:
                JSON string with field data and statistics
            """
            # Parse JSON string
            try:
                if isinstance(tool_input, str):
                    params = json.loads(tool_input)
                elif isinstance(tool_input, dict):
                    params = tool_input
                else:
                    params = {}
                
                limit = params.get('limit')
                field_name = params.get('field_name')
            except json.JSONDecodeError as e:
                return json.dumps({
                    "success": False,
                    "error": f"Invalid JSON input: {str(e)}"
                })
            
            try:
                result_df = executor.get_papers_by_field(limit=limit, field_name=field_name)
                data = result_df.to_dict('records')
                
                stats = {
                    "total_fields": len(result_df),
                    "total_papers": int(result_df['paper_count'].sum()),
                    "top_field": result_df.iloc[0].to_dict() if len(result_df) > 0 else None
                }
                
                result_json = json.dumps({
                    "success": True,
                    "data": data,
                    "stats": stats,
                    "chart_type": "bar"
                }, indent=2)
                return result_json
            except Exception as e:
                import traceback
                error_trace = traceback.format_exc()
                return json.dumps({
                    "success": False,
                    "error": str(e),
                    "traceback": error_trace
                })
        
        @tool
        def query_papers_by_year(tool_input: str) -> str:
            """
            Get paper count grouped by year.
            
            Args:
                tool_input: JSON string with parameters. Example: '{"year": 2023}' or '{"start_year": 2020, "end_year": 2024}'
            
            Returns:
                JSON string with year data and statistics
            """
            # Parse JSON string
            try:
                if isinstance(tool_input, str):
                    params = json.loads(tool_input)
                elif isinstance(tool_input, dict):
                    params = tool_input
                else:
                    params = {}
                
                year = params.get('year')
                start_year = params.get('start_year')
                end_year = params.get('end_year')
                years = params.get('years')
            except json.JSONDecodeError as e:
                return json.dumps({
                    "success": False,
                    "error": f"Invalid JSON input: {str(e)}"
                })
            
            try:
                result_df = executor.get_papers_by_year(
                    year=year, start_year=start_year, end_year=end_year, years=years
                )
                data = result_df.to_dict('records')
                stats = {
                    "total_years": len(result_df),
                    "total_papers": int(result_df['count'].sum()),
                    "avg_per_year": float(result_df['count'].mean()) if len(result_df) > 0 else 0,
                    "max_year": result_df.loc[result_df['count'].idxmax()].to_dict() if len(result_df) > 0 else None
                }
                
                return json.dumps({
                    "success": True,
                    "data": data,
                    "stats": stats,
                    "chart_type": "line"
                }, indent=2)
            except Exception as e:
                import traceback
                error_trace = traceback.format_exc()
                return json.dumps({
                    "success": False,
                    "error": str(e),
                    "traceback": error_trace
                })
        
        @tool
        def query_papers_by_citations(tool_input: str) -> str:
            """
            Get papers filtered by citation count.
            
            Args:
                tool_input: JSON string with parameters. Example: '{"min_citations": 10, "year": 2023, "field": "machine learning"}'
            
            Returns:
                JSON string with paper data
            """
            # Parse JSON string
            try:
                if isinstance(tool_input, str):
                    params = json.loads(tool_input)
                elif isinstance(tool_input, dict):
                    params = tool_input
                else:
                    params = {}
                
                min_citations = params.get('min_citations')
                max_citations = params.get('max_citations')
                year = params.get('year')
                field = params.get('field')
            except json.JSONDecodeError as e:
                return json.dumps({
                    "success": False,
                    "error": f"Invalid JSON input: {str(e)}"
                })
            
            try:
                result_df = executor.get_papers_by_citations(
                    min_citations=min_citations, max_citations=max_citations,
                    year=year, field=field
                )
                data = result_df.head(100).to_dict('records')
                stats = {
                    "total_papers": len(result_df),
                    "avg_citations": float(result_df['cited_by_count'].mean()) if len(result_df) > 0 else 0,
                    "max_citations": int(result_df['cited_by_count'].max()) if len(result_df) > 0 else 0
                }
                return json.dumps({
                    "success": True,
                    "data": data,
                    "stats": stats,
                    "chart_type": "table"
                }, indent=2)
            except Exception as e:
                import traceback
                error_trace = traceback.format_exc()
                return json.dumps({
                    "success": False,
                    "error": str(e),
                    "traceback": error_trace
                })
        
        @tool
        def query_papers_by_patents(tool_input: str) -> str:
            """
            Get papers filtered by patent count.
            
            Args:
                tool_input: JSON string with parameters. Example: '{"min_patents": 1, "has_patents": true, "year": 2023}'
            
            Returns:
                JSON string with paper data
            """
            # Parse JSON string
            try:
                if isinstance(tool_input, str):
                    params = json.loads(tool_input)
                elif isinstance(tool_input, dict):
                    params = tool_input
                else:
                    params = {}
                
                min_patents = params.get('min_patents')
                has_patents = params.get('has_patents')
                year = params.get('year')
            except json.JSONDecodeError as e:
                return json.dumps({
                    "success": False,
                    "error": f"Invalid JSON input: {str(e)}"
                })
            
            try:
                result_df = executor.get_papers_by_patents(
                    min_patents=min_patents, has_patents=has_patents, year=year
                )
                data = result_df.head(100).to_dict('records')
                stats = {
                    "total_papers": len(result_df),
                    "papers_with_patents": len(result_df[result_df['actual_patent_count'] > 0]) if 'actual_patent_count' in result_df.columns else 0,
                    "avg_patents": float(result_df['actual_patent_count'].mean()) if len(result_df) > 0 and 'actual_patent_count' in result_df.columns else 0
                }
                return json.dumps({
                    "success": True,
                    "data": data,
                    "stats": stats,
                    "chart_type": "table"
                }, indent=2)
            except Exception as e:
                import traceback
                error_trace = traceback.format_exc()
                return json.dumps({
                    "success": False,
                    "error": str(e),
                    "traceback": error_trace
                })
        
        @tool
        def query_papers_advanced(tool_input: str) -> str:
            """
            Advanced query with multiple field filters.
            
            Args:
                tool_input: JSON string with filter parameters. Example: '{"year": 2023, "field": "machine learning", "min_citations": 10, "limit": 20}'
                    - year: Specific year
                    - start_year, end_year: Year range
                    - field, fields: Field name(s) to filter
                    - min_citations, max_citations: Citation range
                    - min_patents: Minimum patent count
                    - has_patents: Boolean for papers with patents
                    - limit: Maximum number of results
            
            Returns:
                JSON string with paper data
            """
            # Parse JSON string
            try:
                if isinstance(tool_input, str):
                    # Remove any extra quotes or newlines
                    tool_input = tool_input.strip().strip("'\"")
                    filters = json.loads(tool_input)
                elif isinstance(tool_input, dict):
                    filters = tool_input
                else:
                    filters = {}
                
            except json.JSONDecodeError as e:
                return json.dumps({
                    "success": False,
                    "error": f"Invalid JSON input: {str(e)}"
                })
            
            try:
                result_df = executor.get_papers_advanced(filters=filters)
                data = result_df.head(100).to_dict('records')
                stats = {
                    "total_papers": len(result_df),
                    "sample_size": len(data)
                }
                return json.dumps({
                    "success": True,
                    "data": data,
                    "stats": stats,
                    "chart_type": "table"
                }, indent=2)
            except Exception as e:
                import traceback
                error_trace = traceback.format_exc()
                return json.dumps({
                    "success": False,
                    "error": str(e),
                    "traceback": error_trace
                })
        
        @tool
        def explore_available_fields() -> str:
            """
            List all available research fields with paper counts.
            
            Returns:
                JSON string with field information
            """
            try:
                result_df = executor.get_available_fields()
                data = result_df.to_dict('records')
                stats = {
                    "total_fields": len(result_df),
                    "total_papers": int(result_df['paper_count'].sum())
                }
                return json.dumps({
                    "success": True,
                    "data": data,
                    "stats": stats,
                    "chart_type": "list"
                }, indent=2)
            except Exception as e:
                import traceback
                error_trace = traceback.format_exc()
                return json.dumps({
                    "success": False,
                    "error": str(e),
                    "traceback": error_trace
                })
        
        @tool
        def explore_available_years() -> str:
            """
            List all available years with paper counts.
            
            Returns:
                JSON string with year information
            """
            try:
                result_df = executor.get_available_years()
                data = result_df.to_dict('records')
                stats = {
                    "total_years": len(result_df),
                    "total_papers": int(result_df['paper_count'].sum())
                }
                return json.dumps({
                    "success": True,
                    "data": data,
                    "stats": stats,
                    "chart_type": "list"
                }, indent=2)
            except Exception as e:
                import traceback
                error_trace = traceback.format_exc()
                return json.dumps({
                    "success": False,
                    "error": str(e),
                    "traceback": error_trace
                })
        
        @tool
        def explore_top_authors(tool_input: str = "{}") -> str:
            """
            Find top authors by paper count.
            
            Args:
                tool_input: JSON string with parameters. Example: '{"limit": 10, "min_papers": 5, "field_filter": "machine learning"}'
            
            Returns:
                JSON string with author data
            """
            # Parse JSON string
            try:
                if isinstance(tool_input, str):
                    params = json.loads(tool_input)
                elif isinstance(tool_input, dict):
                    params = tool_input
                else:
                    params = {}
                
                limit = params.get('limit', 10)
                min_papers = params.get('min_papers')
                field_filter = params.get('field_filter')
            except json.JSONDecodeError as e:
                return json.dumps({
                    "success": False,
                    "error": f"Invalid JSON input: {str(e)}"
                })
            
            try:
                result_df = executor.get_top_authors(
                    limit=limit, min_papers=min_papers, field_filter=field_filter
                )
                data = result_df.to_dict('records')
                stats = {
                    "total_authors": len(result_df),
                    "top_author_papers": int(result_df.iloc[0]['paper_count']) if len(result_df) > 0 else 0
                }
                return json.dumps({
                    "success": True,
                    "data": data,
                    "stats": stats,
                    "chart_type": "bar"
                }, indent=2)
            except Exception as e:
                import traceback
                error_trace = traceback.format_exc()
                return json.dumps({
                    "success": False,
                    "error": str(e),
                    "traceback": error_trace
                })
        
        @tool
        def analyze_field_trends(tool_input: str) -> str:
            """
            Analyze how fields change over time.
            
            Args:
                tool_input: JSON string with parameters. Example: '{"field": "machine learning", "start_year": 2020, "end_year": 2024, "metric": "count"}'
                    - field: Optional field name filter
                    - start_year: Start year for analysis
                    - end_year: End year for analysis
                    - metric: Analysis metric - "count", "citations", or "patents" (default: "count")
            
            Returns:
                JSON string with trend data
            """
            # Parse JSON string
            try:
                if isinstance(tool_input, str):
                    # Handle case where tool_input might be a JSON string wrapped in quotes
                    tool_input = tool_input.strip().strip("'\"")
                    params = json.loads(tool_input)
                elif isinstance(tool_input, dict):
                    params = tool_input
                else:
                    params = {}
                
                field = params.get('field')
                start_year = params.get('start_year')
                end_year = params.get('end_year')
                metric = params.get('metric', 'count')
            except json.JSONDecodeError as e:
                return json.dumps({
                    "success": False,
                    "error": f"Invalid JSON input: {str(e)}"
                })
            
            try:
                result_df = executor.analyze_field_trends(
                    field=field, start_year=start_year, end_year=end_year, metric=metric
                )
                data = result_df.to_dict('records')
                stats = {
                    "total_years": len(result_df),
                    "metric": metric
                }
                return json.dumps({
                    "success": True,
                    "data": data,
                    "stats": stats,
                    "chart_type": "line"
                }, indent=2)
            except Exception as e:
                import traceback
                error_trace = traceback.format_exc()
                return json.dumps({
                    "success": False,
                    "error": str(e),
                    "traceback": error_trace
                })
        
        @tool
        def analyze_citation_patterns(tool_input: str) -> str:
            """
            Analyze citation patterns.
            
            Args:
                tool_input: JSON string with parameters. Example: '{"year": 2023, "field": "machine learning", "min_citations": 10}'
                    - year: Optional year filter
                    - field: Optional field name filter
                    - min_citations: Minimum citation threshold
            
            Returns:
                JSON string with citation pattern data
            """
            # Parse JSON string
            try:
                if isinstance(tool_input, str):
                    # Handle case where tool_input might be a JSON string wrapped in quotes
                    tool_input = tool_input.strip().strip("'\"")
                    params = json.loads(tool_input)
                elif isinstance(tool_input, dict):
                    params = tool_input
                else:
                    params = {}
                
                year = params.get('year')
                field = params.get('field')
                min_citations = params.get('min_citations')
            except json.JSONDecodeError as e:
                return json.dumps({
                    "success": False,
                    "error": f"Invalid JSON input: {str(e)}"
                })
            
            try:
                result_df = executor.analyze_citation_patterns(
                    year=year, field=field, min_citations=min_citations
                )
                data = result_df.to_dict('records')
                stats = {
                    "total_papers": int(result_df['paper_count'].sum())
                }
                return json.dumps({
                    "success": True,
                    "data": data,
                    "stats": stats,
                    "chart_type": "bar"
                }, indent=2)
            except Exception as e:
                import traceback
                error_trace = traceback.format_exc()
                return json.dumps({
                    "success": False,
                    "error": str(e),
                    "traceback": error_trace
                })
        
        @tool
        def analyze_patent_distribution(tool_input: str) -> str:
            """
            Analyze patent distribution.
            
            Args:
                tool_input: JSON string with parameters. Example: '{"year": 2023, "field": "machine learning"}'
                    - year: Optional year filter
                    - field: Optional field name filter
            
            Returns:
                JSON string with patent distribution data
            """
            # Parse JSON string
            try:
                if isinstance(tool_input, str):
                    params = json.loads(tool_input)
                elif isinstance(tool_input, dict):
                    params = tool_input
                else:
                    params = {}
                
                year = params.get('year')
                field = params.get('field')
            except json.JSONDecodeError as e:
                return json.dumps({
                    "success": False,
                    "error": f"Invalid JSON input: {str(e)}"
                })
            
            try:
                result_df = executor.get_patent_distribution(year=year, field=field)
                data = result_df.to_dict('records')
                stats = {
                    "total_papers": int(result_df['paper_count'].sum()),
                    "papers_with_patents": int(result_df[result_df['patent_count'] > 0]['paper_count'].sum()),
                    "avg_patents": float((result_df['patent_count'] * result_df['paper_count']).sum() / result_df['paper_count'].sum()) if result_df['paper_count'].sum() > 0 else 0
                }
                return json.dumps({
                    "success": True,
                    "data": data,
                    "stats": stats,
                    "chart_type": "bar"
                }, indent=2)
            except Exception as e:
                import traceback
                error_trace = traceback.format_exc()
                return json.dumps({
                    "success": False,
                    "error": str(e),
                    "traceback": error_trace
                })
        
        @tool
        def ask_clarification_question(question: str) -> str:
            """
            Ask user for missing information when query is unclear.
            
            Args:
                question: The clarification question to ask the user
            
            Returns:
                JSON string indicating that clarification is needed
            """
            return json.dumps({
                "success": False,
                "needs_clarification": True,
                "question": question,
                "message": f"I need more information: {question}"
            }, indent=2)
        
        return [query_papers_by_field,
            query_papers_by_year,
            query_papers_by_citations,
            query_papers_by_patents,
            query_papers_advanced,
            explore_available_fields,
            explore_available_years,
            explore_top_authors,
            analyze_field_trends,
            analyze_citation_patterns,
            analyze_patent_distribution,
            ask_clarification_question
        ]
