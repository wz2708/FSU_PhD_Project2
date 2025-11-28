"""
Query executor for sample dataset with advanced filtering capabilities.
"""

import duckdb
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
from config import SAMPLE_DATA_DIR


class QueryExecutor:
    """Executes SQL queries on sample dataset with advanced filtering."""
    
    def __init__(self, data_dir: Optional[str] = None):
        if data_dir is None:
            data_dir = SAMPLE_DATA_DIR
        
        self.data_dir = Path(data_dir)
        self.conn = duckdb.connect()
        
        self.papers_path = str(self.data_dir / 'sample_papers.parquet')
        self.paperrefs_path = str(self.data_dir / 'sample_paperrefs.parquet')
        self.paper_author_affil_path = str(self.data_dir / 'sample_paper_author_affiliation.parquet')
        self.paperfields_path = str(self.data_dir / 'sample_paperfields.parquet')
        self.link_patents_path = str(self.data_dir / 'sample_link_patents.parquet')
        self.fields_path = str(self.data_dir / 'sample_fields.parquet')
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return DataFrame."""
        try:
            result = self.conn.execute(query).df()
            return result
        except Exception as e:
            raise Exception(f"Query execution failed: {str(e)}")
    
    def get_papers(self, filters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Get papers with optional filters."""
        query = f"SELECT * FROM read_parquet('{self.papers_path}')"
        
        conditions = []
        if filters:
            if 'year' in filters:
                conditions.append(f"year = {filters['year']}")
            if 'year_range' in filters:
                start, end = filters['year_range']
                conditions.append(f"year >= {start} AND year <= {end}")
            if 'start_year' in filters:
                conditions.append(f"year >= {filters['start_year']}")
            if 'end_year' in filters:
                conditions.append(f"year <= {filters['end_year']}")
            if 'min_citations' in filters:
                conditions.append(f"cited_by_count >= {filters['min_citations']}")
            if 'max_citations' in filters:
                conditions.append(f"cited_by_count <= {filters['max_citations']}")
            if 'min_patents' in filters:
                conditions.append(f"patent_count >= {filters['min_patents']}")
            if 'has_patents' in filters and filters['has_patents']:
                conditions.append(f"patent_count > 0")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        return self.execute_query(query)
    
    def get_papers_by_field(self, limit: Optional[int] = None, field_name: Optional[str] = None) -> pd.DataFrame:
        """Get paper count by field."""
        print(f"\n[QUERY_EXECUTOR] get_papers_by_field called")
        print(f"[QUERY_EXECUTOR] Parameters: limit={limit}, field_name={field_name}")
        print(f"[QUERY_EXECUTOR] Papers path: {self.papers_path}")
        print(f"[QUERY_EXECUTOR] Paperfields path: {self.paperfields_path}")
        print(f"[QUERY_EXECUTOR] Fields path: {self.fields_path}")
        
        query = f"""
        SELECT pf.fieldid, f.display_name, COUNT(DISTINCT pf.paperid) as paper_count
        FROM read_parquet('{self.paperfields_path}') pf
        LEFT JOIN read_parquet('{self.fields_path}') f ON pf.fieldid = f.fieldid
        """
        
        conditions = []
        if field_name:
            conditions.append(f"f.display_name ILIKE '%{field_name}%'")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " GROUP BY pf.fieldid, f.display_name ORDER BY paper_count DESC"
        
        if limit and limit > 0:
            query += f" LIMIT {limit}"
        
        print(f"[QUERY_EXECUTOR] Final query:\n{query}")
        
        try:
            print(f"[QUERY_EXECUTOR] Executing query...")
            result = self.execute_query(query)
            print(f"[QUERY_EXECUTOR] Query executed successfully")
            print(f"[QUERY_EXECUTOR] Result shape: {result.shape}")
            print(f"[QUERY_EXECUTOR] Result columns: {result.columns.tolist()}")
            print(f"[QUERY_EXECUTOR] First few rows:\n{result.head()}")
            return result
        except Exception as e:
            import traceback
            print(f"[QUERY_EXECUTOR ERROR] Exception in get_papers_by_field:")
            print(f"[QUERY_EXECUTOR ERROR] Error: {str(e)}")
            print(f"[QUERY_EXECUTOR ERROR] Traceback:\n{traceback.format_exc()}")
            raise
    
    def get_papers_by_year(self, year: Optional[int] = None, start_year: Optional[int] = None, 
                          end_year: Optional[int] = None, years: Optional[int] = None) -> pd.DataFrame:
        """Get paper count by year."""
        query = f"""
        SELECT year, COUNT(*) as count
        FROM read_parquet('{self.papers_path}')
        """
        
        conditions = []
        if year:
            conditions.append(f"year = {year}")
        if start_year:
            conditions.append(f"year >= {start_year}")
        if end_year:
            conditions.append(f"year <= {end_year}")
        if years:
            current_year = pd.Timestamp.now().year
            start = current_year - years
            conditions.append(f"year >= {start}")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " GROUP BY year ORDER BY year"
        return self.execute_query(query)
    
    def get_papers_by_citations(self, min_citations: Optional[int] = None, 
                                max_citations: Optional[int] = None,
                                year: Optional[int] = None,
                                field: Optional[str] = None) -> pd.DataFrame:
        """Get papers filtered by citation count."""
        query = f"""
        SELECT p.*, COUNT(DISTINCT pf.fieldid) as field_count
        FROM read_parquet('{self.papers_path}') p
        LEFT JOIN read_parquet('{self.paperfields_path}') pf ON p.paperid = pf.paperid
        """
        
        conditions = []
        if min_citations is not None:
            conditions.append(f"p.cited_by_count >= {min_citations}")
        if max_citations is not None:
            conditions.append(f"p.cited_by_count <= {max_citations}")
        if year:
            conditions.append(f"p.year = {year}")
        if field:
            query += f"""
            LEFT JOIN read_parquet('{self.fields_path}') f ON pf.fieldid = f.fieldid
            """
            conditions.append(f"f.display_name ILIKE '%{field}%'")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " GROUP BY p.paperid, p.year, p.doctype, p.is_retracted, p.cited_by_count, p.patent_count"
        query += " ORDER BY p.cited_by_count DESC"
        
        return self.execute_query(query)
    
    def get_papers_by_patents(self, min_patents: Optional[int] = None,
                              has_patents: Optional[bool] = None,
                              year: Optional[int] = None) -> pd.DataFrame:
        """Get papers filtered by patent count."""
        query = f"""
        SELECT p.*, COALESCE(pat.patent_count, 0) as actual_patent_count
        FROM read_parquet('{self.papers_path}') p
        LEFT JOIN (
            SELECT paperid, COUNT(*) as patent_count
            FROM read_parquet('{self.link_patents_path}')
            GROUP BY paperid
        ) pat ON p.paperid = pat.paperid
        """
        
        conditions = []
        if min_patents is not None:
            conditions.append(f"COALESCE(pat.patent_count, 0) >= {min_patents}")
        if has_patents:
            conditions.append(f"COALESCE(pat.patent_count, 0) > 0")
        if year:
            conditions.append(f"p.year = {year}")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY actual_patent_count DESC"
        
        return self.execute_query(query)
    
    def get_papers_advanced(self, filters: Dict[str, Any]) -> pd.DataFrame:
        """Advanced query with multiple field filters."""
        query = f"""
        SELECT DISTINCT p.*
        FROM read_parquet('{self.papers_path}') p
        """
        
        joins = []
        conditions = []
        
        if filters.get('field') or filters.get('fields'):
            joins.append(f"INNER JOIN read_parquet('{self.paperfields_path}') pf ON p.paperid = pf.paperid")
            joins.append(f"INNER JOIN read_parquet('{self.fields_path}') f ON pf.fieldid = f.fieldid")
            
            field_conditions = []
            if filters.get('field'):
                field_conditions.append(f"f.display_name ILIKE '%{filters['field']}%'")
            if filters.get('fields'):
                field_list = filters['fields']
                if isinstance(field_list, list):
                    # Build field list string first to avoid backslash in f-string
                    field_values = ', '.join([f"'{f}'" for f in field_list])
                    field_conditions.append(f"f.display_name IN ({field_values})")
            
            if field_conditions:
                conditions.append("(" + " OR ".join(field_conditions) + ")")
        
        if filters.get('author_id') or filters.get('author_name'):
            joins.append(f"INNER JOIN read_parquet('{self.paper_author_affil_path}') paa ON p.paperid = paa.paperid")
            if filters.get('author_id'):
                conditions.append(f"paa.authorid = '{filters['author_id']}'")
        
        for join in joins:
            query += f"\n{join}"
        
        if filters.get('year'):
            conditions.append(f"p.year = {filters['year']}")
        if filters.get('start_year'):
            conditions.append(f"p.year >= {filters['start_year']}")
        if filters.get('end_year'):
            conditions.append(f"p.year <= {filters['end_year']}")
        if filters.get('year_range'):
            start, end = filters['year_range']
            conditions.append(f"p.year >= {start} AND p.year <= {end}")
        if filters.get('min_citations') is not None:
            conditions.append(f"p.cited_by_count >= {filters['min_citations']}")
        if filters.get('max_citations') is not None:
            conditions.append(f"p.cited_by_count <= {filters['max_citations']}")
        if filters.get('min_patents') is not None:
            conditions.append(f"p.patent_count >= {filters['min_patents']}")
        if filters.get('has_patents'):
            conditions.append(f"p.patent_count > 0")
        
        if conditions:
            query += "\nWHERE " + " AND ".join(conditions)
        
        if filters.get('limit'):
            query += f"\nLIMIT {filters['limit']}"
        
        return self.execute_query(query)
    
    def get_patent_distribution(self, year: Optional[int] = None, field: Optional[str] = None) -> pd.DataFrame:
        """Get patent citation distribution."""
        papers_query = f"SELECT paperid FROM read_parquet('{self.papers_path}')"
        
        paper_conditions = []
        if year:
            paper_conditions.append(f"year = {year}")
        
        if paper_conditions:
            papers_query += " WHERE " + " AND ".join(paper_conditions)
        
        query = f"""
        WITH filtered_papers AS ({papers_query})
        SELECT 
            COALESCE(patent_count, 0) as patent_count,
            COUNT(*) as paper_count
        FROM filtered_papers p
        LEFT JOIN (
            SELECT paperid, COUNT(*) as patent_count
            FROM read_parquet('{self.link_patents_path}')
            GROUP BY paperid
        ) pat ON p.paperid = pat.paperid
        """
        
        if field:
            query += f"""
            INNER JOIN read_parquet('{self.paperfields_path}') pf ON p.paperid = pf.paperid
            INNER JOIN read_parquet('{self.fields_path}') f ON pf.fieldid = f.fieldid
            WHERE f.display_name ILIKE '%{field}%'
            """
        
        query += "\nGROUP BY patent_count ORDER BY patent_count"
        
        return self.execute_query(query)
    
    def get_available_fields(self) -> pd.DataFrame:
        """Get all available fields with paper counts."""
        query = f"""
        SELECT f.fieldid, f.display_name, COUNT(DISTINCT pf.paperid) as paper_count
        FROM read_parquet('{self.fields_path}') f
        LEFT JOIN read_parquet('{self.paperfields_path}') pf ON f.fieldid = pf.fieldid
        GROUP BY f.fieldid, f.display_name
        HAVING COUNT(DISTINCT pf.paperid) > 0
        ORDER BY paper_count DESC
        """
        return self.execute_query(query)
    
    def get_available_years(self) -> pd.DataFrame:
        """Get all available years with paper counts."""
        query = f"""
        SELECT year, COUNT(*) as paper_count
        FROM read_parquet('{self.papers_path}')
        GROUP BY year
        ORDER BY year
        """
        return self.execute_query(query)
    
    def get_top_authors(self, limit: Optional[int] = 10, min_papers: Optional[int] = None,
                       field_filter: Optional[str] = None) -> pd.DataFrame:
        """Get top authors by paper count."""
        query = f"""
        SELECT paa.authorid, COUNT(DISTINCT paa.paperid) as paper_count
        FROM read_parquet('{self.paper_author_affil_path}') paa
        """
        
        if field_filter:
            query += f"""
            INNER JOIN read_parquet('{self.paperfields_path}') pf ON paa.paperid = pf.paperid
            INNER JOIN read_parquet('{self.fields_path}') f ON pf.fieldid = f.fieldid
            WHERE f.display_name ILIKE '%{field_filter}%'
            """
        
        query += "\nGROUP BY paa.authorid"
        
        if min_papers:
            query += f"\nHAVING COUNT(DISTINCT paa.paperid) >= {min_papers}"
        
        query += "\nORDER BY paper_count DESC"
        
        if limit:
            query += f"\nLIMIT {limit}"
        
        return self.execute_query(query)
    
    def analyze_field_trends(self, field: Optional[str] = None, start_year: Optional[int] = None,
                            end_year: Optional[int] = None, metric: str = "count") -> pd.DataFrame:
        """Analyze field trends over time."""
        if metric == "count":
            query = f"""
            SELECT p.year, COUNT(DISTINCT p.paperid) as value
            FROM read_parquet('{self.papers_path}') p
            """
        elif metric == "citations":
            query = f"""
            SELECT p.year, AVG(p.cited_by_count) as value
            FROM read_parquet('{self.papers_path}') p
            """
        elif metric == "patents":
            query = f"""
            SELECT p.year, AVG(p.patent_count) as value
            FROM read_parquet('{self.papers_path}') p
            """
        else:
            query = f"""
            SELECT p.year, COUNT(DISTINCT p.paperid) as value
            FROM read_parquet('{self.papers_path}') p
            """
        
        if field:
            query += f"""
            INNER JOIN read_parquet('{self.paperfields_path}') pf ON p.paperid = pf.paperid
            INNER JOIN read_parquet('{self.fields_path}') f ON pf.fieldid = f.fieldid
            WHERE f.display_name ILIKE '%{field}%'
            """
        
        conditions = []
        if start_year:
            conditions.append(f"p.year >= {start_year}")
        if end_year:
            conditions.append(f"p.year <= {end_year}")
        
        if conditions:
            if field:
                query += " AND " + " AND ".join(conditions)
            else:
                query += " WHERE " + " AND ".join(conditions)
        
        query += "\nGROUP BY p.year ORDER BY p.year"
        
        return self.execute_query(query)
    
    def analyze_citation_patterns(self, year: Optional[int] = None, field: Optional[str] = None,
                                  min_citations: Optional[int] = None) -> pd.DataFrame:
        """Analyze citation patterns."""
        query = f"""
        SELECT 
            CASE 
                WHEN p.cited_by_count = 0 THEN '0'
                WHEN p.cited_by_count BETWEEN 1 AND 10 THEN '1-10'
                WHEN p.cited_by_count BETWEEN 11 AND 50 THEN '11-50'
                WHEN p.cited_by_count BETWEEN 51 AND 100 THEN '51-100'
                ELSE '100+'
            END as citation_range,
            COUNT(*) as paper_count
        FROM read_parquet('{self.papers_path}') p
        """
        
        conditions = []
        if year:
            conditions.append(f"p.year = {year}")
        if min_citations is not None:
            conditions.append(f"p.cited_by_count >= {min_citations}")
        
        if field:
            query += f"""
            INNER JOIN read_parquet('{self.paperfields_path}') pf ON p.paperid = pf.paperid
            INNER JOIN read_parquet('{self.fields_path}') f ON pf.fieldid = f.fieldid
            WHERE f.display_name ILIKE '%{field}%'
            """
            if conditions:
                query += " AND " + " AND ".join(conditions)
        elif conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += "\nGROUP BY citation_range ORDER BY MIN(p.cited_by_count)"
        
        return self.execute_query(query)
    
    def close(self):
        """Close database connection."""
        self.conn.close()
