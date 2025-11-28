"""
Data processing module for SciSciNet dataset.

This module handles loading, filtering, and network construction from
SciSciNet parquet datasets using DuckDB for efficient SQL-based queries.
"""

import os
import hashlib
import threading
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from collections import defaultdict

import pandas as pd
import numpy as np
import networkx as nx
import duckdb


class SciSciNetProcessor:
    """
    Processor for SciSciNet dataset using DuckDB.
    
    Handles loading and filtering of SciSciNet parquet files for
    Columbia University Computer Science papers. Uses DuckDB for
    efficient SQL-based queries directly on Parquet files.
    
    Key advantages:
    - No need to load entire files into memory
    - SQL-based filtering with pushdown predicates
    - Exact institution/field ID matching (no string matching)
    - Handles datasets of any size efficiently
    """
    
    # Columbia University official OpenAlex institution ID
    # Verified via OpenAlex API: https://api.openalex.org/institutions/I78577930
    # Display Name: "Columbia University", Country: US, City: New York
    COLUMBIA_INSTITUTION_ID = 'I78577930'
    
    # Computer Science field ID in SciSciNet (OpenAlex concept ID)
    # Verified: C41008148 = "Computer science" (study of computation)
    CS_FIELD_ID = 'C41008148'
    
    def __init__(self, data_dir: Optional[str] = None):
        """
        Initialize the processor.
        
        Args:
            data_dir: Directory containing SciSciNet parquet files.
                     If None, defaults to ../data relative to this script.
        """
        # Always define current_dir for cache directory
        current_dir = Path(__file__).parent
        
        if data_dir is None:
            project_root = current_dir.parent
            self.data_dir = project_root / 'data'
        else:
            self.data_dir = Path(data_dir)
        
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")
        
        # Initialize DuckDB connection with memory limits (lazy, no heavy operations)
        try:
            self.conn = duckdb.connect()
            # Set memory limit to prevent OOM (use 8GB, leave some for system)
            self.conn.execute("SET memory_limit='8GB'")
            self.conn.execute("SET threads=4")  # Reduce threads to save memory
            self.conn.execute("SET preserve_insertion_order=false")  # Allow better optimization
        except Exception as e:
            print(f"Warning: DuckDB initialization issue: {e}")
            raise
        
        # Register Parquet file paths for easy querying
        self.papers_path = str(self.data_dir / 'sciscinet_papers.parquet')
        self.paperrefs_path = str(self.data_dir / 'sciscinet_paperrefs.parquet')
        self.paper_author_affil_path = str(self.data_dir / 'sciscinet_paper_author_affiliation.parquet')
        self.paperfields_path = str(self.data_dir / 'sciscinet_paperfields.parquet')
        self.link_patents_path = str(self.data_dir / 'sciscinet_link_patents.parquet')  
        
        # Verify files exist
        for name, path in [
            ('papers', self.papers_path),
            ('paperrefs', self.paperrefs_path),
            ('paper_author_affiliation', self.paper_author_affil_path),
            ('paperfields', self.paperfields_path),
            ('link_patents', self.link_patents_path) 
        ]:
            if not Path(path).exists():
                print(f"Warning: Optional data file not found: {path}")
        
        # Cached filtered data (per years, not global)
        # Change from single instance cache to per-years cache
        self._filtered_paper_ids_cache: Dict[int, Set[str]] = {}
        self._filtered_papers_df_cache: Dict[int, pd.DataFrame] = {}
        
        # Query lock to prevent concurrent duplicate queries
        # Use RLock (reentrant lock) to allow nested calls
        # This prevents deadlock when filter_columbia_cs_papers calls _get_filtered_paper_ids
        self._query_lock = threading.RLock()
        
        # Cache version identifier (based on filter conditions)
        # This ensures cache is invalidated when filter conditions change
        filter_signature = f"{self.COLUMBIA_INSTITUTION_ID}_{self.CS_FIELD_ID}_first_author_article_not_retracted"
        self.cache_version = hashlib.md5(filter_signature.encode()).hexdigest()[:8]
        
        # Cache directory for preprocessed data
        self.cache_dir = current_dir / 'cache'
        self.cache_dir.mkdir(exist_ok=True)
    
    def _get_filtered_paper_ids(self, years: int = 5) -> Set[str]:
        """
        Get set of paper IDs matching all filters.
        
        Filters (ALL REQUIRED):
        1. Columbia University (institution ID: I78577930)
        2. Computer Science (field ID: C41008148)
        3. Year range (last N years)
        4. Article type (doctype = 'article')
        5. Not retracted (is_retracted = false)
        6. First author from Columbia (author_position = 'first')
        
        Args:
            years: Number of years to look back from current year
            
        Returns:
            Set of filtered paper IDs
        """
        with self._query_lock:
            if years in self._filtered_paper_ids_cache:
                return self._filtered_paper_ids_cache[years]
            
            cache_file = self.cache_dir / f'filtered_paper_ids_{years}yr_{self.cache_version}.pkl'
            
            if cache_file.exists():
                try:
                    import pickle
                    with open(cache_file, 'rb') as f:
                        paper_ids = pickle.load(f)
                    self._filtered_paper_ids_cache[years] = paper_ids
                    return paper_ids
                except Exception as e:
                    pass
            
            old_cache_file = self.cache_dir / f'filtered_paper_ids_{years}yr.pkl'
            
            if old_cache_file.exists():
                try:
                    import pickle
                    with open(old_cache_file, 'rb') as f:
                        paper_ids = pickle.load(f)
                    self._filtered_paper_ids_cache[years] = paper_ids
                    
                    try:
                        new_cache_file = self.cache_dir / f'filtered_paper_ids_{years}yr_{self.cache_version}.pkl'
                        with open(new_cache_file, 'wb') as f:
                            pickle.dump(paper_ids, f)
                    except:
                        pass
                    
                    return paper_ids
                except Exception as e:
                    pass
            
            current_year = datetime.now().year
            start_year = current_year - years
            
            # SQL query with ALL required filters
            query = f"""
            WITH columbia_first_author_papers AS (
                SELECT DISTINCT paperid
                FROM read_parquet('{self.paper_author_affil_path}')
                WHERE institutionid = '{self.COLUMBIA_INSTITUTION_ID}'
                  AND author_position = 'first'  -- REQUIRED: Columbia as first author
            ),
            cs_papers AS (
                SELECT DISTINCT paperid
                FROM read_parquet('{self.paperfields_path}')
                WHERE fieldid = '{self.CS_FIELD_ID}'
            ),
            filtered_papers AS (
                SELECT DISTINCT p.paperid
                FROM read_parquet('{self.papers_path}') p
                INNER JOIN columbia_first_author_papers c ON p.paperid = c.paperid
                INNER JOIN cs_papers cs ON p.paperid = cs.paperid
                WHERE p.year >= {start_year} AND p.year <= {current_year}
                  AND p.doctype = 'article'  -- REQUIRED: Only journal articles
                  AND p.is_retracted = false  -- REQUIRED: Exclude retracted papers
            )
            SELECT paperid FROM filtered_papers
            """
            
            try:
                if years >= 10:
                    temp_file = self.cache_dir / f'temp_paper_ids_{years}yr_{self.cache_version}.parquet'
                    try:
                        self.conn.execute(f"""
                            COPY (
                                {query}
                            ) TO '{temp_file}' (FORMAT PARQUET, COMPRESSION SNAPPY)
                        """)
                        
                        paper_ids = set()
                        chunk_size = 50000
                        offset = 0
                        while True:
                            chunk_df = self.conn.execute(f"""
                                SELECT paperid
                                FROM read_parquet('{temp_file}')
                                LIMIT {chunk_size} OFFSET {offset}
                            """).fetchdf()
                            
                            if chunk_df.empty:
                                break
                            
                            paper_ids.update(chunk_df['paperid'].tolist())
                            offset += len(chunk_df)
                        
                        if temp_file.exists():
                            temp_file.unlink()
                    except Exception:
                        result_df = self.conn.execute(query).fetchdf()
                        paper_ids = set(result_df['paperid'].tolist())
                else:
                    result_df = self.conn.execute(query).fetchdf()
                    paper_ids = set(result_df['paperid'].tolist())
                
                self._filtered_paper_ids_cache[years] = paper_ids
                
                try:
                    import pickle
                    cache_file = self.cache_dir / f'filtered_paper_ids_{years}yr_{self.cache_version}.pkl'
                    cache_file.parent.mkdir(parents=True, exist_ok=True)
                    with open(cache_file, 'wb') as f:
                        pickle.dump(paper_ids, f)
                except Exception:
                    pass
                
                return paper_ids
            except Exception as e:
                print(f"Error querying filtered papers: {e}")
                return set()
    
    def filter_columbia_cs_papers(self, years: int = 5) -> pd.DataFrame:
        """
        Filter papers for Columbia University, Computer Science, and specified years.
        
        Args:
            years: Number of years to look back (default: 5, max: 10 for T2)
            
        Returns:
            Filtered DataFrame with paper information
        """
        with self._query_lock:
            if years in self._filtered_papers_df_cache:
                return self._filtered_papers_df_cache[years]
            
            papers_cache_file = self.cache_dir / f'filtered_papers_{years}yr_{self.cache_version}.parquet'
            
            if papers_cache_file.exists():
                try:
                    df = pd.read_parquet(papers_cache_file)
                    if 'year' in df.columns:
                        self._filtered_papers_df_cache[years] = df
                        return df
                except Exception:
                    pass
            
            filtered_ids = self._get_filtered_paper_ids(years=years)
            
            if not filtered_ids:
                return pd.DataFrame()
            
            filtered_ids_df = pd.DataFrame({'paperid': list(filtered_ids)})
            self.conn.register('filtered_ids', filtered_ids_df)
            
            query = f"""
            SELECT p.*
            FROM read_parquet('{self.papers_path}') p
            INNER JOIN filtered_ids f ON p.paperid = f.paperid
            """
            
            try:
                result = self.conn.execute(query)
                filtered_papers = result.fetchdf()
            except Exception as query_error:
                raise
            
            if filtered_papers.empty:
                return pd.DataFrame()
            
            if 'year' not in filtered_papers.columns:
                raise ValueError("DataFrame missing required 'year' column")
            
            self._filtered_papers_df_cache[years] = filtered_papers
            
            try:
                papers_cache_file.parent.mkdir(parents=True, exist_ok=True)
                filtered_papers.to_parquet(papers_cache_file, index=False, compression='snappy')
            except Exception:
                pass
            
            return filtered_papers
    
    def build_citation_network(self, papers_df: pd.DataFrame) -> nx.DiGraph:
        """
        Build a directed citation network from papers.
        Uses DuckDB to efficiently query only relevant citations.
        
        Args:
            papers_df: DataFrame containing filtered paper information
            
        Returns:
            Directed NetworkX graph representing citation relationships
        """
        G = nx.DiGraph()
        
        if papers_df.empty:
            return G
        
        paper_ids = set(papers_df['paperid'].tolist())
        
        # Estimate years for cache lookup
        years = int((datetime.now().year - papers_df['year'].min()) if not papers_df.empty and 'year' in papers_df.columns else 5)
        citation_cache_file = self.cache_dir / f'citation_network_{years}yr_{self.cache_version}.parquet'
        
        if citation_cache_file.exists():
            try:
                import duckdb
                conn = duckdb.connect()
                citations_df = conn.execute(f"SELECT * FROM read_parquet('{citation_cache_file}')").fetchdf()
                conn.close()
                
                for _, paper in papers_df.iterrows():
                    paper_id = paper['paperid']
                    patent_count = int(paper.get('patent_count', 0) or 0)
                    citation_count = int(paper.get('cited_by_count', 0) or 0)
                    
                    G.add_node(
                        paper_id,
                        year=int(paper.get('year', 0)),
                        citations=citation_count,
                        patents=patent_count
                    )
                
                for _, citation in citations_df.iterrows():
                    citing = citation['citing_paperid']
                    cited = citation['cited_paperid']
                    weight = int(citation['weight'])
                    G.add_edge(citing, cited, weight=weight)
                
                return G
            except Exception:
                pass
        
        # Add nodes (papers)
        for _, paper in papers_df.iterrows():
            paper_id = paper['paperid']
            patent_count = int(paper.get('patent_count', 0) or 0)
            citation_count = int(paper.get('cited_by_count', 0) or 0)
            
            G.add_node(
                paper_id,
                year=int(paper.get('year', 0)),
                citations=citation_count,
                patents=patent_count
            )
        
        conn = duckdb.connect()
        try:
            conn.execute("SET memory_limit='8GB'")
            conn.execute("SET threads=4")
            conn.execute("SET preserve_insertion_order=false")
            
            table_name = 'filtered_papers'
            paper_ids_df = pd.DataFrame({'paperid': list(paper_ids)})
            conn.register(table_name, paper_ids_df)
            
            query = f"""
            WITH citing_refs AS (
                SELECT refs.citing_paperid, refs.cited_paperid
                FROM read_parquet('{self.paperrefs_path}') refs
                INNER JOIN {table_name} fp1 ON refs.citing_paperid = fp1.paperid
            )
            SELECT 
                citing_paperid, 
                cited_paperid, 
                COUNT(*) as weight
            FROM citing_refs
            INNER JOIN {table_name} fp2 ON citing_refs.cited_paperid = fp2.paperid
            WHERE citing_paperid != cited_paperid
            GROUP BY citing_paperid, cited_paperid
            """
            
            citations_df = conn.execute(query).fetchdf()
            
            for _, citation in citations_df.iterrows():
                citing = citation['citing_paperid']
                cited = citation['cited_paperid']
                weight = int(citation['weight'])
                G.add_edge(citing, cited, weight=weight)
            
            if not citations_df.empty:
                citations_df.to_parquet(citation_cache_file, index=False, compression='snappy')
            
        except Exception as e:
            print(f"Error building citation network: {e}")
        finally:
            try:
                conn.close()
            except:
                pass
        
        return G
    
    def build_collaboration_network(self, papers_df: pd.DataFrame) -> nx.Graph:
        """
        Build an undirected collaboration network from authors.
        
        IMPORTANT: Collaboration network includes ALL Columbia authors (not just first authors).
        This is because collaboration requires multiple authors, and restricting to first authors
        only would result in 0 pairs (each paper typically has only 1 first author).
        
        Filter conditions:
        - Only Columbia authors (institution ID: I78577930)
        - Only papers from filtered set (already filtered by: Columbia first author, CS field, article type, not retracted)
        """
        G = nx.Graph()
        
        if papers_df.empty:
            return G
        
        paper_ids = set(papers_df['paperid'].tolist())
        years = int((datetime.now().year - papers_df['year'].min()) if not papers_df.empty and 'year' in papers_df.columns else 5)
        
        conn = duckdb.connect()
        try:
            conn.execute("SET memory_limit='8GB'")
            conn.execute("SET threads=4")
            conn.execute("SET preserve_insertion_order=false")
            
            table_name = 'filtered_papers'
            paper_ids_df = pd.DataFrame({'paperid': list(paper_ids)})
            conn.register(table_name, paper_ids_df)
            
            coauthor_query = f"""
            WITH columbia_authors AS (
                SELECT DISTINCT affil.paperid, affil.authorid
                FROM read_parquet('{self.paper_author_affil_path}') affil
                INNER JOIN {table_name} fp ON affil.paperid = fp.paperid
                WHERE affil.institutionid = '{self.COLUMBIA_INSTITUTION_ID}'
            ),
            paper_author_counts AS (
                SELECT 
                    paperid,
                    COUNT(*) as author_count
                FROM columbia_authors
                GROUP BY paperid
            ),
            limited_authors AS (
                SELECT ca.paperid, ca.authorid
                FROM columbia_authors ca
                INNER JOIN paper_author_counts pac ON ca.paperid = pac.paperid
                WHERE pac.author_count <= 50
            )
            SELECT 
                CASE 
                    WHEN a1.authorid < a2.authorid THEN a1.authorid
                    ELSE a2.authorid
                END as author1,
                CASE 
                    WHEN a1.authorid < a2.authorid THEN a2.authorid
                    ELSE a1.authorid
                END as author2,
                COUNT(*) as weight
            FROM limited_authors a1
            INNER JOIN limited_authors a2 
                ON a1.paperid = a2.paperid 
                AND a1.authorid < a2.authorid
            GROUP BY author1, author2
            """
            
            coauthor_df = conn.execute(coauthor_query).fetchdf()
            
            coauthor_counts = {}
            for _, row in coauthor_df.iterrows():
                pair_key = (row['author1'], row['author2'])
                coauthor_counts[pair_key] = int(row['weight'])
                G.add_edge(pair_key[0], pair_key[1], weight=int(row['weight']))
            
            if len(coauthor_counts) < 500000:
                parquet_cache = self.cache_dir / f'coauthor_pairs_{years}yr.parquet'
                coauthor_df.to_parquet(parquet_cache, index=False, compression='snappy')
            
            all_authors = set()
            for pair_key in coauthor_counts.keys():
                all_authors.add(pair_key[0])
                all_authors.add(pair_key[1])
            
            for author_id in all_authors:
                G.add_node(author_id)
            
        except Exception as e:
            print(f"Error building collaboration network: {e}")
        finally:
            try:
                conn.close()
            except:
                pass
        
        return G
    
    def calculate_node_metrics(self, graph: nx.Graph) -> Dict[str, Dict]:
        """
        Calculate various node metrics for the graph.
        
        Args:
            graph: NetworkX graph
            
        Returns:
            Dictionary mapping node IDs to metric values
        """
        metrics = {}
        
        if len(graph.nodes()) == 0:
            return metrics
        
        # Degree centrality
        degree_centrality = nx.degree_centrality(graph)
        
        # PageRank (for directed graphs) or Eigenvector centrality (for undirected)
        if isinstance(graph, nx.DiGraph):
            try:
                importance = nx.pagerank(graph, max_iter=100)
            except:
                importance = {node: 0.0 for node in graph.nodes()}
        else:
            try:
                importance = nx.eigenvector_centrality(graph, max_iter=100)
            except:
                importance = {node: 0.0 for node in graph.nodes()}
        
        # Betweenness centrality (sampled for large graphs)
        if len(graph.nodes()) < 500:
            betweenness = nx.betweenness_centrality(graph)
        else:
            sample_size = min(100, len(graph.nodes()))
            betweenness = nx.betweenness_centrality(graph, k=sample_size)
        
        # Clustering coefficient (only for undirected graphs)
        if isinstance(graph, nx.Graph):
            clustering = nx.clustering(graph)
        else:
            clustering = {node: 0.0 for node in graph.nodes()}
        
        # Combine all metrics
        for node in graph.nodes():
            metrics[node] = {
                'degree': graph.degree(node),
                'degree_centrality': float(degree_centrality.get(node, 0.0)),
                'importance': float(importance.get(node, 0.0)),
                'betweenness': float(betweenness.get(node, 0.0)),
                'clustering': float(clustering.get(node, 0.0))
            }
        
        return metrics
    
    def detect_communities(self, graph: nx.Graph) -> Dict[str, int]:
        """
        Detect communities in the graph using Louvain algorithm.
        
        Args:
            graph: NetworkX graph
            
        Returns:
            Dictionary mapping node IDs to community IDs
        """
        if len(graph.nodes()) == 0:
            return {}
        
        try:
            import community.community_louvain as community_louvain
            
            # Convert to undirected for Louvain
            if isinstance(graph, nx.DiGraph):
                graph_undirected = graph.to_undirected()
            else:
                graph_undirected = graph
            
            partition = community_louvain.best_partition(graph_undirected)
            return partition
            
        except ImportError:
            # Fallback: assign each node to its own community
            return {node: idx for idx, node in enumerate(graph.nodes())}
        except Exception as e:
            print(f"Error in community detection: {e}")
            return {node: 0 for node in graph.nodes()}
    
    def __del__(self):
        """Close DuckDB connection on cleanup."""
        if hasattr(self, 'conn'):
            try:
                self.conn.close()
            except:
                pass

    def get_patent_counts_for_papers(self, paper_ids: Set[str], years: int = 5) -> Dict[str, int]:
        """
        Get patent citation counts for a set of papers.
        
        Args:
            paper_ids: Set of paper IDs to query
            years: Number of years (for logging)
            
        Returns:
            Dictionary mapping paper_id to patent_count
        """
        if not paper_ids:
            return {}
        
        if not Path(self.link_patents_path).exists():
            return {paper_id: 0 for paper_id in paper_ids}
        
        conn = duckdb.connect()
        try:
            conn.execute("SET memory_limit='8GB'")
            conn.execute("SET threads=4")
            
            paper_ids_df = pd.DataFrame({'paperid': list(paper_ids)})
            conn.register('filtered_papers', paper_ids_df)
            
            query = f"""
            SELECT 
                fp.paperid,
                COUNT(lp.patent) as patent_count
            FROM filtered_papers fp
            LEFT JOIN read_parquet('{self.link_patents_path}') lp 
                ON fp.paperid = lp.paperid
            GROUP BY fp.paperid
            """
            
            result_df = conn.execute(query).fetchdf()
            
            patent_counts = {}
            for _, row in result_df.iterrows():
                patent_counts[row['paperid']] = int(row['patent_count'] or 0)
            
            for paper_id in paper_ids:
                if paper_id not in patent_counts:
                    patent_counts[paper_id] = 0
            
            return patent_counts
            
        except Exception as e:
            return {paper_id: 0 for paper_id in paper_ids}
        finally:
            conn.close()
