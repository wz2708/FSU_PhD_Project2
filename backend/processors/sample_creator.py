"""
Sample data creator for Project 2.

Creates a small sample dataset from SciSciNet for Columbia University CS papers.
"""

import sys
from pathlib import Path

from data_processor import SciSciNetProcessor
import pandas as pd
import duckdb
from typing import Set

backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))
from config import DATA_DIR, SAMPLE_DATA_DIR, YEARS_BACK


def create_sample_dataset():
    """
    Create sample dataset for Project 2.
    
    Filters: Columbia University CS, first author, articles, not retracted, last 5 years.
    """
    print("Creating sample dataset...")
    
    processor = SciSciNetProcessor(data_dir=DATA_DIR)
    
    sample_dir = Path(SAMPLE_DATA_DIR)
    sample_dir.mkdir(exist_ok=True, parents=True)
    
    print(f"Filtering papers for {YEARS_BACK} years...")
    filtered_papers = processor.filter_columbia_cs_papers(years=YEARS_BACK)
    paper_ids = set(filtered_papers['paperid'].tolist())
    
    print(f"Found {len(paper_ids)} papers")
    
    conn = duckdb.connect()
    
    papers_path = str(Path(DATA_DIR) / 'sciscinet_papers.parquet')
    paperrefs_path = str(Path(DATA_DIR) / 'sciscinet_paperrefs.parquet')
    paper_author_affil_path = str(Path(DATA_DIR) / 'sciscinet_paper_author_affiliation.parquet')
    paperfields_path = str(Path(DATA_DIR) / 'sciscinet_paperfields.parquet')
    link_patents_path = str(Path(DATA_DIR) / 'sciscinet_link_patents.parquet')
    fields_path = str(Path(DATA_DIR) / 'sciscinet_fields.parquet')
    
    print(f"Extracting papers ({len(paper_ids)} papers)...")
    
    paper_ids_list = list(paper_ids)
    batch_size = 10000
    papers_dfs = []
    
    for i in range(0, len(paper_ids_list), batch_size):
        batch = paper_ids_list[i:i+batch_size]
        paper_ids_str = "', '".join(batch)
        papers_query = f"""
        SELECT * FROM read_parquet('{papers_path}')
        WHERE paperid IN ('{paper_ids_str}')
        """
        batch_df = conn.execute(papers_query).df()
        papers_dfs.append(batch_df)
    
    papers_df = pd.concat(papers_dfs, ignore_index=True)
    papers_df.to_parquet(sample_dir / 'sample_papers.parquet', index=False)
    print(f"Saved {len(papers_df)} papers")
    
    print("Extracting paper references...")
    refs_dfs = []
    for i in range(0, len(paper_ids_list), batch_size):
        batch = paper_ids_list[i:i+batch_size]
        paper_ids_str = "', '".join(batch)
        refs_query = f"""
        SELECT * FROM read_parquet('{paperrefs_path}')
        WHERE citing_paperid IN ('{paper_ids_str}')
           OR cited_paperid IN ('{paper_ids_str}')
        """
        batch_df = conn.execute(refs_query).df()
        refs_dfs.append(batch_df)
    refs_df = pd.concat(refs_dfs, ignore_index=True).drop_duplicates()
    refs_df.to_parquet(sample_dir / 'sample_paperrefs.parquet', index=False)
    print(f"Saved {len(refs_df)} references")
    
    print("Extracting paper-author-affiliation...")
    paa_dfs = []
    for i in range(0, len(paper_ids_list), batch_size):
        batch = paper_ids_list[i:i+batch_size]
        paper_ids_str = "', '".join(batch)
        paa_query = f"""
        SELECT * FROM read_parquet('{paper_author_affil_path}')
        WHERE paperid IN ('{paper_ids_str}')
        """
        batch_df = conn.execute(paa_query).df()
        paa_dfs.append(batch_df)
    paa_df = pd.concat(paa_dfs, ignore_index=True)
    paa_df.to_parquet(sample_dir / 'sample_paper_author_affiliation.parquet', index=False)
    print(f"Saved {len(paa_df)} paper-author-affiliation records")
    
    print("Extracting paper fields...")
    pf_dfs = []
    for i in range(0, len(paper_ids_list), batch_size):
        batch = paper_ids_list[i:i+batch_size]
        paper_ids_str = "', '".join(batch)
        pf_query = f"""
        SELECT * FROM read_parquet('{paperfields_path}')
        WHERE paperid IN ('{paper_ids_str}')
        """
        batch_df = conn.execute(pf_query).df()
        pf_dfs.append(batch_df)
    pf_df = pd.concat(pf_dfs, ignore_index=True)
    pf_df.to_parquet(sample_dir / 'sample_paperfields.parquet', index=False)
    print(f"Saved {len(pf_df)} paper-field records")
    
    print("Extracting patent links...")
    patents_dfs = []
    for i in range(0, len(paper_ids_list), batch_size):
        batch = paper_ids_list[i:i+batch_size]
        paper_ids_str = "', '".join(batch)
        patents_query = f"""
        SELECT * FROM read_parquet('{link_patents_path}')
        WHERE paperid IN ('{paper_ids_str}')
        """
        batch_df = conn.execute(patents_query).df()
        patents_dfs.append(batch_df)
    patents_df = pd.concat(patents_dfs, ignore_index=True)
    patents_df.to_parquet(sample_dir / 'sample_link_patents.parquet', index=False)
    print(f"Saved {len(patents_df)} patent links")
    
    print("Extracting fields...")
    field_ids = set(pf_df['fieldid'].unique().tolist())
    field_ids_str = "', '".join(field_ids)
    fields_query = f"""
    SELECT * FROM read_parquet('{fields_path}')
    WHERE fieldid IN ('{field_ids_str}')
    """
    fields_df = conn.execute(fields_query).df()
    fields_df.to_parquet(sample_dir / 'sample_fields.parquet', index=False)
    print(f"Saved {len(fields_df)} fields")
    
    conn.close()
    
    print(f"\nSample dataset created successfully in {sample_dir}")
    print(f"Total papers: {len(paper_ids)}")


if __name__ == '__main__':
    create_sample_dataset()

