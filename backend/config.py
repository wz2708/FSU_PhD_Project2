"""
Configuration settings for the backend application.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BACKEND_DIR = Path(__file__).parent
PROJECT_ROOT = BACKEND_DIR.parent
DATA_DIR = str(PROJECT_ROOT / 'data')
SAMPLE_DATA_DIR = str(BACKEND_DIR / 'data')

API_HOST = os.environ.get('API_HOST', '0.0.0.0')
API_PORT = int(os.environ.get('PORT', 5000))

CORS_ORIGINS = os.environ.get('CORS_ORIGINS', '*').split(',')

UNIVERSITY = 'Columbia University'
FIELD = 'Computer Science'
YEARS_BACK = 5

BEDROCK_REGION = os.environ.get('BEDROCK_REGION', 'us-east-2')
BEDROCK_MODEL_ID = os.environ.get('BEDROCK_MODEL_ID', 'us.anthropic.claude-sonnet-4-20250514-v1:0')

COLUMBIA_INSTITUTION_ID = 'I78577930'
CS_FIELD_ID = 'C41008148'

