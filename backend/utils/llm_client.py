"""
LLM client wrapper for AWS Bedrock.
"""

from langchain_aws import ChatBedrock
from langchain_core.prompts import ChatPromptTemplate
from config import BEDROCK_REGION, BEDROCK_MODEL_ID


def create_llm():
    """Create and return Bedrock LLM client."""
    return ChatBedrock(
        model_id=BEDROCK_MODEL_ID,
        region_name=BEDROCK_REGION,
        model_kwargs={
            "temperature": 0.1,
            "max_tokens": 4000
        }
    )


def create_prompt_template(template: str) -> ChatPromptTemplate:
    """Create a prompt template."""
    return ChatPromptTemplate.from_template(template)

