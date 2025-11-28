"""
Chat API endpoints.
"""

from flask import Blueprint, jsonify, request
from agents.orchestrator import Orchestrator
from processors.query_executor import QueryExecutor
from config import SAMPLE_DATA_DIR

chat_bp = Blueprint('chat', __name__)

query_executor = QueryExecutor(data_dir=SAMPLE_DATA_DIR)
orchestrator = Orchestrator(query_executor)


@chat_bp.route('/message', methods=['POST'])
def handle_message():
    """Handle chat message and return response with visualization."""
    try:
        data = request.get_json()
        user_query = data.get('message', '').strip()
        
        if not user_query:
            return jsonify({
                "success": False,
                "error": "Empty message"
            }), 400
        
        result = orchestrator.process_query(user_query)
        
        if result.get("success"):
            return jsonify(result), 200
        else:
            return jsonify(result), 500
    
    except Exception as e:
        import traceback
        return jsonify({
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500