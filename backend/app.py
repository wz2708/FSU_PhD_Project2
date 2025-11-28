"""
Flask application for Project 2.
"""

from flask import Flask
from flask_cors import CORS
from config import API_HOST, API_PORT, CORS_ORIGINS
from api.chat import chat_bp


def create_app():
    """Create and configure Flask application."""
    app = Flask(__name__)
    
    CORS(app, origins=CORS_ORIGINS)
    
    app.register_blueprint(chat_bp, url_prefix='/api/chat')
    
    @app.route('/health', methods=['GET'])
    def health():
        return {"status": "ok"}, 200
    
    return app


if __name__ == '__main__':
    app = create_app()
    app.run(host=API_HOST, port=API_PORT, debug=True)

