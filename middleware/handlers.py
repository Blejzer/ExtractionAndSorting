#TODO add automatic logging to file (logs/errors.log) inside handlers.py — so each error is written with timestamp, class, and message

"""
Global Flask error handling middleware.

All exceptions (custom or unexpected) are returned as JSON payloads:
{
    "status": "error",
    "error": "ErrorClassName",
    "message": "Human readable message",
    "details": { ... optional context ... }
}
"""

from flask import jsonify
from middleware.errors import BaseAppError
import traceback
import os


def register_error_handlers(app):
    """Attach all JSON error handlers to a Flask app instance."""

    @app.errorhandler(BaseAppError)
    def handle_custom_error(err):
        """Handle custom, domain-specific errors."""
        response = jsonify(err.to_dict())
        response.status_code = err.code
        return response

    @app.errorhandler(Exception)
    def handle_unexpected_error(err):
        """Catch-all handler for unexpected exceptions."""
        details = {}
        if app.debug or os.getenv("FLASK_DEBUG") == "1":
            details["traceback"] = traceback.format_exc()

        payload = {
            "status": "error",
            "error": err.__class__.__name__,
            "message": str(err) or "Unexpected internal error",
            "details": details
        }
        return jsonify(payload), 500
