import os
from flask import Flask, jsonify
from config.database import mongodb



def create_app() -> Flask:
    """
    Flask application factory.
    - Registers blueprints (later).
    - Initializes extensions.
    """
    app = Flask(__name__)

    @app.route("/health", methods=["GET"])
    def health_check():
        """
        Simple health-check route.
        Verifies app is running and DB connection is alive.
        """
        try:
            mongodb.client.admin.command("ping")
            db_status = "ok"
        except Exception as e:
            db_status = f"error: {str(e)}"

        return jsonify({
            "status": "ok",
            "database": db_status
        }), 200

    return app


# at bottom of app.py
if __name__ == "__main__":
    from os import getenv
    app = create_app()
    app.run(
        host="0.0.0.0",
        port=int(getenv("PORT", 3000)),
        debug=getenv("FLASK_DEBUG", "0") == "1",
        use_reloader=False,   # <- important
    )