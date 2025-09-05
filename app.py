import os
from flask import Flask, jsonify
from config.database import mongodb
from repositories.country_repository import CountryRepository
from repositories.participant_repository import ParticipantRepository
from middleware.auth import login_required
from routes.auth import auth_bp
from routes.main import main_bp
from routes.participants import participants_bp
from routes.events import events_bp
from routes.tests import tests_bp

def create_app() -> Flask:
    """
    Flask application factory.
    - Registers blueprints (later).
    - Initializes extensions.
    """
    app = Flask(__name__)
    app.secret_key = os.getenv("SECRET_KEY", "dev")
    app.register_blueprint(auth_bp)
    app.register_blueprint(main_bp)
    app.register_blueprint(participants_bp)
    app.register_blueprint(events_bp)
    app.register_blueprint(tests_bp)


    @app.route("/health", methods=["GET"])
    @login_required
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


    @app.route("/stats", methods=["GET"])
    @login_required
    def get_stats():
        """
        Get statistics about the database including counts of participants and countries.
        """
        try:
            # Initialize repositories
            country_repo = CountryRepository()
            participant_repo = ParticipantRepository()

            # Get counts
            participants_count = participant_repo.collection.count_documents({})
            countries_count = country_repo.collection.count_documents({})

            return jsonify({
                "status": "ok",
                "participants_count": participants_count,
                "countries_count": countries_count,
                "message": "Successfully retrieved statistics"
            }), 200

        except Exception as e:
            return jsonify({
                "status": "error",
                "message": f"Failed to retrieve statistics: {str(e)}"
            }), 500

    @app.route("/stats/detailed", methods=["GET"])
    @login_required
    def get_detailed_stats():
        """
        Get more detailed statistics about the database.
        """
        try:
            # Initialize repositories
            country_repo = CountryRepository()
            participant_repo = ParticipantRepository()

            # Get basic counts
            participants_count = participant_repo.collection.count_documents({})
            countries_count = country_repo.collection.count_documents({})

            # Get counts by grade
            from domain.models.participant import Grade
            black_list_count = participant_repo.collection.count_documents({"grade": Grade.BLACK_LIST.value})
            normal_count = participant_repo.collection.count_documents({"grade": Grade.NORMAL.value})
            excellent_count = participant_repo.collection.count_documents({"grade": Grade.EXCELLENT.value})

            # Get participants by country (top 5)
            pipeline = [
                {"$group": {"_id": "$representing_country", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 5}
            ]
            participants_by_country = list(participant_repo.collection.aggregate(pipeline))

            return jsonify({
                "status": "ok",
                "participants_count": participants_count,
                "countries_count": countries_count,
                "participants_by_grade": {
                    "black_list": black_list_count,
                    "normal": normal_count,
                    "excellent": excellent_count
                },
                "top_countries": participants_by_country,
                "message": "Successfully retrieved detailed statistics"
            }), 200

        except Exception as e:
            return jsonify({
                "status": "error",
                "message": f"Failed to retrieve detailed statistics: {str(e)}"
            }), 500

    return app


# at bottom of app.py
if __name__ == "__main__":
    from os import getenv

    app = create_app()
    app.run(
        host="0.0.0.0",
        port=int(getenv("PORT", 5000)),
        debug=getenv("FLASK_DEBUG", "0") == "1",
        use_reloader=False,  # <- important
    )