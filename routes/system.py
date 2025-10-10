"""System endpoints (health check and stats)."""

from flask import Blueprint, jsonify

from config.database import mongodb
from middleware.auth import login_required
from repositories.country_repository import CountryRepository

system_bp = Blueprint("system", __name__)


@system_bp.route("/health", methods=["GET"])
def health_check():
    """Simple health-check route without authentication."""
    try:
        mongodb.client.admin.command("ping")
        db_status = "ok"
    except Exception as e:  # pragma: no cover - best effort
        db_status = f"error: {str(e)}"

    return jsonify({
        "status": "ok",
        "database": db_status,
    }), 200


@system_bp.route("/stats", methods=["GET"])
@login_required
def get_stats():
    """Return basic database statistics."""
    try:
        country_repo = CountryRepository()
        from repositories.participant_repository import ParticipantRepository

        participant_repo = ParticipantRepository()
        participants_count = participant_repo.collection.count_documents({})
        countries_count = country_repo.collection.count_documents({})

        return jsonify({
            "status": "ok",
            "participants_count": participants_count,
            "countries_count": countries_count,
            "message": "Successfully retrieved statistics",
        }), 200
    except Exception as e:  # pragma: no cover - best effort
        return jsonify({
            "status": "error",
            "message": f"Failed to retrieve statistics: {str(e)}",
        }), 500


@system_bp.route("/stats/detailed", methods=["GET"])
@login_required
def get_detailed_stats():
    """Return detailed database statistics."""
    try:
        country_repo = CountryRepository()
        from repositories.participant_repository import ParticipantRepository

        participant_repo = ParticipantRepository()
        participants_count = participant_repo.collection.count_documents({})
        countries_count = country_repo.collection.count_documents({})

        from domain.models.participant import Grade

        black_list_count = participant_repo.collection.count_documents({"grade": Grade.BLACK_LIST.value})
        normal_count = participant_repo.collection.count_documents({"grade": Grade.NORMAL.value})
        excellent_count = participant_repo.collection.count_documents({"grade": Grade.EXCELLENT.value})

        pipeline = [
            {"$group": {"_id": "$representing_country", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 5},
        ]
        participants_by_country = list(participant_repo.collection.aggregate(pipeline))

        return jsonify({
            "status": "ok",
            "participants_count": participants_count,
            "countries_count": countries_count,
            "participants_by_grade": {
                "black_list": black_list_count,
                "normal": normal_count,
                "excellent": excellent_count,
            },
            "top_countries": participants_by_country,
            "message": "Successfully retrieved detailed statistics",
        }), 200
    except Exception as e:  # pragma: no cover - best effort
        return jsonify({
            "status": "error",
            "message": f"Failed to retrieve detailed statistics: {str(e)}",
        }), 500
