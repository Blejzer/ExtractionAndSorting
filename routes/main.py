# routes/main.py
from flask import Blueprint, render_template, url_for
from config.database import mongodb_connection

main_bp = Blueprint("main", __name__)

@main_bp.route("/")
def show_home():
    participants_col = mongodb_connection.participants
    events_col = mongodb_connection.events
    countries_col = mongodb_connection.countries

    stats = {
        "participants": participants_col.count_documents({}),
        "events": events_col.count_documents({}),
        "countries": countries_col.count_documents({}),
    }

    # optional: latest event
    latest_event = events_col.find().sort("dateFrom", -1).limit(1)
    latest = next(latest_event, None)
    stats["latest_event"] = latest["title"] if latest and latest.get("title") else (latest["eid"] if latest else None)
    stats["latest_event_date"] = latest.get("dateFrom") if latest else None

    return render_template("main.html", stats=stats)
