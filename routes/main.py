# routes/main.py
"""Routes for the landing page.

The home page provides a quick overview of the database content. To keep the
route lean, the heavy lifting (counting documents and fetching the latest
event) lives in ``services.main_service``.
"""

from flask import Blueprint, render_template

from services.main_service import fetch_main_stats

main_bp = Blueprint("main", __name__)

@main_bp.route("/")
def show_home():
    """Render the application dashboard with basic statistics."""

    stats = fetch_main_stats()
    return render_template("main.html", stats=stats)
