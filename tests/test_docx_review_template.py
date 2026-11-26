from flask import Flask, render_template
from pathlib import Path


def _build_app() -> Flask:
    root = Path(__file__).resolve().parents[1]
    app = Flask(__name__, template_folder=str(root / "templates"), static_folder=str(root / "static"))
    app.secret_key = "test"

    app.add_url_rule("/home", endpoint="main.show_home", view_func=lambda: "home")
    app.add_url_rule("/participants", endpoint="participants.show_participants", view_func=lambda: "participants")
    app.add_url_rule("/events", endpoint="events.show_events", view_func=lambda: "events")
    app.add_url_rule("/imports", endpoint="imports.upload_form", view_func=lambda: "imports")
    app.add_url_rule("/docx", endpoint="docx_upload.upload_docx", view_func=lambda: "docx")
    app.add_url_rule("/docx/commit", endpoint="docx_upload.commit_docx", view_func=lambda: "commit", methods=["POST"])
    app.add_url_rule("/docx/compare", endpoint="docx_upload.compare_docx", view_func=lambda: "compare", methods=["POST"])
    app.add_url_rule("/login", endpoint="auth.login", view_func=lambda: "login")
    app.add_url_rule("/logout", endpoint="auth.logout", view_func=lambda: "logout", methods=["POST"])

    return app


def test_docx_compare_template_renders_without_loop_parent_error() -> None:
    app = _build_app()
    compared = [
        {
            "extracted": {"name": "Alice Doe", "email": "alice@example.com"},
            "existing": {"name": "Alice Doe", "email": "old@example.com"},
            "score": 0.93,
        }
    ]

    with app.test_request_context("/docx"):
        html = render_template("docx_review.html", stage="compare", compared=compared, eid="E001")

    assert "choice_0_name" in html
    assert "choice_0_email" in html
    assert "Matched (0.93)" in html
