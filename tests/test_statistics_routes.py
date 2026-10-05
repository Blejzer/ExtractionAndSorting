from datetime import date
from pathlib import Path

from flask import Blueprint, Flask, url_for
from pymongo.errors import ConnectionFailure
import pytest

import routes.statistics as statistics_routes
from services.statistics_service import build_statistics


@pytest.fixture
def app(monkeypatch):
    app = Flask(__name__, template_folder=str(Path(__file__).resolve().parents[1] / "templates"))
    app.secret_key = "test"
    app.config["LOGIN_DISABLED"] = True
    app.register_blueprint(statistics_routes.statistics_bp)
    for name, endpoints in {"main": ["show_home"], "participants": ["show_participants"],
                            "events": ["show_events", "event_detail", "edit_event"],
                            "imports": ["upload_form"], "auth": ["login", "logout"]}.items():
        bp = Blueprint(name, __name__)
        for endpoint in endpoints:
            suffix = "/<eid>" if endpoint in ("event_detail", "edit_event") else ""
            bp.add_url_rule(f"/{name}/{endpoint}{suffix}", endpoint, lambda **kwargs: "stub")
        app.register_blueprint(bp)
    monkeypatch.delenv("STATISTICS_POLICY_CHANGE_DATE", raising=False)
    return app


def report(**kwargs):
    return build_statistics([dict(eid="E1", title="Cybercrime", start_date="2024-01-01", participants=["P"])],
                            [dict(pid="P", name="<script>alert(1)</script>", representing_country="AL", gender="Female",
                                  bio_short="15 years of police service. <script>alert(1)</script>")], [], [], as_of=date(2026, 10, 3), **kwargs)


def test_html_statistics_and_navigation_link_and_escaped_bio(app, monkeypatch):
    monkeypatch.setattr(statistics_routes, "fetch_statistics", report)
    response = app.test_client().get("/statistics?policy_change=2021-01-01")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    for section in ("Country attendance", "Training areas", "Participant diversity", "Professional experience from bios"):
        assert section in html
    assert 'href="/statistics"' in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html
    assert "<script>alert(1)</script>" not in html
    assert "15 years of police service" in html
    with app.test_request_context():
        assert url_for("statistics.show_statistics") == "/statistics"


def test_statistics_api_returns_matching_filtered_aggregates(app, monkeypatch):
    monkeypatch.setattr(statistics_routes, "fetch_statistics", report)
    response = app.test_client().get("/api/statistics?year=2024&area=cybercrime&policy_change=2021-01-01")
    assert response.status_code == 200
    assert response.json["summary"]["attendances"] == 1
    assert next(row for row in response.json["countries"] if row["code"] == "HR")["no_show_events"] == 1


def test_statistics_works_without_any_manual_configuration(app, monkeypatch):
    monkeypatch.setattr(statistics_routes, "fetch_statistics", report)
    response = app.test_client().get("/api/statistics")
    assert response.status_code == 200
    assert response.json["summary"]["unconfigured_events"] == 0
    assert next(row for row in response.json["countries"] if row["code"] == "HR")["shortfall"] == 3
    assert response.json["experience"]["known"] == 1
    html = app.test_client().get("/statistics").get_data(as_text=True)
    assert "transition is estimated at 2021-01-01" in html
    assert "need an invitation policy" not in html


def test_unavailable_country_comparisons_and_unlinked_profiles_are_explained(app, monkeypatch):
    monkeypatch.setattr(statistics_routes, "fetch_statistics", lambda **kwargs: build_statistics(
        [dict(eid="E1", title="Training", start_date="2024-01-01")],
        [dict(pid="P")], [dict(event_id="orphan", participant_id="P")], [], **kwargs))
    html = app.test_client().get("/statistics").get_data(as_text=True)
    assert "No attendance could be linked" in html
    assert "1 attendance link(s) do not match" in html
    assert "<td>—</td>" in html


@pytest.mark.parametrize("path", ["/statistics", "/api/statistics"])
def test_all_reporting_endpoints_require_login(app, path, monkeypatch):
    app.config["LOGIN_DISABLED"] = False
    monkeypatch.setattr(statistics_routes, "fetch_statistics", lambda **kwargs: pytest.fail("Must not query DB before login"))
    assert app.test_client().get(path).status_code == 302


@pytest.mark.parametrize("query", ["year=bad", "year=0", "area=unknown", "policy_change=2021-02-30"])
def test_invalid_filters_do_not_query_database(app, monkeypatch, query):
    monkeypatch.setattr(statistics_routes, "fetch_statistics", lambda **kwargs: pytest.fail("Invalid filters must not query DB"))
    response = app.test_client().get("/statistics?" + query)
    assert response.status_code == 400


def test_database_failure_returns_503_without_connection_details(app, monkeypatch):
    def fail(**kwargs):
        raise ConnectionFailure("secret://password@host")
    monkeypatch.setattr(statistics_routes, "fetch_statistics", fail)
    response = app.test_client().get("/api/statistics")
    assert response.status_code == 503
    assert "password" not in response.get_data(as_text=True)


def test_configured_policy_date_is_used_and_can_be_overridden(app, monkeypatch):
    monkeypatch.setenv("STATISTICS_POLICY_CHANGE_DATE", "2021-01-01")
    calls = []
    def fetch(**kwargs):
        calls.append(kwargs)
        return report(**kwargs)
    monkeypatch.setattr(statistics_routes, "fetch_statistics", fetch)
    app.test_client().get("/statistics")
    assert calls[-1]["policy_change"] == date(2021, 1, 1)
    app.test_client().get("/statistics?policy_change=")
    assert calls[-1]["policy_change"] is None


def test_prosecutor_bounds_role_and_country_evidence_render_in_html_and_json(app, monkeypatch):
    def prosecutor_report(**kwargs):
        return build_statistics(
            [dict(eid="E1", title="Organized crime", start_date="2026-05-04", participants=["P0104"])],
            [dict(pid="P0104", name="Danica ARAPOVIĆ KOVAČEVIĆ", representing_country="C027",
                  position="Tuzla Canton Cantonal Prosecutor's Office / Organized Crime Department Head",
                  bio_short="I have been Cantonal prosecutor for over 20 years <script>alert(1)</script>.")],
            [], [dict(cid="C027", country="Bosnia and Herzegovina, Europe & Eurasia")],
            as_of=date(2026, 10, 5), **kwargs)
    monkeypatch.setattr(statistics_routes, "fetch_statistics", prosecutor_report)
    response = app.test_client().get("/api/statistics")
    assert response.status_code == 200
    record = response.json["experience"]["records"][0]
    assert record["display_years"] == "20+"
    assert record["scope"] == "Prosecution"
    assert record["country"] == "BiH"
    assert record["reference_date"] == "2026-05-04"
    html = app.test_client().get("/statistics").get_data(as_text=True)
    assert "<td>20+</td>" in html
    assert "Profile country" in html
    assert "Stated lower bound" in html
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html


def test_diversity_shows_only_requested_breakdowns_and_keeps_gender_by_country(app, monkeypatch):
    def sector_report(**kwargs):
        return build_statistics([dict(eid="E1", start_date="2026-05-04", participants=["P", "Q"])],
                                [dict(pid="P", representing_country="AL", gender="Male", organization="Police Directorate",
                                      rank="Captain", position="Police officer"),
                                 dict(pid="Q", representing_country="BA", gender="Female", organization="Cantonal Prosecutor's Office")],
                                [], [], **kwargs)
    monkeypatch.setattr(statistics_routes, "fetch_statistics", sector_report)
    response = app.test_client().get("/api/statistics")
    assert set(response.json["diversity"]) == {"gender", "age", "organization"}
    assert [row["label"] for row in response.json["diversity"]["organization"]] == ["Police", "Prosecutor"]
    html = app.test_client().get("/statistics").get_data(as_text=True)
    assert "Gender by country" in html
    assert "<th scope=\"row\">Police</th>" in html
    assert "<th scope=\"row\">Prosecutor</th>" in html
    for label in ("Stored rank", "Stored position", "Seniority / leadership", "Professional role", "Role / seniority",
                  "Police Directorate", "Cantonal Prosecutor&#39;s Office", "Captain"):
        assert label not in html
