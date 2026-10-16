import os
import sys
from flask import Flask



# Ensure the project root is on sys.path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from routes.auth import auth_bp
from routes.participants import participants_bp
from routes.main import main_bp
import middleware.auth as m_auth
import routes.auth as r_auth

import pytest

@pytest.fixture
def app(monkeypatch):
    monkeypatch.setattr(m_auth, "ensure_default_users", lambda: None)
    template_dir = os.path.join(os.path.dirname(__file__), "..", "templates")
    app = Flask(__name__, template_folder=template_dir)
    app.secret_key = 'test'
    app.register_blueprint(auth_bp)
    app.register_blueprint(main_bp)
    app.register_blueprint(participants_bp)

    # Minimal events blueprint so base.html links resolve
    from flask import Blueprint

    events_bp = Blueprint("events", __name__)

    @events_bp.route("/events")
    def show_events():
        return "events"

    imports_bp = Blueprint("imports", __name__)

    @imports_bp.route("/import")
    def upload_form():
        return "imports"

    app.register_blueprint(events_bp)
    app.register_blueprint(imports_bp)

    from middleware.auth import login_required

    @app.route("/private")
    @login_required
    def private():
        return "private"
    return app

@pytest.fixture
def client(app):
    return app.test_client()

def test_protected_route_requires_login(client):
    resp = client.get('/private')
    assert resp.status_code == 302
    assert '/login' in resp.headers['Location']

def test_login_success_allows_access(client, monkeypatch):
    monkeypatch.setattr(r_auth, 'authenticate', lambda u, p: {'username': u} if (u, p) == ('user', 'pass') else None)
    resp = client.post('/login', data={'username': 'user', 'password': 'pass'})
    assert resp.status_code == 302
    assert resp.headers['Location'].endswith('/')
    resp2 = client.get('/private')
    assert resp2.status_code == 200

def test_login_failure(client, monkeypatch):
    monkeypatch.setattr(r_auth, 'authenticate', lambda u, p: None)
    resp = client.post('/login', data={'username': 'user', 'password': 'wrong'})
    assert resp.status_code == 200
    assert b'Login' in resp.data


def test_login_respects_next_on_success(client, monkeypatch):
    monkeypatch.setattr(r_auth, 'authenticate', lambda u, p: {'username': u})
    resp = client.post('/login?next=/private', data={'username': 'user', 'password': 'pass'})
    assert resp.status_code == 302
    assert resp.headers['Location'].endswith('/private')


def test_login_ignores_external_next(client, monkeypatch):
    monkeypatch.setattr(r_auth, 'authenticate', lambda u, p: {'username': u})
    resp = client.post('/login?next=http://evil.com', data={'username': 'user', 'password': 'pass'})
    assert resp.status_code == 302
    # Should redirect to home since external next is unsafe
    assert resp.headers['Location'].endswith('/')
