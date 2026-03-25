from __future__ import annotations

import json
import secrets
import threading
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .config import CONFIG, STATIC_DIR, TEMPLATE_DIR, ensure_directories
from .store import FineStore
from .sync_manager import SyncManager


SESSION_COOKIE = "frutamina_session"

_env = Environment(
    loader=FileSystemLoader(str(TEMPLATE_DIR)),
    autoescape=select_autoescape(["html", "xml"]),
)
_store = FineStore()
_sync_manager = SyncManager(_store)
_sessions: dict[str, str] = {}
_sessions_lock = threading.Lock()


def _render(template_name: str, **context: object) -> str:
    return _env.get_template(template_name).render(**context)


def _create_session(username: str) -> str:
    token = secrets.token_urlsafe(32)
    with _sessions_lock:
        _sessions[token] = username
    return token


def _current_user(handler: SimpleHTTPRequestHandler) -> str | None:
    raw_cookie = handler.headers.get("Cookie")
    if not raw_cookie:
        return None

    cookie = SimpleCookie()
    cookie.load(raw_cookie)
    token = cookie.get(SESSION_COOKIE)
    if token is None:
        return None

    with _sessions_lock:
        return _sessions.get(token.value)


def _clear_session(handler: SimpleHTTPRequestHandler) -> None:
    raw_cookie = handler.headers.get("Cookie")
    if not raw_cookie:
        return

    cookie = SimpleCookie()
    cookie.load(raw_cookie)
    token = cookie.get(SESSION_COOKIE)
    if token is None:
        return

    with _sessions_lock:
        _sessions.pop(token.value, None)


def _read_form(handler: SimpleHTTPRequestHandler) -> dict[str, str]:
    length = int(handler.headers.get("Content-Length", "0"))
    raw_body = handler.rfile.read(length).decode("utf-8")
    parsed = parse_qs(raw_body, keep_blank_values=True)
    return {key: values[0] for key, values in parsed.items()}


def _send_html(handler: SimpleHTTPRequestHandler, html: str, status: HTTPStatus = HTTPStatus.OK) -> None:
    payload = html.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Content-Length", str(len(payload)))
    handler.end_headers()
    handler.wfile.write(payload)


def _send_json(handler: SimpleHTTPRequestHandler, payload: dict[str, object], status: HTTPStatus = HTTPStatus.OK) -> None:
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(raw)))
    handler.end_headers()
    handler.wfile.write(raw)


def _redirect(handler: SimpleHTTPRequestHandler, location: str, cookie_header: str | None = None) -> None:
    handler.send_response(HTTPStatus.SEE_OTHER)
    handler.send_header("Location", location)
    if cookie_header:
        handler.send_header("Set-Cookie", cookie_header)
    handler.end_headers()


class AppHandler(SimpleHTTPRequestHandler):
    def translate_path(self, path: str) -> str:
        route_path = urlparse(path).path
        if route_path.startswith("/static/"):
            relative = route_path.removeprefix("/static/")
            return str((STATIC_DIR / relative).resolve())
        return super().translate_path(path)

    def do_GET(self) -> None:
        route = urlparse(self.path).path

        if route == "/":
            _redirect(self, "/dashboard" if _current_user(self) else "/login")
            return

        if route == "/login":
            _send_html(self, _render("login.html", page_title="Login | Frutamina Multas", error_message=""))
            return

        if route == "/logout":
            _clear_session(self)
            _redirect(
                self,
                "/login",
                cookie_header=f"{SESSION_COOKIE}=; Path=/; HttpOnly; Max-Age=0; SameSite=Lax",
            )
            return

        if route == "/dashboard":
            username = _current_user(self)
            if not username:
                _redirect(self, "/login")
                return

            payload = _store.build_dashboard_payload()
            _send_html(
                self,
                _render(
                    "dashboard.html",
                    page_title="Dashboard | Frutamina Multas",
                    username=username,
                    initial_payload_json=json.dumps(payload, ensure_ascii=False),
                    sync_snapshot_json=json.dumps(_sync_manager.snapshot(), ensure_ascii=False),
                    mock_mode=CONFIG.mock_sync,
                ),
            )
            return

        if route == "/api/dashboard-data":
            if not _current_user(self):
                _send_json(self, {"error": "Nao autenticado."}, HTTPStatus.UNAUTHORIZED)
                return
            _send_json(self, _store.build_dashboard_payload())
            return

        if route == "/api/sync-status":
            if not _current_user(self):
                _send_json(self, {"error": "Nao autenticado."}, HTTPStatus.UNAUTHORIZED)
                return
            _send_json(self, _sync_manager.snapshot())
            return

        if route == "/export/csv":
            if not _current_user(self):
                _redirect(self, "/login")
                return

            csv_path = _store.csv_path()
            if not csv_path.exists():
                self.send_error(HTTPStatus.NOT_FOUND, "Nenhum CSV gerado ainda.")
                return

            payload = csv_path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="multas_ativas.csv"')
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if route.startswith("/downloads/"):
            if not _current_user(self):
                _redirect(self, "/login")
                return

            filename = Path(unquote(route.removeprefix("/downloads/"))).name
            file_path = (_store.downloads_dir() / filename).resolve()
            if not file_path.exists() or file_path.parent != _store.downloads_dir().resolve():
                self.send_error(HTTPStatus.NOT_FOUND, "Arquivo nao encontrado.")
                return

            payload = file_path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/pdf")
            self.send_header("Content-Disposition", f'inline; filename="{filename}"')
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if route.startswith("/static/"):
            return super().do_GET()

        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        route = urlparse(self.path).path

        if route == "/login":
            form = _read_form(self)
            username = (form.get("username") or "").strip()
            password = form.get("password") or ""
            if username == CONFIG.dashboard_user and password == CONFIG.dashboard_password:
                token = _create_session(username)
                _redirect(
                    self,
                    "/dashboard",
                    cookie_header=f"{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Lax",
                )
                return

            _send_html(
                self,
                _render(
                    "login.html",
                    page_title="Login | Frutamina Multas",
                    error_message="Usuario ou senha invalidos.",
                ),
                HTTPStatus.UNAUTHORIZED,
            )
            return

        if route == "/api/sync-start":
            if not _current_user(self):
                _send_json(self, {"error": "Nao autenticado."}, HTTPStatus.UNAUTHORIZED)
                return

            started = _sync_manager.start()
            if not started:
                _send_json(self, {"ok": False, "message": "Ja existe uma sincronizacao em andamento."}, HTTPStatus.CONFLICT)
                return

            _send_json(self, {"ok": True, "message": "Sincronizacao iniciada."}, HTTPStatus.ACCEPTED)
            return

        self.send_error(HTTPStatus.NOT_FOUND)

    def log_message(self, format: str, *args: object) -> None:
        return


def create_server(host: str | None = None, port: int | None = None) -> ThreadingHTTPServer:
    ensure_directories()
    return ThreadingHTTPServer((host or CONFIG.app_host, port or CONFIG.app_port), AppHandler)


def run() -> None:
    server = create_server()
    print(f"Aplicacao disponivel em http://{CONFIG.app_host}:{server.server_port}/login")
    print(f"Usuario do dashboard: {CONFIG.dashboard_user}")
    print("Use Ctrl+C para encerrar.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServidor encerrado.")
    finally:
        server.server_close()
