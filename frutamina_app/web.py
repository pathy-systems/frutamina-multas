from __future__ import annotations

import json
import secrets
import threading
import time
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlencode, urlparse

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .config import CONFIG, STATIC_DIR, TEMPLATE_DIR, ensure_directories
from .models import FineRecord
from .store import FineStore
from .sync_manager import SyncManager


SESSION_COOKIE = "frutamina_session"
SESSION_ONLINE_WINDOW_SECONDS = 300

_env = Environment(
    loader=FileSystemLoader(str(TEMPLATE_DIR)),
    autoescape=select_autoescape(["html", "xml"]),
)
_store = FineStore()
_sync_manager = SyncManager(_store)
_sessions: dict[str, dict[str, object]] = {}
_sessions_lock = threading.Lock()


def _render(template_name: str, **context: object) -> str:
    return _env.get_template(template_name).render(**context)


def _create_session(username: str) -> str:
    token = secrets.token_urlsafe(32)
    now = time.time()
    with _sessions_lock:
        _sessions[token] = {
            "username": username,
            "created_at": now,
            "last_seen_at": now,
        }
    return token


def _session_record(handler: SimpleHTTPRequestHandler, touch: bool = True) -> dict[str, object] | None:
    raw_cookie = handler.headers.get("Cookie")
    if not raw_cookie:
        return None

    cookie = SimpleCookie()
    cookie.load(raw_cookie)
    token = cookie.get(SESSION_COOKIE)
    if token is None:
        return None

    with _sessions_lock:
        record = _sessions.get(token.value)
        if not record:
            return None
        if touch:
            record["last_seen_at"] = time.time()
        return dict(record)


def _active_usernames() -> set[str]:
    threshold = time.time() - SESSION_ONLINE_WINDOW_SECONDS
    active: set[str] = set()
    with _sessions_lock:
        stale_tokens = [
            token
            for token, record in _sessions.items()
            if float(record.get("last_seen_at", 0) or 0) < threshold
        ]
        for token in stale_tokens:
            _sessions.pop(token, None)
        for record in _sessions.values():
            username = str(record.get("username") or "")
            if username:
                active.add(username)
    return active


def _current_user(handler: SimpleHTTPRequestHandler) -> dict[str, object] | None:
    session = _session_record(handler)
    username = str(session.get("username") or "") if session else ""
    if not username:
        return None
    user = _store.get_user(username)
    if not isinstance(user, dict):
        return None
    if not user.get("is_active", False):
        return None
    user["roleLabel"] = "Administrador" if user.get("role") == "admin" else "Operador"
    return user


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


def _read_json(handler: SimpleHTTPRequestHandler) -> dict[str, object]:
    length = int(handler.headers.get("Content-Length", "0"))
    raw_body = handler.rfile.read(length) if length > 0 else b"{}"
    if not raw_body:
        return {}
    return json.loads(raw_body.decode("utf-8"))


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


def _dashboard_location(message: str = "", tone: str = "info", anchor: str = "") -> str:
    query = urlencode({"admin_message": message, "admin_tone": tone}) if message else ""
    location = "/dashboard"
    if query:
        location += f"?{query}"
    if anchor:
        location += f"#{anchor}"
    return location


def _users_location(message: str = "", tone: str = "info") -> str:
    query = urlencode({"admin_message": message, "admin_tone": tone}) if message else ""
    location = "/admin/users"
    if query:
        location += f"?{query}"
    return location


def _login_location(message: str = "", tone: str = "info", modal: str = "") -> str:
    payload: dict[str, str] = {}
    if message:
        payload["auth_message"] = message
        payload["auth_tone"] = tone
    if modal:
        payload["auth_modal"] = modal
    query = urlencode(payload) if payload else ""
    return f"/login?{query}" if query else "/login"


def _agent_request_authorized(handler: SimpleHTTPRequestHandler) -> bool:
    if not CONFIG.sync_agent_token:
        return False

    auth_header = handler.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header.removeprefix("Bearer ").strip()
        return secrets.compare_digest(token, CONFIG.sync_agent_token)

    header_token = handler.headers.get("X-Agent-Token", "")
    if header_token:
        return secrets.compare_digest(header_token, CONFIG.sync_agent_token)
    return False


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
            params = parse_qs(urlparse(self.path).query, keep_blank_values=True)
            _send_html(
                self,
                _render(
                    "login.html",
                    page_title="Login | Frutamina Multas",
                    error_message="",
                    auth_message=(params.get("auth_message") or [""])[0],
                    auth_tone=(params.get("auth_tone") or ["info"])[0],
                    auth_modal=(params.get("auth_modal") or [""])[0],
                ),
            )
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
            user = _current_user(self)
            if not user:
                _redirect(self, "/login")
                return

            payload = _store.build_dashboard_payload()
            _send_html(
                self,
                _render(
                    "dashboard.html",
                    page_title="Dashboard | Frutamina Multas",
                    username=user.get("display_name") or user.get("username") or "",
                    current_user=user,
                    is_admin=user.get("role") == "admin",
                    initial_payload_json=json.dumps(payload, ensure_ascii=False),
                    sync_snapshot_json=json.dumps(_store.get_sync_snapshot(), ensure_ascii=False),
                    mock_mode=CONFIG.mock_sync,
                    sync_mode=CONFIG.sync_mode,
                    database_enabled=_store.uses_database,
                    recent_jobs=_store.list_recent_jobs(),
                ),
            )
            return

        if route == "/admin/users":
            user = _current_user(self)
            if not user:
                _redirect(self, "/login")
                return
            if user.get("role") != "admin":
                _redirect(self, _dashboard_location("Somente administradores podem acessar a gestao de usuarios.", "error"))
                return

            active_usernames = _active_usernames()
            users = []
            for row in _store.list_users():
                username = str(row.get("username") or "")
                row["isOnline"] = username in active_usernames
                row["onlineLabel"] = "Online" if row["isOnline"] else "Offline"
                row["passwordLabel"] = "Protegida"
                users.append(row)

            params = parse_qs(urlparse(self.path).query, keep_blank_values=True)
            _send_html(
                self,
                _render(
                    "users.html",
                    page_title="Usuarios | Frutamina Multas",
                    username=user.get("display_name") or user.get("username") or "",
                    current_user=user,
                    is_admin=True,
                    users=users,
                    pending_requests=_store.list_account_requests("pending"),
                    pending_password_resets=_store.list_password_reset_requests("pending"),
                    admin_message=(params.get("admin_message") or [""])[0],
                    admin_tone=(params.get("admin_tone") or ["info"])[0],
                    mock_mode=CONFIG.mock_sync,
                    sync_mode=CONFIG.sync_mode,
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
            snapshot = _store.get_sync_snapshot()
            snapshot["mode"] = CONFIG.sync_mode
            snapshot["jobs"] = _store.list_recent_jobs(5)
            snapshot["agent_status"] = _store.get_agent_status()
            _send_json(self, snapshot)
            return

        if route == "/api/fine-history":
            if not _current_user(self):
                _send_json(self, {"error": "Nao autenticado."}, HTTPStatus.UNAUTHORIZED)
                return
            params = parse_qs(urlparse(self.path).query, keep_blank_values=True)
            auto = (params.get("auto") or [""])[0]
            processo = (params.get("processo") or [""])[0]
            history = _store.get_fine_history(auto, processo)
            _send_json(self, {"history": history, "auto": auto, "processo": processo})
            return

        if route == "/api/agent/jobs/next":
            if not _agent_request_authorized(self):
                _send_json(self, {"error": "Agente nao autorizado."}, HTTPStatus.UNAUTHORIZED)
                return

            payload = _read_json(self) if self.command == "POST" else {}
            agent_name = str(payload.get("agent_name") or CONFIG.sync_agent_name)
            job = _store.claim_next_job(agent_name)
            _send_json(self, {"job": job})
            return

        if route == "/export/csv":
            if not _current_user(self):
                _redirect(self, "/login")
                return

            payload = _store.build_csv_bytes()
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
            pdf_payload = _store.read_pdf(filename)
            if not pdf_payload:
                self.send_error(HTTPStatus.NOT_FOUND, "Arquivo nao encontrado.")
                return

            payload, content_type = pdf_payload
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
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
            user = _store.authenticate_user(username, password)
            if user:
                token = _create_session(str(user.get("username") or username))
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
                    auth_message="",
                    auth_tone="info",
                    auth_modal="",
                ),
                HTTPStatus.UNAUTHORIZED,
            )
            return

        if route == "/account-request":
            form = _read_form(self)
            ok, message = _store.submit_account_request(
                display_name=form.get("display_name", ""),
                username=form.get("username", ""),
                password=form.get("password", ""),
            )
            _redirect(self, _login_location(message, "info" if ok else "error", "account"))
            return

        if route == "/password-reset-request":
            form = _read_form(self)
            ok, message = _store.submit_password_reset_request(
                username=form.get("username", ""),
                display_name=form.get("display_name", ""),
            )
            _redirect(self, _login_location(message, "info" if ok else "error", "reset"))
            return

        if route == "/admin/users/create":
            user = _current_user(self)
            if not user:
                _redirect(self, "/login")
                return
            if user.get("role") != "admin":
                _redirect(self, _dashboard_location("Somente administradores podem gerenciar usuarios.", "error"))
                return

            form = _read_form(self)
            ok, message = _store.create_user(
                username=form.get("new_username", ""),
                password=form.get("new_password", ""),
                role=form.get("new_role", "operador"),
                display_name=form.get("new_display_name", ""),
                actor=str(user.get("username") or ""),
            )
            _redirect(self, _users_location(message, "info" if ok else "error"))
            return

        if route == "/admin/requests/review":
            user = _current_user(self)
            if not user:
                _redirect(self, "/login")
                return
            if user.get("role") != "admin":
                _redirect(self, _dashboard_location("Somente administradores podem gerenciar solicitacoes.", "error"))
                return

            form = _read_form(self)
            ok, message = _store.review_account_request(
                request_id=form.get("request_id", ""),
                action=form.get("action", ""),
                actor=str(user.get("username") or ""),
                note=form.get("review_note", ""),
            )
            _redirect(self, _users_location(message, "info" if ok else "error"))
            return

        if route == "/admin/password-reset/review":
            user = _current_user(self)
            if not user:
                _redirect(self, "/login")
                return
            if user.get("role") != "admin":
                _redirect(self, _dashboard_location("Somente administradores podem gerenciar redefinicoes.", "error"))
                return

            form = _read_form(self)
            ok, message = _store.review_password_reset_request(
                request_id=form.get("request_id", ""),
                action=form.get("action", ""),
                actor=str(user.get("username") or ""),
                new_password=form.get("new_password", ""),
                note=form.get("review_note", ""),
            )
            _redirect(self, _users_location(message, "info" if ok else "error"))
            return

        if route == "/admin/users/update":
            user = _current_user(self)
            if not user:
                _redirect(self, "/login")
                return
            if user.get("role") != "admin":
                _redirect(self, _dashboard_location("Somente administradores podem gerenciar usuarios.", "error"))
                return

            form = _read_form(self)
            ok, message = _store.update_user(
                username=form.get("username", ""),
                display_name=form.get("display_name", ""),
                role=form.get("role", "operador"),
                is_active=form.get("is_active") == "1",
                new_password=form.get("new_password", ""),
                actor=str(user.get("username") or ""),
                acting_username=str(user.get("username") or ""),
            )
            _redirect(self, _users_location(message, "info" if ok else "error"))
            return

        if route == "/admin/users/delete":
            user = _current_user(self)
            if not user:
                _redirect(self, "/login")
                return
            if user.get("role") != "admin":
                _redirect(self, _dashboard_location("Somente administradores podem remover usuarios.", "error"))
                return

            form = _read_form(self)
            ok, message = _store.delete_user(
                username=form.get("username", ""),
                acting_username=str(user.get("username") or ""),
            )
            _redirect(self, _users_location(message, "info" if ok else "error"))
            return

        if route == "/api/sync-start":
            user = _current_user(self)
            if not user:
                _send_json(self, {"error": "Nao autenticado."}, HTTPStatus.UNAUTHORIZED)
                return

            if CONFIG.sync_mode == "agent":
                created, payload = _store.request_sync(str(user.get("username") or ""))
                if not created:
                    _send_json(self, {"ok": False, "message": payload}, HTTPStatus.CONFLICT)
                    return
                _send_json(
                    self,
                    {
                        "ok": True,
                        "message": "Solicitacao criada. O agente local vai processar a leitura.",
                        "job_id": payload,
                    },
                    HTTPStatus.ACCEPTED,
                )
                return

            started = _sync_manager.start()
            if not started:
                _send_json(self, {"ok": False, "message": "Ja existe uma sincronizacao em andamento."}, HTTPStatus.CONFLICT)
                return

            _send_json(self, {"ok": True, "message": "Sincronizacao iniciada."}, HTTPStatus.ACCEPTED)
            return

        if route == "/api/sync-cancel":
            user = _current_user(self)
            if not user:
                _send_json(self, {"error": "Nao autenticado."}, HTTPStatus.UNAUTHORIZED)
                return

            if CONFIG.sync_mode != "agent":
                _send_json(
                    self,
                    {"ok": False, "message": "O cancelamento pelo dashboard esta disponivel apenas no modo com agente."},
                    HTTPStatus.BAD_REQUEST,
                )
                return

            canceled, message = _store.cancel_active_job(str(user.get("username") or ""))
            if not canceled:
                _send_json(self, {"ok": False, "message": message}, HTTPStatus.CONFLICT)
                return

            _send_json(self, {"ok": True, "message": message}, HTTPStatus.OK)
            return

        if route == "/api/fines/review":
            user = _current_user(self)
            if not user:
                _send_json(self, {"error": "Nao autenticado."}, HTTPStatus.UNAUTHORIZED)
                return
            payload = _read_json(self)
            auto = str(payload.get("auto") or "")
            processo = str(payload.get("processo") or "")
            action = str(payload.get("action") or "")
            note = str(payload.get("note") or "")
            ok, message = _store.set_manual_review(auto, processo, action, note, str(user.get("username") or ""))
            _send_json(
                self,
                {"ok": ok, "message": message},
                HTTPStatus.OK if ok else HTTPStatus.BAD_REQUEST,
            )
            return

        if route == "/api/agent/heartbeat":
            if not _agent_request_authorized(self):
                _send_json(self, {"error": "Agente nao autorizado."}, HTTPStatus.UNAUTHORIZED)
                return
            payload = _read_json(self)
            agent_name = str(payload.get("agent_name") or CONFIG.sync_agent_name)
            status = str(payload.get("status") or "idle")
            message = str(payload.get("message") or "")
            current_job_id = str(payload.get("current_job_id") or "")
            _store.update_agent_status(agent_name, status, message, current_job_id)
            _send_json(self, {"ok": True})
            return

        if route == "/api/agent/jobs/next":
            if not _agent_request_authorized(self):
                _send_json(self, {"error": "Agente nao autorizado."}, HTTPStatus.UNAUTHORIZED)
                return
            payload = _read_json(self)
            agent_name = str(payload.get("agent_name") or CONFIG.sync_agent_name)
            job = _store.claim_next_job(agent_name)
            _send_json(self, {"job": job})
            return

        if route.endswith("/progress") and route.startswith("/api/agent/jobs/"):
            if not _agent_request_authorized(self):
                _send_json(self, {"error": "Agente nao autorizado."}, HTTPStatus.UNAUTHORIZED)
                return
            job_id = route.removeprefix("/api/agent/jobs/").removesuffix("/progress")
            payload = _read_json(self)
            message = str(payload.get("message") or "Agente atualizou o progresso.")
            _store.update_job_progress(job_id, message)
            _send_json(self, {"ok": True})
            return

        if route.endswith("/complete") and route.startswith("/api/agent/jobs/"):
            if not _agent_request_authorized(self):
                _send_json(self, {"error": "Agente nao autorizado."}, HTTPStatus.UNAUTHORIZED)
                return
            job_id = route.removeprefix("/api/agent/jobs/").removesuffix("/complete")
            payload = _read_json(self)
            agent_name = str(payload.get("agent_name") or CONFIG.sync_agent_name)
            fines = [FineRecord.from_dict(item) for item in payload.get("fines", [])]
            message = str(payload.get("message") or f"Leitura concluida pelo agente {agent_name}.")
            pdf_documents = payload.get("pdf_documents", [])
            _store.complete_job(job_id, fines, agent_name, message, pdf_documents=pdf_documents)
            _send_json(self, {"ok": True, "total_fines": len(fines)})
            return

        if route.endswith("/fail") and route.startswith("/api/agent/jobs/"):
            if not _agent_request_authorized(self):
                _send_json(self, {"error": "Agente nao autorizado."}, HTTPStatus.UNAUTHORIZED)
                return
            job_id = route.removeprefix("/api/agent/jobs/").removesuffix("/fail")
            payload = _read_json(self)
            agent_name = str(payload.get("agent_name") or CONFIG.sync_agent_name)
            message = str(payload.get("message") or "Sincronizacao falhou.")
            _store.fail_job(job_id, message, agent_name)
            _send_json(self, {"ok": True})
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
    print(f"Admin inicial configurado: {CONFIG.dashboard_user}")
    print(f"Modo de sincronizacao: {CONFIG.sync_mode}")
    print("Use Ctrl+C para encerrar.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServidor encerrado.")
    finally:
        server.server_close()
