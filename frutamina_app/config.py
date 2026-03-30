from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DOWNLOAD_DIR = BASE_DIR / "downloads" / "pdfs"
TEMPLATE_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"


def load_dotenv() -> None:
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_dotenv()


def _database_url() -> str:
    return (
        os.getenv("DATABASE_URL", "")
        or os.getenv("DATABASE_PUBLIC_URL", "")
        or os.getenv("POSTGRES_URL", "")
    )


def _app_timezone() -> ZoneInfo:
    return ZoneInfo(os.getenv("APP_TIMEZONE", "America/Sao_Paulo"))


APP_TIMEZONE = _app_timezone()


def now_local() -> datetime:
    return datetime.now(APP_TIMEZONE).replace(tzinfo=None)


def now_label() -> str:
    return now_local().strftime("%d/%m/%Y %H:%M:%S")


@dataclass(frozen=True)
class AppConfig:
    app_host: str = os.getenv("APP_HOST", os.getenv("HOST", "0.0.0.0"))
    app_port: int = int(os.getenv("PORT", os.getenv("APP_PORT", "8080")))
    dashboard_user: str = os.getenv("DASHBOARD_USER", "admin")
    dashboard_password: str = os.getenv("DASHBOARD_PASSWORD", "admin123")
    database_url: str = _database_url()
    antt_user: str = os.getenv("ANTT_CPF_CNPJ", "")
    antt_password: str = os.getenv("ANTT_SENHA", "")
    mock_sync: bool = os.getenv("MOCK_SYNC", "0") == "1"
    playwright_headless: bool = os.getenv("PLAYWRIGHT_HEADLESS", "1") == "1"
    sync_mode: str = os.getenv("SYNC_MODE", "agent" if _database_url() else "embedded")
    sync_agent_token: str = os.getenv("SYNC_AGENT_TOKEN", "")
    sync_agent_name: str = os.getenv("SYNC_AGENT_NAME", "frutamina-agent")
    agent_server_url: str = os.getenv("AGENT_SERVER_URL", "").rstrip("/")
    agent_poll_interval: int = int(os.getenv("AGENT_POLL_INTERVAL", "15"))
    antt_representado_match: str = os.getenv("ANTT_REPRESENTADO_MATCH", "FRUTAMINA - COMERCIAL AGRICOLA LTDA.")
    app_timezone: str = os.getenv("APP_TIMEZONE", "America/Sao_Paulo")


CONFIG = AppConfig()


def ensure_directories() -> None:
    for path in (DATA_DIR, DOWNLOAD_DIR, TEMPLATE_DIR, STATIC_DIR):
        path.mkdir(parents=True, exist_ok=True)
