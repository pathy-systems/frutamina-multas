from __future__ import annotations

import base64
import binascii
import csv
import io
import json
import re
import unicodedata
import uuid
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from .config import CONFIG, DATA_DIR, DOWNLOAD_DIR, ensure_directories
from .models import FineRecord, SyncSnapshot


JSON_PATH = DATA_DIR / "multas_ativas.json"
CSV_PATH = DATA_DIR / "multas_ativas.csv"
SNAPSHOT_PATH = DATA_DIR / "sync_snapshot.json"
JOBS_PATH = DATA_DIR / "sync_jobs.json"
HISTORY_PATH = DATA_DIR / "fine_history.json"
AGENT_STATUS_PATH = DATA_DIR / "agent_status.json"
LEGACY_FIRST_SEEN_AT = "01/01/2000 00:00:00"
NEW_FINE_HIGHLIGHT_DAYS = 7


def _now_label() -> str:
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")


def _parse_label_datetime(value: str) -> datetime | None:
    try:
        return datetime.strptime(value, "%d/%m/%Y %H:%M:%S")
    except (TypeError, ValueError):
        return None


def _is_recent_new(first_seen_at: str) -> bool:
    seen_at = _parse_label_datetime(first_seen_at)
    if not seen_at:
        return False
    return (datetime.now() - seen_at).days < NEW_FINE_HIGHLIGHT_DAYS


def _format_brl(value: Decimal) -> str:
    inteiro, decimal = f"{value.quantize(Decimal('0.01')):.2f}".split(".")
    grupos: list[str] = []
    while inteiro:
        grupos.append(inteiro[-3:])
        inteiro = inteiro[:-3]
    return f"R$ {'.'.join(reversed(grupos))},{decimal}"


def _parse_decimal(value: str) -> Decimal:
    normalized = (value or "").replace("R$", "").replace(".", "").replace(",", ".").strip()
    try:
        return Decimal(normalized or "0")
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _default_snapshot() -> dict[str, object]:
    return SyncSnapshot().to_dict()


def _normalize_pdf_name(value: str) -> str:
    return Path(value or "").name


def _normalize_lookup_text(value: str) -> str:
    folded = unicodedata.normalize("NFKD", value or "")
    ascii_text = "".join(char for char in folded if not unicodedata.combining(char))
    return re.sub(r"[^A-Z0-9]", "", ascii_text.upper())


def _record_lookup_keys(fine: FineRecord) -> list[str]:
    keys: list[str] = []
    if fine.numero_processo:
        keys.append(_normalize_lookup_text(fine.numero_processo))
    if fine.auto_infracao:
        keys.append(_normalize_lookup_text(fine.auto_infracao))
    return [key for key in keys if key]


def _label_status_carteira(status: str) -> str:
    return {
        "ativa_com_boleto": "Ativa com boleto",
        "ativa_sem_boleto": "Aguardando boleto",
        "revisar": "Revisar",
        "quitada_confirmada": "Quitada confirmada",
    }.get(status, "Em acompanhamento")


def _default_agent_status() -> dict[str, object]:
    return {
        "agent_name": CONFIG.sync_agent_name,
        "status": "offline",
        "message": "Aguardando heartbeat do agente.",
        "current_job_id": "",
        "last_seen_at": "",
    }


class FineStore:
    def __init__(self) -> None:
        ensure_directories()
        self.database_url = CONFIG.database_url
        self.uses_database = bool(self.database_url)
        if self.uses_database:
            self._ensure_postgres_schema()
        else:
            self._ensure_file_state()

    def _ensure_file_state(self) -> None:
        if not SNAPSHOT_PATH.exists():
            SNAPSHOT_PATH.write_text(json.dumps(_default_snapshot(), ensure_ascii=False, indent=2), encoding="utf-8")
        if not JOBS_PATH.exists():
            JOBS_PATH.write_text("[]", encoding="utf-8")
        if not HISTORY_PATH.exists():
            HISTORY_PATH.write_text("[]", encoding="utf-8")
        if not AGENT_STATUS_PATH.exists():
            AGENT_STATUS_PATH.write_text(
                json.dumps(_default_agent_status(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    def _connect(self):
        try:
            import psycopg
            from psycopg.rows import dict_row
        except Exception as exc:
            raise RuntimeError(
                "DATABASE_URL foi definido, mas a dependencia psycopg nao esta instalada."
            ) from exc

        return psycopg.connect(self.database_url, row_factory=dict_row)

    def _ensure_postgres_schema(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS fines (
                        auto_infracao TEXT PRIMARY KEY,
                        tipo_fiscalizacao TEXT NOT NULL,
                        numero_processo TEXT NOT NULL DEFAULT '',
                        autuado TEXT NOT NULL DEFAULT '',
                        situacao TEXT NOT NULL DEFAULT '',
                        data_auto TEXT NOT NULL DEFAULT '',
                        valor_multa NUMERIC(14, 2) NOT NULL DEFAULT 0,
                        pdf_nome TEXT NOT NULL DEFAULT '',
                        boleto_disponivel BOOLEAN NOT NULL DEFAULT FALSE,
                        valor_disponivel BOOLEAN NOT NULL DEFAULT FALSE,
                        mensagem_valor TEXT NOT NULL DEFAULT 'Boleto e valor ainda nao estao disponiveis',
                        fonte_valor TEXT NOT NULL DEFAULT '',
                        status_carteira TEXT NOT NULL DEFAULT 'ativa_sem_boleto',
                        ja_teve_boleto BOOLEAN NOT NULL DEFAULT FALSE,
                        first_seen_at TEXT NOT NULL DEFAULT '',
                        decision_trail TEXT NOT NULL DEFAULT '[]',
                        manual_override_status TEXT NOT NULL DEFAULT '',
                        manual_override_note TEXT NOT NULL DEFAULT '',
                        fonte TEXT NOT NULL DEFAULT 'ANTT',
                        updated_at TEXT NOT NULL DEFAULT ''
                    )
                    """
                )
                cur.execute("ALTER TABLE fines ADD COLUMN IF NOT EXISTS boleto_disponivel BOOLEAN NOT NULL DEFAULT FALSE")
                cur.execute("ALTER TABLE fines ADD COLUMN IF NOT EXISTS valor_disponivel BOOLEAN NOT NULL DEFAULT FALSE")
                cur.execute(
                    """
                    ALTER TABLE fines
                    ADD COLUMN IF NOT EXISTS mensagem_valor TEXT NOT NULL DEFAULT 'Boleto e valor ainda nao estao disponiveis'
                    """
                )
                cur.execute("ALTER TABLE fines ADD COLUMN IF NOT EXISTS fonte_valor TEXT NOT NULL DEFAULT ''")
                cur.execute("ALTER TABLE fines ADD COLUMN IF NOT EXISTS status_carteira TEXT NOT NULL DEFAULT 'ativa_sem_boleto'")
                cur.execute("ALTER TABLE fines ADD COLUMN IF NOT EXISTS ja_teve_boleto BOOLEAN NOT NULL DEFAULT FALSE")
                cur.execute("ALTER TABLE fines ADD COLUMN IF NOT EXISTS first_seen_at TEXT NOT NULL DEFAULT ''")
                cur.execute("ALTER TABLE fines ADD COLUMN IF NOT EXISTS decision_trail TEXT NOT NULL DEFAULT '[]'")
                cur.execute("ALTER TABLE fines ADD COLUMN IF NOT EXISTS manual_override_status TEXT NOT NULL DEFAULT ''")
                cur.execute("ALTER TABLE fines ADD COLUMN IF NOT EXISTS manual_override_note TEXT NOT NULL DEFAULT ''")
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pdf_documents (
                        name TEXT PRIMARY KEY,
                        content BYTEA NOT NULL,
                        content_type TEXT NOT NULL DEFAULT 'application/pdf',
                        updated_at TEXT NOT NULL DEFAULT ''
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS fine_history (
                        id TEXT PRIMARY KEY,
                        auto_infracao TEXT NOT NULL DEFAULT '',
                        numero_processo TEXT NOT NULL DEFAULT '',
                        tipo_fiscalizacao TEXT NOT NULL DEFAULT '',
                        status_carteira TEXT NOT NULL DEFAULT '',
                        message TEXT NOT NULL DEFAULT '',
                        actor TEXT NOT NULL DEFAULT '',
                        created_at TEXT NOT NULL DEFAULT '',
                        details_json TEXT NOT NULL DEFAULT '{}'
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS agent_heartbeat (
                        agent_name TEXT PRIMARY KEY,
                        status TEXT NOT NULL DEFAULT 'offline',
                        message TEXT NOT NULL DEFAULT '',
                        current_job_id TEXT NOT NULL DEFAULT '',
                        last_seen_at TEXT NOT NULL DEFAULT ''
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS sync_snapshot (
                        singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
                        status TEXT NOT NULL DEFAULT 'idle',
                        message TEXT NOT NULL DEFAULT 'Pronto para sincronizar.',
                        started_at TEXT NOT NULL DEFAULT '',
                        finished_at TEXT NOT NULL DEFAULT '',
                        last_success_at TEXT NOT NULL DEFAULT '',
                        total_fines INTEGER NOT NULL DEFAULT 0,
                        error TEXT NOT NULL DEFAULT ''
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS sync_jobs (
                        id TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        requested_at TEXT NOT NULL,
                        started_at TEXT NOT NULL DEFAULT '',
                        finished_at TEXT NOT NULL DEFAULT '',
                        requested_by TEXT NOT NULL DEFAULT '',
                        runner_name TEXT NOT NULL DEFAULT '',
                        message TEXT NOT NULL DEFAULT '',
                        error TEXT NOT NULL DEFAULT ''
                    )
                    """
                )
                cur.execute(
                    """
                    INSERT INTO sync_snapshot (singleton, status, message, started_at, finished_at, last_success_at, total_fines, error)
                    VALUES (TRUE, 'idle', 'Pronto para sincronizar.', '', '', '', 0, '')
                    ON CONFLICT (singleton) DO NOTHING
                    """
                )
                cur.execute(
                    """
                    INSERT INTO agent_heartbeat (agent_name, status, message, current_job_id, last_seen_at)
                    VALUES (%s, 'offline', 'Aguardando heartbeat do agente.', '', '')
                    ON CONFLICT (agent_name) DO NOTHING
                    """,
                    (CONFIG.sync_agent_name,),
                )
            conn.commit()

    def load(self) -> list[FineRecord]:
        if self.uses_database:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT tipo_fiscalizacao, auto_infracao, numero_processo, autuado, situacao, data_auto,
                               valor_multa, pdf_nome, boleto_disponivel, valor_disponivel, mensagem_valor,
                               fonte_valor, status_carteira, ja_teve_boleto, first_seen_at, decision_trail,
                               manual_override_status, manual_override_note, fonte
                        FROM fines
                        ORDER BY tipo_fiscalizacao, auto_infracao
                        """
                    )
                    rows = cur.fetchall()
            return [
                FineRecord(
                    tipo_fiscalizacao=row["tipo_fiscalizacao"],
                    auto_infracao=row["auto_infracao"],
                    numero_processo=row["numero_processo"],
                    autuado=row["autuado"],
                    situacao=row["situacao"],
                    data_auto=row["data_auto"],
                    valor_multa=Decimal(str(row["valor_multa"])),
                    pdf_nome=row["pdf_nome"],
                    boleto_disponivel=bool(row.get("boleto_disponivel", False) or row["pdf_nome"]),
                    valor_disponivel=bool(
                        row.get("valor_disponivel", False) or Decimal(str(row["valor_multa"])) > Decimal("0")
                    ),
                    mensagem_valor=row.get("mensagem_valor")
                    or (
                        "Valor do boleto encontrado"
                        if Decimal(str(row["valor_multa"])) > Decimal("0")
                        else "Boleto e valor ainda nao estao disponiveis"
                    ),
                    fonte_valor=row.get("fonte_valor", ""),
                    status_carteira=str(row.get("status_carteira") or "ativa_sem_boleto"),
                    ja_teve_boleto=bool(
                        row.get("ja_teve_boleto", False)
                        or row.get("boleto_disponivel", False)
                        or Decimal(str(row["valor_multa"])) > Decimal("0")
                    ),
                    first_seen_at=str(row.get("first_seen_at") or ""),
                    decision_trail=json.loads(str(row.get("decision_trail") or "[]")),
                    manual_override_status=str(row.get("manual_override_status") or ""),
                    manual_override_note=str(row.get("manual_override_note") or ""),
                    fonte=row["fonte"],
                )
                for row in rows
            ]

        if JSON_PATH.exists():
            payload = json.loads(JSON_PATH.read_text(encoding="utf-8"))
            return [FineRecord.from_dict(item) for item in payload]

        if CSV_PATH.exists():
            with CSV_PATH.open("r", encoding="utf-8", newline="") as file:
                reader = csv.DictReader(file, delimiter=";")
                return [
                    FineRecord(
                        tipo_fiscalizacao=row.get("Tipo Fiscalizacao", ""),
                        auto_infracao=row.get("Auto de Infracao", ""),
                        numero_processo=row.get("Numero do Processo", ""),
                        autuado=row.get("Autuado", ""),
                        situacao=row.get("Situacao", ""),
                        data_auto=row.get("Data do Auto", ""),
                        valor_multa=_parse_decimal(row.get("Valor do Boleto", row.get("Valor da Multa", ""))),
                        pdf_nome=row.get("PDF", ""),
                        boleto_disponivel=(row.get("Boleto Disponivel", "").strip().lower() == "sim")
                        if row.get("Boleto Disponivel") is not None
                        else bool(row.get("PDF", "")),
                        valor_disponivel=(row.get("Valor Disponivel", "").strip().lower() == "sim")
                        if row.get("Valor Disponivel") is not None
                        else _parse_decimal(row.get("Valor do Boleto", row.get("Valor da Multa", ""))) > Decimal("0"),
                        mensagem_valor=row.get("Mensagem do Boleto")
                        or (
                            "Valor do boleto encontrado"
                            if _parse_decimal(row.get("Valor do Boleto", row.get("Valor da Multa", ""))) > Decimal("0")
                            else "Boleto e valor ainda nao estao disponiveis"
                        ),
                        fonte_valor=row.get("Fonte do Valor", ""),
                        status_carteira=row.get("Status da Carteira", "ativa_sem_boleto"),
                        ja_teve_boleto=(row.get("Ja Teve Boleto", "").strip().lower() == "sim")
                        if row.get("Ja Teve Boleto") is not None
                        else (
                            (row.get("Boleto Disponivel", "").strip().lower() == "sim")
                            if row.get("Boleto Disponivel") is not None
                            else bool(row.get("PDF", ""))
                        ),
                        first_seen_at=row.get("Primeira Aparicao", ""),
                        decision_trail=json.loads(row.get("Trilha de Decisao", "[]") or "[]"),
                        manual_override_status=row.get("Override Manual", ""),
                        manual_override_note=row.get("Nota Manual", ""),
                    )
                    for row in reader
                ]
        return []

    def _prepare_pdf_documents(self, pdf_documents: list[dict[str, object]] | None) -> list[dict[str, object]]:
        prepared: list[dict[str, object]] = []
        for item in pdf_documents or []:
            name = _normalize_pdf_name(str(item.get("name", "")))
            encoded = str(item.get("content_base64", ""))
            if not name or not encoded:
                continue
            try:
                content = base64.b64decode(encoded, validate=True)
            except (ValueError, binascii.Error):
                continue
            if not content:
                continue
            prepared.append(
                {
                    "name": name,
                    "content": content,
                    "content_type": str(item.get("content_type") or "application/pdf"),
                }
            )
        return prepared

    def _sync_database_pdfs(self, valid_names: set[str], pdf_documents: list[dict[str, object]]) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                if valid_names:
                    cur.execute("DELETE FROM pdf_documents WHERE NOT (name = ANY(%s))", (sorted(valid_names),))
                else:
                    cur.execute("DELETE FROM pdf_documents")

                for document in pdf_documents:
                    cur.execute(
                        """
                        INSERT INTO pdf_documents (name, content, content_type, updated_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (name) DO UPDATE
                        SET content = EXCLUDED.content,
                            content_type = EXCLUDED.content_type,
                            updated_at = EXCLUDED.updated_at
                        """,
                        (
                            document["name"],
                            document["content"],
                            document["content_type"],
                            _now_label(),
                        ),
                    )
            conn.commit()

    def _sync_local_pdfs(self, valid_names: set[str], pdf_documents: list[dict[str, object]] | None) -> None:
        ensure_directories()
        for file_path in DOWNLOAD_DIR.glob("*.pdf"):
            if file_path.name not in valid_names:
                file_path.unlink(missing_ok=True)

        for document in pdf_documents or []:
            file_path = DOWNLOAD_DIR / str(document["name"])
            file_path.write_bytes(bytes(document["content"]))

    def has_pdf(self, filename: str) -> bool:
        normalized = _normalize_pdf_name(filename)
        if not normalized:
            return False

        if (DOWNLOAD_DIR / normalized).exists():
            return True

        if not self.uses_database:
            return False

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pdf_documents WHERE name = %s", (normalized,))
                return cur.fetchone() is not None

    def available_pdf_names(self) -> set[str]:
        names = {file_path.name for file_path in DOWNLOAD_DIR.glob("*.pdf")}
        if not self.uses_database:
            return names

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM pdf_documents")
                rows = cur.fetchall()

        names.update(str(row["name"]) for row in rows)
        return names

    def read_pdf(self, filename: str) -> tuple[bytes, str] | None:
        normalized = _normalize_pdf_name(filename)
        if not normalized:
            return None

        local_path = DOWNLOAD_DIR / normalized
        if local_path.exists():
            return local_path.read_bytes(), "application/pdf"

        if not self.uses_database:
            return None

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT content, content_type FROM pdf_documents WHERE name = %s",
                    (normalized,),
                )
                row = cur.fetchone()

        if not row:
            return None
        return bytes(row["content"]), str(row.get("content_type") or "application/pdf")

    def _load_history(self) -> list[dict[str, object]]:
        if self.uses_database:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, auto_infracao, numero_processo, tipo_fiscalizacao, status_carteira,
                               message, actor, created_at, details_json
                        FROM fine_history
                        ORDER BY created_at DESC
                        """
                    )
                    return cur.fetchall()

        self._ensure_file_state()
        return json.loads(HISTORY_PATH.read_text(encoding="utf-8"))

    def _append_history_entries(self, fines: list[FineRecord], actor: str) -> None:
        timestamp = _now_label()
        entries = [
            {
                "id": str(uuid.uuid4()),
                "auto_infracao": fine.auto_infracao,
                "numero_processo": fine.numero_processo,
                "tipo_fiscalizacao": fine.tipo_fiscalizacao,
                "status_carteira": fine.status_carteira,
                "message": fine.mensagem_valor,
                "actor": actor,
                "created_at": timestamp,
                "details_json": json.dumps(
                    {
                        "decision_trail": fine.decision_trail,
                        "manual_override_status": fine.manual_override_status,
                        "manual_override_note": fine.manual_override_note,
                        "boleto_disponivel": fine.boleto_disponivel,
                        "valor_disponivel": fine.valor_disponivel,
                        "fonte_valor": fine.fonte_valor,
                        "first_seen_at": fine.first_seen_at,
                    },
                    ensure_ascii=False,
                ),
            }
            for fine in fines
        ]

        if self.uses_database:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    for entry in entries:
                        cur.execute(
                            """
                            INSERT INTO fine_history (
                                id, auto_infracao, numero_processo, tipo_fiscalizacao, status_carteira,
                                message, actor, created_at, details_json
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                entry["id"],
                                entry["auto_infracao"],
                                entry["numero_processo"],
                                entry["tipo_fiscalizacao"],
                                entry["status_carteira"],
                                entry["message"],
                                entry["actor"],
                                entry["created_at"],
                                entry["details_json"],
                            ),
                        )
                conn.commit()
            return

        history = entries + self._load_history()
        HISTORY_PATH.write_text(json.dumps(history[:5000], ensure_ascii=False, indent=2), encoding="utf-8")

    def get_fine_history(self, auto_infracao: str, numero_processo: str = "", limit: int = 20) -> list[dict[str, object]]:
        auto_lookup = _normalize_lookup_text(auto_infracao)
        process_lookup = _normalize_lookup_text(numero_processo)
        filtered: list[dict[str, object]] = []

        for entry in self._load_history():
            entry_auto = _normalize_lookup_text(str(entry.get("auto_infracao", "")))
            entry_process = _normalize_lookup_text(str(entry.get("numero_processo", "")))
            if auto_lookup and entry_auto == auto_lookup:
                filtered.append(entry)
            elif process_lookup and entry_process == process_lookup:
                filtered.append(entry)
            if len(filtered) >= limit:
                break

        history: list[dict[str, object]] = []
        for entry in filtered:
            try:
                details = json.loads(str(entry.get("details_json") or "{}"))
            except json.JSONDecodeError:
                details = {}
            history.append(
                {
                    "auto": entry.get("auto_infracao", ""),
                    "processo": entry.get("numero_processo", ""),
                    "tipo": entry.get("tipo_fiscalizacao", ""),
                    "statusCarteira": entry.get("status_carteira", ""),
                    "statusCarteiraLabel": _label_status_carteira(str(entry.get("status_carteira", ""))),
                    "message": entry.get("message", ""),
                    "actor": entry.get("actor", ""),
                    "createdAt": entry.get("created_at", ""),
                    "decisionTrail": details.get("decision_trail", []),
                    "manualOverrideStatus": details.get("manual_override_status", ""),
                    "manualOverrideNote": details.get("manual_override_note", ""),
                    "firstSeenAt": details.get("first_seen_at", ""),
                }
            )
        return history

    def _load_agent_status(self) -> dict[str, object]:
        if self.uses_database:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT agent_name, status, message, current_job_id, last_seen_at
                        FROM agent_heartbeat
                        ORDER BY last_seen_at DESC, agent_name ASC
                        LIMIT 1
                        """
                    )
                    row = cur.fetchone()
            return row or _default_agent_status()

        self._ensure_file_state()
        return json.loads(AGENT_STATUS_PATH.read_text(encoding="utf-8"))

    def get_agent_status(self) -> dict[str, object]:
        status = dict(self._load_agent_status())
        last_seen = str(status.get("last_seen_at") or "")
        online = False
        if last_seen:
            try:
                seen_at = datetime.strptime(last_seen, "%d/%m/%Y %H:%M:%S")
                grace_seconds = max(CONFIG.agent_poll_interval * 3, 45)
                online = (datetime.now() - seen_at).total_seconds() <= grace_seconds
            except ValueError:
                online = False
        status["online"] = online
        status["statusLabel"] = "Online" if online else ("Sem sinal" if not last_seen else "Offline")
        return status

    def update_agent_status(self, agent_name: str, status: str, message: str = "", current_job_id: str = "") -> None:
        payload = {
            "agent_name": agent_name,
            "status": status,
            "message": message or "",
            "current_job_id": current_job_id or "",
            "last_seen_at": _now_label(),
        }

        if self.uses_database:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO agent_heartbeat (agent_name, status, message, current_job_id, last_seen_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (agent_name) DO UPDATE
                        SET status = EXCLUDED.status,
                            message = EXCLUDED.message,
                            current_job_id = EXCLUDED.current_job_id,
                            last_seen_at = EXCLUDED.last_seen_at
                        """,
                        (
                            payload["agent_name"],
                            payload["status"],
                            payload["message"],
                            payload["current_job_id"],
                            payload["last_seen_at"],
                        ),
                    )
                conn.commit()
            return

        AGENT_STATUS_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def set_manual_review(
        self,
        auto_infracao: str,
        numero_processo: str,
        action: str,
        note: str,
        actor: str,
    ) -> tuple[bool, str]:
        fines = self.load()
        auto_lookup = _normalize_lookup_text(auto_infracao)
        process_lookup = _normalize_lookup_text(numero_processo)
        target: FineRecord | None = None

        for fine in fines:
            if auto_lookup and _normalize_lookup_text(fine.auto_infracao) == auto_lookup:
                target = fine
                break
            if process_lookup and _normalize_lookup_text(fine.numero_processo) == process_lookup:
                target = fine
                break

        if not target:
            return False, "Multa nao encontrada para revisao."

        action_map = {
            "manter_ativa": "ativa_sem_boleto",
            "marcar_quitada": "quitada_confirmada",
            "revisar": "revisar",
            "limpar_override": "",
        }
        if action not in action_map:
            return False, "Acao de revisao invalida."

        target.manual_override_status = action_map[action]
        target.manual_override_note = note.strip()
        self.save(fines, actor=actor, preserve_current_overrides=True)
        return True, "Revisao manual atualizada."

    def _apply_history_rules(
        self,
        fines: list[FineRecord],
        previous_records: list[FineRecord],
        preserve_current_overrides: bool = False,
    ) -> list[FineRecord]:
        previous_by_key: dict[str, FineRecord] = {}
        for previous in previous_records:
            for key in _record_lookup_keys(previous):
                previous_by_key.setdefault(key, previous)

        for fine in fines:
            previous = next(
                (previous_by_key[key] for key in _record_lookup_keys(fine) if key in previous_by_key),
                None,
            )
            fine_is_new = previous is None
            current_has_boleto = fine.boleto_disponivel or fine.valor_disponivel
            previous_had_boleto = previous.ja_teve_boleto if previous else False
            fine.ja_teve_boleto = current_has_boleto or previous_had_boleto
            if previous and not preserve_current_overrides:
                fine.manual_override_status = previous.manual_override_status
                fine.manual_override_note = previous.manual_override_note
            if previous:
                fine.first_seen_at = previous.first_seen_at or LEGACY_FIRST_SEEN_AT
            elif not fine.first_seen_at:
                fine.first_seen_at = _now_label()
            trail: list[str] = [
                "Lida em Vistas ao Processo como multa nao arquivada/cancelada.",
            ]
            if fine_is_new:
                trail.append("Nova multa identificada nesta leitura.")

            if current_has_boleto:
                fine.status_carteira = "ativa_com_boleto"
                trail.append("Boleto confirmado por PDF ou Boleto/Listar.aspx.")
                if not fine.mensagem_valor or "ainda nao" in fine.mensagem_valor.lower():
                    fine.mensagem_valor = "Boleto localizado na ANTT"
            else:
                fine.status_carteira = "ativa_sem_boleto"
                trail.append("Nao houve confirmacao positiva de boleto nesta leitura.")
                if previous_had_boleto:
                    fine.status_carteira = "revisar"
                    fine.mensagem_valor = "Historico com boleto anterior; revisar divergencia"
                    trail.append("Historico indica que a multa ja teve boleto em leitura anterior.")
                elif not fine.mensagem_valor or "ainda nao" in fine.mensagem_valor.lower():
                    fine.mensagem_valor = "Multa ativa sem boleto localizado na ANTT"

            if fine.manual_override_status == "ativa_sem_boleto":
                fine.status_carteira = "ativa_sem_boleto"
                trail.append("Override manual aplicado: manter ativa.")
            elif fine.manual_override_status == "revisar":
                fine.status_carteira = "revisar"
                trail.append("Override manual aplicado: revisar.")
            elif fine.manual_override_status == "quitada_confirmada":
                fine.status_carteira = "quitada_confirmada"
                trail.append("Override manual aplicado: quitada confirmada.")

            if fine.manual_override_note:
                trail.append(f"Nota manual: {fine.manual_override_note}")

            if fine.status_carteira == "revisar" and previous_had_boleto and not current_has_boleto:
                fine.mensagem_valor = "Historico com boleto anterior; revisar divergencia"
                trail.append("A multa ficou marcada para revisao por divergencia de historico.")
            elif fine.status_carteira == "quitada_confirmada":
                fine.mensagem_valor = "Quitada confirmada manualmente"

            fine.decision_trail = trail

        return fines

    def save(
        self,
        fines: list[FineRecord],
        pdf_documents: list[dict[str, object]] | None = None,
        actor: str = "sistema",
        preserve_current_overrides: bool = False,
    ) -> None:
        previous_records = self.load()
        fines = self._apply_history_rules(
            fines,
            previous_records,
            preserve_current_overrides=preserve_current_overrides,
        )
        valid_pdf_names = {_normalize_pdf_name(fine.pdf_nome) for fine in fines if fine.pdf_nome}
        prepared_pdf_documents = self._prepare_pdf_documents(pdf_documents)

        if self.uses_database:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM fines")
                    now = _now_label()
                    for fine in fines:
                        cur.execute(
                            """
                            INSERT INTO fines (
                                auto_infracao, tipo_fiscalizacao, numero_processo, autuado, situacao,
                                data_auto, valor_multa, pdf_nome, boleto_disponivel, valor_disponivel,
                                mensagem_valor, fonte_valor, status_carteira, ja_teve_boleto,
                                first_seen_at, decision_trail, manual_override_status, manual_override_note, fonte, updated_at
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                fine.auto_infracao,
                                fine.tipo_fiscalizacao,
                                fine.numero_processo,
                                fine.autuado,
                                fine.situacao,
                                fine.data_auto,
                                str(fine.valor_multa),
                                fine.pdf_nome,
                                fine.boleto_disponivel,
                                fine.valor_disponivel,
                                fine.mensagem_valor,
                                fine.fonte_valor,
                                fine.status_carteira,
                                fine.ja_teve_boleto,
                                fine.first_seen_at,
                                json.dumps(fine.decision_trail, ensure_ascii=False),
                                fine.manual_override_status,
                                fine.manual_override_note,
                                fine.fonte,
                                now,
                            ),
                        )
                conn.commit()
            if pdf_documents is not None:
                self._sync_database_pdfs(valid_pdf_names, prepared_pdf_documents)
            self._append_history_entries(fines, actor)
            return

        ensure_directories()
        JSON_PATH.write_text(
            json.dumps([fine.to_dict() for fine in fines], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        CSV_PATH.write_bytes(self.build_csv_bytes(fines))
        self._sync_local_pdfs(valid_pdf_names, prepared_pdf_documents)
        self._append_history_entries(fines, actor)

    def build_csv_bytes(self, fines: list[FineRecord] | None = None) -> bytes:
        fines = fines if fines is not None else self.load()
        buffer = io.StringIO()
        writer = csv.DictWriter(
            buffer,
            fieldnames=[
                "Tipo Fiscalizacao",
                "Auto de Infracao",
                "Numero do Processo",
                "Autuado",
                "Situacao",
                "Data do Auto",
                "Valor do Boleto",
                "Boleto Disponivel",
                "Valor Disponivel",
                "Mensagem do Boleto",
                "Fonte do Valor",
                "Status da Carteira",
                "Ja Teve Boleto",
                "Primeira Aparicao",
                "Trilha de Decisao",
                "Override Manual",
                "Nota Manual",
                "PDF",
            ],
            delimiter=";",
        )
        writer.writeheader()
        for fine in fines:
            writer.writerow(
                {
                    "Tipo Fiscalizacao": fine.tipo_fiscalizacao,
                    "Auto de Infracao": fine.auto_infracao,
                    "Numero do Processo": fine.numero_processo,
                    "Autuado": fine.autuado,
                    "Situacao": fine.situacao,
                    "Data do Auto": fine.data_auto,
                    "Valor do Boleto": _format_brl(fine.valor_multa) if fine.valor_disponivel else "",
                    "Boleto Disponivel": "Sim" if fine.boleto_disponivel else "Nao",
                    "Valor Disponivel": "Sim" if fine.valor_disponivel else "Nao",
                    "Mensagem do Boleto": fine.mensagem_valor,
                    "Fonte do Valor": fine.fonte_valor,
                    "Status da Carteira": fine.status_carteira,
                    "Ja Teve Boleto": "Sim" if fine.ja_teve_boleto else "Nao",
                    "Primeira Aparicao": fine.first_seen_at,
                    "Trilha de Decisao": json.dumps(fine.decision_trail, ensure_ascii=False),
                    "Override Manual": fine.manual_override_status,
                    "Nota Manual": fine.manual_override_note,
                    "PDF": fine.pdf_nome,
                }
            )
        return buffer.getvalue().encode("utf-8")

    def build_dashboard_payload(self) -> dict[str, object]:
        fines = self.load()
        visible_fines = [fine for fine in fines if fine.status_carteira != "quitada_confirmada"]
        review_items = [fine for fine in fines if fine.status_carteira in {"revisar", "quitada_confirmada"}]
        new_fines = [fine for fine in visible_fines if _is_recent_new(fine.first_seen_at)]
        pdf_names = self.available_pdf_names()
        fines_com_valor = [fine for fine in visible_fines if fine.valor_disponivel]
        total_valor = sum((fine.valor_multa for fine in fines_com_valor), Decimal("0"))
        tipos: dict[str, int] = {}
        portfolio_status: dict[str, int] = {}
        for fine in visible_fines:
            tipos[fine.tipo_fiscalizacao] = tipos.get(fine.tipo_fiscalizacao, 0) + 1
            portfolio_status[fine.status_carteira] = portfolio_status.get(fine.status_carteira, 0) + 1

        top_items = sorted(fines_com_valor, key=lambda item: item.valor_multa, reverse=True)[:5]
        updated_at = self.get_sync_snapshot().get("last_success_at") or self.last_updated_label()

        return {
            "summary": {
                "total_fines": len(visible_fines),
                "total_value": _format_brl(total_valor),
                "available_boleto_count": len(fines_com_valor),
                "pending_boleto_count": len(visible_fines) - len(fines_com_valor),
                "review_count": portfolio_status.get("revisar", 0),
                "manual_quitada_count": len([fine for fine in fines if fine.status_carteira == "quitada_confirmada"]),
                "new_count": len(new_fines),
                "active_types": len(tipos),
                "updated_at": updated_at or "Sem sincronizacao ainda",
            },
            "new_fines": [
                {
                    "auto": fine.auto_infracao,
                    "tipo": fine.tipo_fiscalizacao,
                    "processo": fine.numero_processo,
                    "firstSeenAt": fine.first_seen_at,
                }
                for fine in sorted(
                    new_fines,
                    key=lambda item: _parse_label_datetime(item.first_seen_at) or datetime.min,
                    reverse=True,
                )[:10]
            ],
            "type_counts": [
                {"name": name, "count": count}
                for name, count in sorted(tipos.items(), key=lambda item: (-item[1], item[0]))
            ],
            "portfolio_status_counts": [
                {"status": status, "label": _label_status_carteira(status), "count": count}
                for status, count in sorted(portfolio_status.items(), key=lambda item: (-item[1], item[0]))
            ],
            "top_fines": [
                {
                    "auto": fine.auto_infracao,
                    "tipo": fine.tipo_fiscalizacao,
                    "valor": _format_brl(fine.valor_multa),
                    "situacao": fine.situacao,
                }
                for fine in top_items
            ],
            "agent_status": self.get_agent_status(),
            "review_items": [
                {
                    "tipo": fine.tipo_fiscalizacao,
                    "auto": fine.auto_infracao,
                    "processo": fine.numero_processo,
                    "situacao": fine.situacao,
                    "mensagemValor": fine.mensagem_valor,
                    "statusCarteira": fine.status_carteira,
                    "statusCarteiraLabel": _label_status_carteira(fine.status_carteira),
                    "decisionTrail": fine.decision_trail,
                    "manualOverrideStatus": fine.manual_override_status,
                    "manualOverrideNote": fine.manual_override_note,
                    "firstSeenAt": fine.first_seen_at,
                    "isNew": _is_recent_new(fine.first_seen_at),
                }
                for fine in review_items
            ],
            "fines": [
                {
                    "tipo": fine.tipo_fiscalizacao,
                    "auto": fine.auto_infracao,
                    "processo": fine.numero_processo,
                    "autuado": fine.autuado,
                    "situacao": fine.situacao,
                    "dataAuto": fine.data_auto,
                    "valor": _format_brl(fine.valor_multa) if fine.valor_disponivel else "",
                    "valorDisponivel": fine.valor_disponivel,
                    "boletoDisponivel": fine.boleto_disponivel,
                    "mensagemValor": fine.mensagem_valor,
                    "fonteValor": fine.fonte_valor,
                    "statusCarteira": fine.status_carteira,
                    "statusCarteiraLabel": _label_status_carteira(fine.status_carteira),
                    "jaTeveBoleto": fine.ja_teve_boleto,
                    "firstSeenAt": fine.first_seen_at,
                    "isNew": _is_recent_new(fine.first_seen_at),
                    "decisionTrail": fine.decision_trail,
                    "manualOverrideStatus": fine.manual_override_status,
                    "manualOverrideNote": fine.manual_override_note,
                    "pdfNome": fine.pdf_nome,
                    "pdfUrl": f"/downloads/{fine.pdf_nome}"
                    if fine.pdf_nome and _normalize_pdf_name(fine.pdf_nome) in pdf_names
                    else "",
                }
                for fine in visible_fines
            ],
        }

    def csv_path(self) -> Path:
        return CSV_PATH

    def downloads_dir(self) -> Path:
        return DOWNLOAD_DIR

    def last_updated_label(self) -> str:
        if self.uses_database:
            snapshot = self.get_sync_snapshot()
            if snapshot.get("last_success_at"):
                return str(snapshot["last_success_at"])
            return "Sem sincronizacao ainda"

        source = JSON_PATH if JSON_PATH.exists() else CSV_PATH
        if not source.exists():
            return "Sem sincronizacao ainda"
        return datetime.fromtimestamp(source.stat().st_mtime).strftime("%d/%m/%Y %H:%M")

    def get_sync_snapshot(self) -> dict[str, object]:
        if self.uses_database:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT status, message, started_at, finished_at, last_success_at, total_fines, error
                        FROM sync_snapshot
                        WHERE singleton = TRUE
                        """
                    )
                    row = cur.fetchone()
            return row or _default_snapshot()

        self._ensure_file_state()
        return json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))

    def _save_sync_snapshot(self, snapshot: dict[str, object]) -> None:
        if self.uses_database:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE sync_snapshot
                        SET status = %s,
                            message = %s,
                            started_at = %s,
                            finished_at = %s,
                            last_success_at = %s,
                            total_fines = %s,
                            error = %s
                        WHERE singleton = TRUE
                        """,
                        (
                            snapshot.get("status", "idle"),
                            snapshot.get("message", "Pronto para sincronizar."),
                            snapshot.get("started_at", ""),
                            snapshot.get("finished_at", ""),
                            snapshot.get("last_success_at", ""),
                            int(snapshot.get("total_fines", 0) or 0),
                            snapshot.get("error", ""),
                        ),
                    )
                conn.commit()
            return

        SNAPSHOT_PATH.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")

    def save_sync_snapshot(self, snapshot: dict[str, object]) -> None:
        self._save_sync_snapshot(snapshot)

    def _load_jobs(self) -> list[dict[str, object]]:
        if self.uses_database:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, status, requested_at, started_at, finished_at, requested_by, runner_name, message, error
                        FROM sync_jobs
                        ORDER BY requested_at DESC
                        """
                    )
                    return cur.fetchall()

        self._ensure_file_state()
        return json.loads(JOBS_PATH.read_text(encoding="utf-8"))

    def _save_jobs(self, jobs: list[dict[str, object]]) -> None:
        if self.uses_database:
            raise RuntimeError("Nao use _save_jobs diretamente com PostgreSQL.")
        JOBS_PATH.write_text(json.dumps(jobs, ensure_ascii=False, indent=2), encoding="utf-8")

    def get_job(self, job_id: str) -> dict[str, object] | None:
        if self.uses_database:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, status, requested_at, started_at, finished_at, requested_by, runner_name, message, error
                        FROM sync_jobs
                        WHERE id = %s
                        """,
                        (job_id,),
                    )
                    return cur.fetchone()

        for job in self._load_jobs():
            if job["id"] == job_id:
                return job
        return None

    def has_pending_job(self) -> bool:
        jobs = self._load_jobs()
        return any(job["status"] in {"pending", "running"} for job in jobs)

    def request_sync(self, requested_by: str) -> tuple[bool, str]:
        if self.has_pending_job():
            return False, "Ja existe uma sincronizacao pendente ou em andamento."

        job_id = str(uuid.uuid4())
        job = {
            "id": job_id,
            "status": "pending",
            "requested_at": _now_label(),
            "started_at": "",
            "finished_at": "",
            "requested_by": requested_by,
            "runner_name": "",
            "message": "Solicitacao registrada. Aguardando agente local.",
            "error": "",
        }

        if self.uses_database:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO sync_jobs (id, status, requested_at, started_at, finished_at, requested_by, runner_name, message, error)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            job["id"],
                            job["status"],
                            job["requested_at"],
                            job["started_at"],
                            job["finished_at"],
                            job["requested_by"],
                            job["runner_name"],
                            job["message"],
                            job["error"],
                        ),
                    )
                conn.commit()
        else:
            jobs = self._load_jobs()
            jobs.insert(0, job)
            self._save_jobs(jobs)

        snapshot = self.get_sync_snapshot()
        snapshot.update(
            {
                "status": "queued",
                "message": "Solicitacao enviada pelo dashboard. Aguardando agente local.",
                "started_at": "",
                "finished_at": "",
                "error": "",
            }
        )
        self._save_sync_snapshot(snapshot)
        return True, job_id

    def cancel_active_job(self, requested_by: str) -> tuple[bool, str]:
        finished_at = _now_label()
        message = f"Sincronizacao cancelada por {requested_by}."

        if self.uses_database:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id
                        FROM sync_jobs
                        WHERE status IN ('pending', 'running')
                        ORDER BY requested_at DESC
                        LIMIT 1
                        """
                    )
                    job = cur.fetchone()
                    if not job:
                        return False, "Nao existe sincronizacao pendente ou em andamento."

                    cur.execute(
                        """
                        UPDATE sync_jobs
                        SET status = %s, finished_at = %s, message = %s, error = ''
                        WHERE id = %s
                        """,
                        ("canceled", finished_at, message, job["id"]),
                    )
                conn.commit()
        else:
            jobs = self._load_jobs()
            job = next((item for item in jobs if item["status"] in {"pending", "running"}), None)
            if not job:
                return False, "Nao existe sincronizacao pendente ou em andamento."
            job["status"] = "canceled"
            job["finished_at"] = finished_at
            job["message"] = message
            job["error"] = ""
            self._save_jobs(jobs)

        snapshot = self.get_sync_snapshot()
        snapshot.update(
            {
                "status": "canceled",
                "message": message,
                "finished_at": finished_at,
                "error": "",
            }
        )
        self._save_sync_snapshot(snapshot)
        return True, message

    def claim_next_job(self, agent_name: str) -> dict[str, object] | None:
        if self.uses_database:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, status, requested_at, started_at, finished_at, requested_by, runner_name, message, error
                        FROM sync_jobs
                        WHERE status = 'pending'
                        ORDER BY requested_at ASC
                        LIMIT 1
                        """
                    )
                    job = cur.fetchone()
                    if not job:
                        return None
                    cur.execute(
                        """
                        UPDATE sync_jobs
                        SET status = %s, started_at = %s, runner_name = %s, message = %s, error = ''
                        WHERE id = %s
                        """,
                        ("running", _now_label(), agent_name, f"Agente {agent_name} iniciou a leitura.", job["id"]),
                    )
                conn.commit()
            snapshot = self.get_sync_snapshot()
            snapshot.update(
                {
                    "status": "running",
                    "message": f"Agente {agent_name} iniciou a leitura da ANTT.",
                    "started_at": _now_label(),
                    "finished_at": "",
                    "error": "",
                }
            )
            self._save_sync_snapshot(snapshot)
            return {**job, "status": "running", "runner_name": agent_name}

        jobs = self._load_jobs()
        for job in jobs:
            if job["status"] == "pending":
                job["status"] = "running"
                job["runner_name"] = agent_name
                job["started_at"] = _now_label()
                job["message"] = f"Agente {agent_name} iniciou a leitura."
                self._save_jobs(jobs)
                snapshot = self.get_sync_snapshot()
                snapshot.update(
                    {
                        "status": "running",
                        "message": f"Agente {agent_name} iniciou a leitura da ANTT.",
                        "started_at": job["started_at"],
                        "finished_at": "",
                        "error": "",
                    }
                )
                self._save_sync_snapshot(snapshot)
                return job
        return None

    def update_job_progress(self, job_id: str, message: str) -> None:
        current_job = self.get_job(job_id)
        if not current_job or current_job["status"] == "canceled":
            return

        if self.uses_database:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE sync_jobs SET message = %s WHERE id = %s", (message, job_id))
                conn.commit()
        else:
            jobs = self._load_jobs()
            for job in jobs:
                if job["id"] == job_id:
                    job["message"] = message
                    break
            self._save_jobs(jobs)

        snapshot = self.get_sync_snapshot()
        snapshot["status"] = "running"
        snapshot["message"] = message
        self._save_sync_snapshot(snapshot)

    def complete_job(
        self,
        job_id: str,
        fines: list[FineRecord],
        agent_name: str,
        message: str = "",
        pdf_documents: list[dict[str, object]] | None = None,
    ) -> None:
        current_job = self.get_job(job_id)
        if not current_job or current_job["status"] == "canceled":
            return

        self.save(fines, pdf_documents=pdf_documents, actor=agent_name)
        finished_at = _now_label()
        final_message = message or f"Leitura concluida pelo agente {agent_name}."

        if self.uses_database:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE sync_jobs
                        SET status = %s, finished_at = %s, runner_name = %s, message = %s, error = ''
                        WHERE id = %s
                        """,
                        ("success", finished_at, agent_name, final_message, job_id),
                    )
                conn.commit()
        else:
            jobs = self._load_jobs()
            for job in jobs:
                if job["id"] == job_id:
                    job["status"] = "success"
                    job["finished_at"] = finished_at
                    job["runner_name"] = agent_name
                    job["message"] = final_message
                    job["error"] = ""
                    break
            self._save_jobs(jobs)

        snapshot = self.get_sync_snapshot()
        snapshot.update(
            {
                "status": "success",
                "message": final_message,
                "finished_at": finished_at,
                "last_success_at": finished_at,
                "total_fines": len(fines),
                "error": "",
            }
        )
        self._save_sync_snapshot(snapshot)

    def fail_job(self, job_id: str, error_message: str, agent_name: str) -> None:
        current_job = self.get_job(job_id)
        if not current_job or current_job["status"] == "canceled":
            return

        finished_at = _now_label()
        if self.uses_database:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE sync_jobs
                        SET status = %s, finished_at = %s, runner_name = %s, message = %s, error = %s
                        WHERE id = %s
                        """,
                        ("error", finished_at, agent_name, error_message, error_message, job_id),
                    )
                conn.commit()
        else:
            jobs = self._load_jobs()
            for job in jobs:
                if job["id"] == job_id:
                    job["status"] = "error"
                    job["finished_at"] = finished_at
                    job["runner_name"] = agent_name
                    job["message"] = error_message
                    job["error"] = error_message
                    break
            self._save_jobs(jobs)

        snapshot = self.get_sync_snapshot()
        snapshot.update(
            {
                "status": "error",
                "message": error_message,
                "finished_at": finished_at,
                "error": error_message,
            }
        )
        self._save_sync_snapshot(snapshot)

    def list_recent_jobs(self, limit: int = 10) -> list[dict[str, object]]:
        jobs = self._load_jobs()
        return jobs[:limit]
