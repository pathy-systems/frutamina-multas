from __future__ import annotations

import csv
import io
import json
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


def _now_label() -> str:
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")


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
                        fonte TEXT NOT NULL DEFAULT 'ANTT',
                        updated_at TEXT NOT NULL DEFAULT ''
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
            conn.commit()

    def load(self) -> list[FineRecord]:
        if self.uses_database:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT tipo_fiscalizacao, auto_infracao, numero_processo, autuado, situacao, data_auto,
                               valor_multa, pdf_nome, fonte
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
                        valor_multa=_parse_decimal(row.get("Valor da Multa", "")),
                        pdf_nome=row.get("PDF", ""),
                    )
                    for row in reader
                ]
        return []

    def save(self, fines: list[FineRecord]) -> None:
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
                                data_auto, valor_multa, pdf_nome, fonte, updated_at
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                                fine.fonte,
                                now,
                            ),
                        )
                conn.commit()
            return

        ensure_directories()
        JSON_PATH.write_text(
            json.dumps([fine.to_dict() for fine in fines], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        CSV_PATH.write_bytes(self.build_csv_bytes(fines))

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
                "Valor da Multa",
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
                    "Valor da Multa": _format_brl(fine.valor_multa),
                    "PDF": fine.pdf_nome,
                }
            )
        return buffer.getvalue().encode("utf-8")

    def build_dashboard_payload(self) -> dict[str, object]:
        fines = self.load()
        total_valor = sum((fine.valor_multa for fine in fines), Decimal("0"))
        tipos: dict[str, int] = {}
        for fine in fines:
            tipos[fine.tipo_fiscalizacao] = tipos.get(fine.tipo_fiscalizacao, 0) + 1

        top_items = sorted(fines, key=lambda item: item.valor_multa, reverse=True)[:5]
        updated_at = self.get_sync_snapshot().get("last_success_at") or self.last_updated_label()

        return {
            "summary": {
                "total_fines": len(fines),
                "total_value": _format_brl(total_valor),
                "active_types": len(tipos),
                "updated_at": updated_at or "Sem sincronizacao ainda",
            },
            "type_counts": [
                {"name": name, "count": count}
                for name, count in sorted(tipos.items(), key=lambda item: (-item[1], item[0]))
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
            "fines": [
                {
                    "tipo": fine.tipo_fiscalizacao,
                    "auto": fine.auto_infracao,
                    "processo": fine.numero_processo,
                    "autuado": fine.autuado,
                    "situacao": fine.situacao,
                    "dataAuto": fine.data_auto,
                    "valor": _format_brl(fine.valor_multa),
                    "pdfNome": fine.pdf_nome,
                    "pdfUrl": f"/downloads/{fine.pdf_nome}"
                    if fine.pdf_nome and (DOWNLOAD_DIR / fine.pdf_nome).exists()
                    else "",
                }
                for fine in fines
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

    def complete_job(self, job_id: str, fines: list[FineRecord], agent_name: str, message: str = "") -> None:
        self.save(fines)
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
