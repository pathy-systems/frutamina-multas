from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from typing import Any

from frutamina_app.config import CONFIG
from frutamina_app.scraper import run_sync


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {CONFIG.sync_agent_token}",
        "Content-Type": "application/json; charset=utf-8",
    }


def _post_json(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers=_headers(),
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        raw = response.read().decode("utf-8")
        return json.loads(raw or "{}")


def _claim_next_job() -> dict[str, Any] | None:
    response = _post_json(
        f"{CONFIG.agent_server_url}/api/agent/jobs/next",
        {"agent_name": CONFIG.sync_agent_name},
    )
    return response.get("job")


def _send_progress(job_id: str, message: str) -> None:
    _post_json(
        f"{CONFIG.agent_server_url}/api/agent/jobs/{job_id}/progress",
        {"agent_name": CONFIG.sync_agent_name, "message": message},
    )


def _send_complete(job_id: str, fines_payload: list[dict[str, Any]], message: str) -> None:
    _post_json(
        f"{CONFIG.agent_server_url}/api/agent/jobs/{job_id}/complete",
        {
            "agent_name": CONFIG.sync_agent_name,
            "message": message,
            "fines": fines_payload,
        },
    )


def _send_fail(job_id: str, message: str) -> None:
    _post_json(
        f"{CONFIG.agent_server_url}/api/agent/jobs/{job_id}/fail",
        {"agent_name": CONFIG.sync_agent_name, "message": message},
    )


def _validate_agent_config() -> None:
    if not CONFIG.agent_server_url:
        raise RuntimeError("Defina AGENT_SERVER_URL apontando para a URL publica do Railway.")
    if not CONFIG.sync_agent_token:
        raise RuntimeError("Defina SYNC_AGENT_TOKEN para autenticar o agente local.")


def process_single_job() -> bool:
    job = _claim_next_job()
    if not job:
        print("Nenhum job pendente.")
        return False

    job_id = str(job["id"])
    print(f"Job recebido: {job_id}")
    last_progress_at = 0.0

    def progress(message: str) -> None:
        nonlocal last_progress_at
        now = time.time()
        print(message)
        if now - last_progress_at >= 1.0:
            last_progress_at = now
            try:
                _send_progress(job_id, message)
            except Exception as exc:
                print(f"Aviso ao enviar progresso: {exc}")

    try:
        fines = run_sync(progress)
        _send_complete(
            job_id,
            [fine.to_dict() for fine in fines],
            f"Leitura concluida pelo agente {CONFIG.sync_agent_name}.",
        )
        print(f"Job finalizado com sucesso. Total de multas: {len(fines)}")
        return True
    except Exception as exc:
        message = f"Falha ao processar a leitura: {exc}"
        print(message)
        try:
            _send_fail(job_id, message)
        except Exception as submit_exc:
            print(f"Nao foi possivel notificar falha ao servidor: {submit_exc}")
        return True


def run_loop(poll_interval: int) -> None:
    print(f"Agente ativo. Consultando jobs em {CONFIG.agent_server_url} a cada {poll_interval}s.")
    while True:
        try:
            handled = process_single_job()
            if not handled:
                time.sleep(poll_interval)
        except urllib.error.HTTPError as exc:
            print(f"Erro HTTP no agente: {exc}")
            time.sleep(poll_interval)
        except Exception as exc:
            print(f"Erro no agente: {exc}")
            time.sleep(poll_interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="Agente local de sincronizacao das multas ANTT.")
    parser.add_argument("--once", action="store_true", help="Executa apenas um ciclo de busca por job.")
    args = parser.parse_args()

    _validate_agent_config()
    if args.once:
        process_single_job()
        return
    run_loop(CONFIG.agent_poll_interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAgente encerrado.")
        sys.exit(0)
