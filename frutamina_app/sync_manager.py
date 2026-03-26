from __future__ import annotations

import threading
from datetime import datetime
from .scraper import run_sync
from .store import FineStore


class SyncManager:
    def __init__(self, store: FineStore) -> None:
        self._store = store
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def snapshot(self) -> dict[str, object]:
        return self._store.get_sync_snapshot()

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> bool:
        with self._lock:
            if self.is_running():
                return False
            snapshot = self._store.get_sync_snapshot()
            snapshot.update(
                {
                    "status": "running",
                    "message": "Inicializando sincronizacao embutida.",
                    "started_at": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                    "finished_at": "",
                    "error": "",
                }
            )
            self._store.save_sync_snapshot(snapshot)
            self._thread = threading.Thread(target=self._worker, daemon=True)
            self._thread.start()
            return True

    def _worker(self) -> None:
        def update(message: str) -> None:
            with self._lock:
                snapshot = self._store.get_sync_snapshot()
                snapshot["status"] = "running"
                snapshot["message"] = message
                self._store.save_sync_snapshot(snapshot)

        try:
            fines = run_sync(update)
            self._store.save(fines)
            with self._lock:
                timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                snapshot = self._store.get_sync_snapshot()
                snapshot.update(
                    {
                        "status": "success",
                        "message": "Sincronizacao concluida com sucesso.",
                        "finished_at": timestamp,
                        "last_success_at": timestamp,
                        "total_fines": len(fines),
                        "error": "",
                    }
                )
                self._store.save_sync_snapshot(snapshot)
        except Exception as exc:
            with self._lock:
                timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                snapshot = self._store.get_sync_snapshot()
                snapshot.update(
                    {
                        "status": "error",
                        "message": str(exc),
                        "finished_at": timestamp,
                        "error": str(exc),
                    }
                )
                self._store.save_sync_snapshot(snapshot)
