from __future__ import annotations

import threading
from typing import Callable

from .models import SyncSnapshot
from .scraper import run_sync
from .store import FineStore


class SyncManager:
    def __init__(self, store: FineStore) -> None:
        self._store = store
        self._snapshot = SyncSnapshot()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return self._snapshot.to_dict()

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> bool:
        with self._lock:
            if self.is_running():
                return False
            self._snapshot.mark_running("Inicializando sincronizacao.")
            self._thread = threading.Thread(target=self._worker, daemon=True)
            self._thread.start()
            return True

    def _worker(self) -> None:
        def update(message: str) -> None:
            with self._lock:
                self._snapshot.message = message

        try:
            fines = run_sync(update)
            self._store.save(fines)
            with self._lock:
                self._snapshot.mark_success(len(fines))
        except Exception as exc:
            with self._lock:
                self._snapshot.mark_error(str(exc))
