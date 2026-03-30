from __future__ import annotations

from dataclasses import asdict, dataclass, field
from decimal import Decimal

from .config import now_label


@dataclass
class FineRecord:
    tipo_fiscalizacao: str
    auto_infracao: str
    numero_processo: str
    autuado: str
    situacao: str
    data_auto: str
    valor_multa: Decimal
    pdf_nome: str = ""
    fonte: str = "ANTT"
    boleto_disponivel: bool = False
    valor_disponivel: bool = False
    mensagem_valor: str = "Boleto e valor ainda nao estao disponiveis"
    fonte_valor: str = ""
    status_carteira: str = "ativa_sem_boleto"
    ja_teve_boleto: bool = False
    first_seen_at: str = ""
    decision_trail: list[str] = field(default_factory=list)
    manual_override_status: str = ""
    manual_override_note: str = ""

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["valor_multa"] = f"{self.valor_multa:.2f}"
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "FineRecord":
        valor_multa = Decimal(str(payload.get("valor_multa", "0")))
        pdf_nome = str(payload.get("pdf_nome", ""))
        raw_boleto_disponivel = payload.get("boleto_disponivel")
        raw_valor_disponivel = payload.get("valor_disponivel")
        valor_disponivel = (
            bool(raw_valor_disponivel)
            if raw_valor_disponivel is not None
            else valor_multa > Decimal("0")
        )
        boleto_disponivel = (
            bool(raw_boleto_disponivel)
            if raw_boleto_disponivel is not None
            else bool(pdf_nome) or valor_disponivel
        )
        mensagem_padrao = (
            "Valor do boleto encontrado"
            if valor_disponivel
            else "Boleto e valor ainda nao estao disponiveis"
        )
        ja_teve_boleto = (
            bool(payload.get("ja_teve_boleto"))
            if payload.get("ja_teve_boleto") is not None
            else boleto_disponivel or valor_disponivel
        )
        status_carteira = str(payload.get("status_carteira") or "").strip() or (
            "ativa_com_boleto" if boleto_disponivel or valor_disponivel else "ativa_sem_boleto"
        )
        return cls(
            tipo_fiscalizacao=str(payload.get("tipo_fiscalizacao", "")),
            auto_infracao=str(payload.get("auto_infracao", "")),
            numero_processo=str(payload.get("numero_processo", "")),
            autuado=str(payload.get("autuado", "")),
            situacao=str(payload.get("situacao", "")),
            data_auto=str(payload.get("data_auto", "")),
            valor_multa=valor_multa,
            pdf_nome=pdf_nome,
            fonte=str(payload.get("fonte", "ANTT")),
            boleto_disponivel=boleto_disponivel,
            valor_disponivel=valor_disponivel,
            mensagem_valor=str(payload.get("mensagem_valor") or mensagem_padrao),
            fonte_valor=str(payload.get("fonte_valor", "")),
            status_carteira=status_carteira,
            ja_teve_boleto=ja_teve_boleto,
            first_seen_at=str(payload.get("first_seen_at", "") or ""),
            decision_trail=[str(item) for item in payload.get("decision_trail", [])],
            manual_override_status=str(payload.get("manual_override_status", "") or ""),
            manual_override_note=str(payload.get("manual_override_note", "") or ""),
        )


@dataclass
class SyncSnapshot:
    status: str = "idle"
    message: str = "Pronto para sincronizar."
    started_at: str = ""
    finished_at: str = ""
    last_success_at: str = ""
    total_fines: int = 0
    error: str = ""

    @staticmethod
    def _label_now() -> str:
        return now_label()

    def mark_running(self, message: str) -> None:
        self.status = "running"
        self.message = message
        self.started_at = self._label_now()
        self.finished_at = ""
        self.error = ""

    def mark_success(self, total_fines: int) -> None:
        timestamp = self._label_now()
        self.status = "success"
        self.message = "Sincronizacao concluida com sucesso."
        self.finished_at = timestamp
        self.last_success_at = timestamp
        self.total_fines = total_fines
        self.error = ""

    def mark_error(self, message: str) -> None:
        self.status = "error"
        self.message = message
        self.finished_at = self._label_now()
        self.error = message

    def to_dict(self) -> dict[str, object]:
        return asdict(self)
