from __future__ import annotations

import csv
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from .config import DATA_DIR, DOWNLOAD_DIR, ensure_directories
from .models import FineRecord


JSON_PATH = DATA_DIR / "multas_ativas.json"
CSV_PATH = DATA_DIR / "multas_ativas.csv"


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


class FineStore:
    def __init__(self) -> None:
        ensure_directories()

    def load(self) -> list[FineRecord]:
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
        ensure_directories()
        JSON_PATH.write_text(
            json.dumps([fine.to_dict() for fine in fines], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        with CSV_PATH.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(
                file,
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

    def build_dashboard_payload(self) -> dict[str, object]:
        fines = self.load()
        total_valor = sum((fine.valor_multa for fine in fines), Decimal("0"))
        tipos: dict[str, int] = {}
        for fine in fines:
            tipos[fine.tipo_fiscalizacao] = tipos.get(fine.tipo_fiscalizacao, 0) + 1

        top_items = sorted(fines, key=lambda item: item.valor_multa, reverse=True)[:5]

        return {
            "summary": {
                "total_fines": len(fines),
                "total_value": _format_brl(total_valor),
                "active_types": len(tipos),
                "updated_at": self.last_updated_label(),
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
                    "pdfUrl": f"/downloads/{fine.pdf_nome}" if fine.pdf_nome else "",
                }
                for fine in fines
            ],
        }

    def csv_path(self) -> Path:
        return CSV_PATH

    def downloads_dir(self) -> Path:
        return DOWNLOAD_DIR

    def last_updated_label(self) -> str:
        source = JSON_PATH if JSON_PATH.exists() else CSV_PATH
        if not source.exists():
            return "Sem sincronizacao ainda"
        return datetime.fromtimestamp(source.stat().st_mtime).strftime("%d/%m/%Y %H:%M")
