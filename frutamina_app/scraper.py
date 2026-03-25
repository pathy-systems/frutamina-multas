from __future__ import annotations

import asyncio
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable

import pdfplumber
from playwright.async_api import Page, async_playwright

from .config import CONFIG, DOWNLOAD_DIR, ensure_directories
from .models import FineRecord


StatusCallback = Callable[[str], None]

URL_LOGIN = "https://appweb1.antt.gov.br/spmi/Site/Login.aspx?ReturnUrl=%2fspmi%2fSite%2fBoleto%2fListar.aspx"
URL_DESTINO_LOGIN = "**/*Default.aspx"
URL_VISTAS_PROCESSO = "https://appweb1.antt.gov.br/spmi/Site/Acessos/VistasAoProcesso.aspx"

ID_LOGIN = "#ContentPlaceHolderCorpo_ContentPlaceHolderCorpo_ContentPlaceHolderCorpo_ContentPlaceHolderCorpo_txtLoginCpjCnpj"
ID_SENHA = "#ContentPlaceHolderCorpo_ContentPlaceHolderCorpo_ContentPlaceHolderCorpo_ContentPlaceHolderCorpo_txtLoginSenha"
RECAPTCHA_IFRAME_SELECTOR = 'iframe[title="reCAPTCHA"]'
RECAPTCHA_CHECKBOX_SELECTOR = ".recaptcha-checkbox-border"

SELECTOR_DROPDOWN_TIPO_FISCALIZACAO = "#Corpo_ddlTipoFiscalizacao"
SELECTOR_BOTAO_PESQUISAR = "#Corpo_btnPesquisar"
SELECTOR_MODAL_PROCESSANDO = "#Progress_DivProgress"
SELECTOR_TABELA_RESULTADO = "#Corpo_gdvResultado"
SELECTOR_NENHUM_REGISTRO = f"{SELECTOR_TABELA_RESULTADO} td[colspan='6']"
SELECTOR_RADIO_NAO = "#MessageBoxPesquisa_rdbNao"
SELECTOR_BOTAO_OK_PESQUISA = "#MessageBoxPesquisa_ButtonOkPesquisa"

OPCOES_FISCALIZACAO = {
    "Excesso de Peso": "2",
    "Cargas": "3",
    "Passageiros": "4",
    "Cargas Internacional": "5",
    "Passageiros Internacional": "7",
    "Infraestrutura Rodoviaria": "8",
    "Evasao de Pedagio": "9",
}


def run_sync(callback: StatusCallback | None = None) -> list[FineRecord]:
    return asyncio.run(_run_sync(callback))


async def _run_sync(callback: StatusCallback | None = None) -> list[FineRecord]:
    if CONFIG.mock_sync:
        if callback:
            callback("Gerando dados de exemplo.")
        return _mock_fines()

    if not CONFIG.antt_user or not CONFIG.antt_password:
        raise RuntimeError("Defina ANTT_CPF_CNPJ e ANTT_SENHA no .env para sincronizar.")

    ensure_directories()
    fines: list[FineRecord] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=False)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        try:
            if callback:
                callback("Abrindo portal da ANTT.")
            await page.goto(URL_LOGIN, wait_until="load")
            await page.fill(ID_LOGIN, CONFIG.antt_user)
            await page.fill(ID_SENHA, CONFIG.antt_password)

            try:
                await page.wait_for_selector(RECAPTCHA_IFRAME_SELECTOR, timeout=5000)
                iframe = page.frame_locator(RECAPTCHA_IFRAME_SELECTOR)
                await iframe.locator(RECAPTCHA_CHECKBOX_SELECTOR).click(timeout=3000)
            except Exception:
                pass

            if callback:
                callback("Resolva o CAPTCHA e conclua o login no navegador aberto.")
            await page.wait_for_url(URL_DESTINO_LOGIN, timeout=180000, wait_until="networkidle")

            if callback:
                callback("Login concluido. Lendo multas ativas.")
            await page.goto(URL_VISTAS_PROCESSO, wait_until="networkidle")

            for nome, valor in OPCOES_FISCALIZACAO.items():
                if callback:
                    callback(f"Consultando {nome}.")
                await page.select_option(SELECTOR_DROPDOWN_TIPO_FISCALIZACAO, value=valor)
                await _wait_modal_cycle(page)
                await page.click(SELECTOR_BOTAO_PESQUISAR, timeout=10000)
                await _wait_modal_cycle(page)
                fines.extend(await _extract_table_data(page, nome, callback))

        finally:
            await browser.close()

    return fines


async def _wait_modal_cycle(page: Page) -> None:
    try:
        await page.wait_for_selector(SELECTOR_MODAL_PROCESSANDO, state="visible", timeout=10000)
    except Exception:
        pass
    try:
        await page.wait_for_selector(SELECTOR_MODAL_PROCESSANDO, state="hidden", timeout=120000)
    except Exception:
        pass


async def _extract_table_data(page: Page, tipo: str, callback: StatusCallback | None) -> list[FineRecord]:
    nenhum_registro = page.locator(SELECTOR_NENHUM_REGISTRO, has_text="Nenhum registro encontrado.")
    try:
        if await nenhum_registro.is_visible():
            return []
    except Exception:
        pass

    try:
        await page.wait_for_selector(SELECTOR_TABELA_RESULTADO, timeout=8000)
    except Exception:
        return []

    rows = await page.locator(f"{SELECTOR_TABELA_RESULTADO} > tbody > tr:not(:first-child)").all()
    fines: list[FineRecord] = []

    for row in rows:
        cells = row.locator("td")
        if await cells.count() < 5:
            continue

        auto = ((await cells.nth(0).text_content()) or "").strip()
        processo = ((await cells.nth(1).text_content()) or "").strip()
        autuado = ((await cells.nth(2).text_content()) or "").strip()
        situacao = " ".join((((await cells.nth(3).text_content()) or "").replace("\n", " ")).split())
        data_auto = ((await cells.nth(4).text_content()) or "").strip()

        if "Arquivado" in situacao or "Cancelado" in situacao:
            continue

        pdf_name = auto.replace("/", "_").replace("\\", "_") + ".pdf" if auto else ""
        pdf_path = DOWNLOAD_DIR / pdf_name if pdf_name else Path()
        valor = Decimal("0")

        if pdf_name:
            valor = await _download_pdf_and_extract_value(page, auto, pdf_path, callback)

        fines.append(
            FineRecord(
                tipo_fiscalizacao=tipo,
                auto_infracao=auto,
                numero_processo=processo,
                autuado=autuado,
                situacao=situacao,
                data_auto=data_auto,
                valor_multa=valor,
                pdf_nome=pdf_name if pdf_name and pdf_path.exists() else "",
            )
        )

    return fines


async def _download_pdf_and_extract_value(
    page: Page,
    auto_infracao: str,
    pdf_path: Path,
    callback: StatusCallback | None,
) -> Decimal:
    if pdf_path.exists():
        return _extract_pdf_value(pdf_path)

    row_locator = page.locator(f"{SELECTOR_TABELA_RESULTADO} tbody tr:has-text(\"{auto_infracao}\")").first
    button = row_locator.locator('[id^="Corpo_gdvResultado_btnVisualizar"], [id*="btnVisualizar"], a:has-text("Visualizar")').first

    try:
        async with page.expect_download(timeout=120000) as download_info:
            await button.click()
            await page.wait_for_selector(SELECTOR_RADIO_NAO, state="visible", timeout=10000)
            await page.click(SELECTOR_RADIO_NAO)
            await page.wait_for_timeout(400)
            await page.click(SELECTOR_BOTAO_OK_PESQUISA)
            download = await download_info.value
            await download.save_as(str(pdf_path))
    except Exception:
        if callback:
            callback(f"Nao foi possivel baixar o PDF de {auto_infracao}.")
        return Decimal("0")

    return _extract_pdf_value(pdf_path)


def _extract_pdf_value(pdf_path: Path) -> Decimal:
    monetary_pattern = re.compile(r"(\d{1,3}(?:\.\d{3})*,\d{2})")
    text_parts: list[str] = []

    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text() or ""
                if page_text:
                    text_parts.append(page_text.upper())
    except Exception:
        return Decimal("0")

    full_text = "\n".join(text_parts)
    matches = monetary_pattern.findall(full_text)
    values: list[Decimal] = []
    for match in matches:
        normalized = match.replace(".", "").replace(",", ".")
        try:
            value = Decimal(normalized)
        except (InvalidOperation, ValueError):
            continue
        if value >= Decimal("10"):
            values.append(value)

    return max(values) if values else Decimal("0")


def _mock_fines() -> list[FineRecord]:
    return [
        FineRecord(
            tipo_fiscalizacao="Excesso de Peso",
            auto_infracao="FRM00012026",
            numero_processo="50501.000001/2026-11",
            autuado="FRUTAMINA - COMERCIAL AGRICOLA LTDA.",
            situacao="Notificacao de penalidade emitida",
            data_auto="18/03/2026",
            valor_multa=Decimal("195.23"),
        ),
        FineRecord(
            tipo_fiscalizacao="Infraestrutura Rodoviaria",
            auto_infracao="FRM00022026",
            numero_processo="50501.000002/2026-22",
            autuado="FRUTAMINA - COMERCIAL AGRICOLA LTDA.",
            situacao="Processo em andamento",
            data_auto="22/03/2026",
            valor_multa=Decimal("850.00"),
        ),
    ]
