from __future__ import annotations

import asyncio
import re
import unicodedata
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from .config import CONFIG, DOWNLOAD_DIR, ensure_directories
from .models import FineRecord

if TYPE_CHECKING:
    from playwright.async_api import Page


StatusCallback = Callable[[str], None]


@dataclass
class BoletoExtractionResult:
    valor: Decimal
    boleto_disponivel: bool
    valor_disponivel: bool
    mensagem: str
    fonte_valor: str = ""
    divida_quitada: bool = False

URL_LOGIN = "https://appweb1.antt.gov.br/spmi/Site/Login.aspx?ReturnUrl=%2fspmi%2fSite%2fBoleto%2fListar.aspx"
URL_DESTINO_LOGIN = "**/*Default.aspx"
URL_VISTAS_PROCESSO = "https://appweb1.antt.gov.br/spmi/Site/Acessos/VistasAoProcesso.aspx"
URL_BOLETO_LISTAR = "https://appweb1.antt.gov.br/spmi/Site/Boleto/Listar.aspx"

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
SELECTOR_REPRESENTADO_BOLETO = "#ContentPlaceHolderCorpo_ContentPlaceHolderCorpo_ContentPlaceHolderCorpo_ddlRepresentado"
SELECTOR_TIPO_MULTA_BOLETO = "#ContentPlaceHolderCorpo_ContentPlaceHolderCorpo_ContentPlaceHolderCorpo_ddlTipoMulta"
SELECTOR_BOTAO_PESQUISAR_BOLETO = "#ContentPlaceHolderCorpo_ContentPlaceHolderCorpo_ContentPlaceHolderCorpo_btnPesquisar"
PDF_MODAL_SELECTORS = (
    "#MessageBoxPesquisa",
    "#MessageBox",
    "div[id*='MessageBox']",
    ".ui-dialog",
    ".modal",
    ".swal2-popup",
)
PDF_MODAL_BUTTON_SELECTORS = (
    "#MessageBoxPesquisa_ButtonOkPesquisa",
    "#MessageBoxPesquisa_ButtonOk",
    "#MessageBox_ButtonOK",
    "input[value='OK']",
    "button:has-text('OK')",
    "button:has-text('Fechar')",
    "a:has-text('Fechar')",
)
PDF_ERROR_PATTERNS = (
    "FALHADECOMUNICACAO",
    "ERRODECOMUNICACAO",
    "FALHANACOMUNICACAO",
    "FALHACOMUNICACAO",
)
PDF_DOWNLOAD_TIMEOUT_MS = 45000
PDF_DOWNLOAD_RETRIES = 3

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
    try:
        from playwright.async_api import async_playwright
    except Exception as exc:
        raise RuntimeError(
            "Playwright nao esta instalado no servidor. Adicione a dependencia no deploy."
        ) from exc

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=CONFIG.playwright_headless)
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
                if CONFIG.playwright_headless:
                    callback("Fluxo em modo headless. Se houver CAPTCHA, a sincronizacao pode falhar no servidor.")
                else:
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

            if fines:
                await _crosscheck_with_boleto_list(page, fines, callback)

        finally:
            await browser.close()

    return fines


async def _wait_modal_cycle(page: "Page") -> None:
    try:
        await page.wait_for_selector(SELECTOR_MODAL_PROCESSANDO, state="visible", timeout=10000)
    except Exception:
        pass
    try:
        await page.wait_for_selector(SELECTOR_MODAL_PROCESSANDO, state="hidden", timeout=120000)
    except Exception:
        pass


async def _crosscheck_with_boleto_list(
    page: "Page",
    fines: list[FineRecord],
    callback: StatusCallback | None,
) -> None:
    try:
        if callback:
            callback("Validando multas na pagina de boletos.")
        await page.goto(URL_BOLETO_LISTAR, wait_until="networkidle")
        await _wait_modal_cycle(page)

        representado = await _select_option_containing_text(
            page,
            SELECTOR_REPRESENTADO_BOLETO,
            CONFIG.antt_representado_match,
        )
        if not representado:
            if callback:
                callback("Nao foi possivel selecionar o representado na pagina de boletos.")
            return
        await _wait_modal_cycle(page)

        tipo_options = await _get_select_options(page, SELECTOR_TIPO_MULTA_BOLETO)
        if not tipo_options:
            if callback:
                callback("Nao foi possivel carregar os tipos de multa em Boleto/Listar.aspx.")
            return

        matched_indexes: set[int] = set()
        for option in tipo_options:
            if callback:
                callback(f"Cruzando boletos: {option['label']}.")
            await page.select_option(SELECTOR_TIPO_MULTA_BOLETO, value=option["value"])
            await _wait_modal_cycle(page)
            await page.click(SELECTOR_BOTAO_PESQUISAR_BOLETO, timeout=10000)
            await _wait_modal_cycle(page)
            matched_indexes.update(await _extract_boleto_matches_from_page(page, fines))

        for index, fine in enumerate(fines):
            if index not in matched_indexes:
                continue
            fine.boleto_disponivel = True
            if not fine.fonte_valor:
                fine.fonte_valor = "lista_boletos"
            if not fine.valor_disponivel:
                fine.mensagem_valor = "Boleto localizado em Boleto/Listar.aspx"
    except Exception as exc:
        if callback:
            callback(f"Falha ao cruzar a pagina de boletos: {exc}")


async def _select_option_containing_text(page: "Page", selector: str, text: str) -> str:
    wanted = (text or "").strip()
    if not wanted:
        return ""

    return await page.eval_on_selector(
        selector,
        """
        (element, wantedText) => {
          const wanted = (wantedText || '').toUpperCase();
          const options = Array.from(element.options || []);
          const option = options.find((item) => (item.textContent || '').toUpperCase().includes(wanted));
          if (!option) {
            return '';
          }
          element.value = option.value;
          element.dispatchEvent(new Event('input', { bubbles: true }));
          element.dispatchEvent(new Event('change', { bubbles: true }));
          return option.textContent || '';
        }
        """,
        wanted,
    )


async def _get_select_options(page: "Page", selector: str) -> list[dict[str, str]]:
    return await page.eval_on_selector(
        selector,
        """
        (element) => Array.from(element.options || [])
          .filter((option) => option.value)
          .map((option) => ({
            value: option.value,
            label: (option.textContent || '').trim()
          }))
        """,
    )


async def _extract_boleto_matches_from_page(page: "Page", fines: list[FineRecord]) -> set[int]:
    rows = await page.locator("table tr").all()
    matched_indexes: set[int] = set()

    for row in rows:
        row_text = " ".join((((await row.text_content()) or "").split()))
        row_lookup = _normalize_lookup_text(row_text)
        if not row_lookup or "NENHUMREGISTRO" in row_lookup:
            continue

        for index, fine in enumerate(fines):
            if index in matched_indexes:
                continue

            process_lookup = _normalize_lookup_text(fine.numero_processo)
            auto_lookup = _normalize_lookup_text(fine.auto_infracao)
            process_match = bool(process_lookup and process_lookup in row_lookup)
            auto_match = bool(auto_lookup and auto_lookup in row_lookup)
            if process_match or auto_match:
                matched_indexes.add(index)

    return matched_indexes


async def _cancel_download_task(download_task: "asyncio.Task[object] | None") -> None:
    if not download_task or download_task.done():
        return
    download_task.cancel()
    try:
        await download_task
    except BaseException:
        pass


async def _visible_pdf_error_message(page: "Page") -> str:
    for selector in PDF_MODAL_SELECTORS:
        locator = page.locator(selector).first
        try:
            if not await locator.is_visible():
                continue
            text = " ".join((((await locator.text_content()) or "").split()))
            normalized = _normalize_pdf_text(text)
            if any(pattern in normalized for pattern in PDF_ERROR_PATTERNS):
                return text or "Falha de comunicacao"
        except Exception:
            continue

    return ""


async def _dismiss_pdf_error_modal(page: "Page") -> None:
    for selector in PDF_MODAL_BUTTON_SELECTORS:
        locator = page.locator(selector).first
        try:
            if await locator.is_visible():
                await locator.click(timeout=1000)
                await page.wait_for_timeout(250)
                return
        except Exception:
            continue

    try:
        await page.keyboard.press("Escape")
    except Exception:
        pass


async def _prepare_pdf_download(page: "Page") -> str:
    for _ in range(20):
        try:
            if await page.locator(SELECTOR_RADIO_NAO).is_visible():
                await page.click(SELECTOR_RADIO_NAO)
                await page.wait_for_timeout(250)
                await page.click(SELECTOR_BOTAO_OK_PESQUISA)
                return ""
        except Exception:
            pass

        error_message = await _visible_pdf_error_message(page)
        if error_message:
            return error_message

        await page.wait_for_timeout(250)

    return ""


async def _extract_table_data(page: "Page", tipo: str, callback: StatusCallback | None) -> list[FineRecord]:
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
        boleto = BoletoExtractionResult(
            valor=Decimal("0"),
            boleto_disponivel=False,
            valor_disponivel=False,
            mensagem="Boleto e valor ainda nao estao disponiveis",
        )

        if pdf_name:
            boleto = await _download_pdf_and_extract_value(page, auto, pdf_path, callback)
            if boleto.divida_quitada:
                if callback:
                    callback(f"Auto {auto} ignorado porque o PDF indica divida quitada.")
                continue

        fines.append(
            FineRecord(
                tipo_fiscalizacao=tipo,
                auto_infracao=auto,
                numero_processo=processo,
                autuado=autuado,
                situacao=situacao,
                data_auto=data_auto,
                valor_multa=boleto.valor,
                pdf_nome=pdf_name if pdf_name and pdf_path.exists() else "",
                boleto_disponivel=boleto.boleto_disponivel,
                valor_disponivel=boleto.valor_disponivel,
                mensagem_valor=boleto.mensagem,
                fonte_valor=boleto.fonte_valor,
            )
        )

    return fines


async def _download_pdf_and_extract_value(
    page: "Page",
    auto_infracao: str,
    pdf_path: Path,
    callback: StatusCallback | None,
) -> BoletoExtractionResult:
    if pdf_path.exists():
        return _extract_pdf_value(pdf_path)

    last_error_message = ""

    for attempt in range(1, PDF_DOWNLOAD_RETRIES + 1):
        row_locator = page.locator(f"{SELECTOR_TABELA_RESULTADO} tbody tr:has-text(\"{auto_infracao}\")").first
        button = row_locator.locator(
            '[id^="Corpo_gdvResultado_btnVisualizar"], [id*="btnVisualizar"], a:has-text("Visualizar")'
        ).first
        download_task: asyncio.Task[object] | None = None

        try:
            download_task = asyncio.create_task(page.wait_for_event("download", timeout=PDF_DOWNLOAD_TIMEOUT_MS))
            await button.click(timeout=10000)
            immediate_error = await _prepare_pdf_download(page)
            if immediate_error:
                last_error_message = immediate_error
                await _dismiss_pdf_error_modal(page)
                raise RuntimeError(immediate_error)

            deadline = asyncio.get_running_loop().time() + (PDF_DOWNLOAD_TIMEOUT_MS / 1000)
            while True:
                if download_task.done():
                    download = await download_task
                    await download.save_as(str(pdf_path))
                    return _extract_pdf_value(pdf_path)

                modal_error = await _visible_pdf_error_message(page)
                if modal_error:
                    last_error_message = modal_error
                    await _dismiss_pdf_error_modal(page)
                    raise RuntimeError(modal_error)

                if asyncio.get_running_loop().time() >= deadline:
                    last_error_message = "Tempo limite esgotado ao gerar o PDF."
                    raise TimeoutError(last_error_message)

                await page.wait_for_timeout(400)
        except Exception:
            await _cancel_download_task(download_task)
            if attempt < PDF_DOWNLOAD_RETRIES:
                if callback:
                    callback(
                        f"Falha ao gerar o PDF de {auto_infracao} (tentativa {attempt}/{PDF_DOWNLOAD_RETRIES}). Tentando novamente."
                    )
                await page.wait_for_timeout(1000 * attempt)
                continue
        finally:
            await _cancel_download_task(download_task)

        break

    if callback:
        callback(f"Nao foi possivel baixar o PDF de {auto_infracao}.")
    mensagem = "Boleto e valor ainda nao estao disponiveis"
    normalized_error = _normalize_pdf_text(last_error_message)
    if any(pattern in normalized_error for pattern in PDF_ERROR_PATTERNS):
        mensagem = "Portal ANTT falhou ao gerar o PDF nesta leitura"
    return BoletoExtractionResult(
        valor=Decimal("0"),
        boleto_disponivel=False,
        valor_disponivel=False,
        mensagem=mensagem,
    )


def _extract_pdf_value(pdf_path: Path) -> BoletoExtractionResult:
    try:
        import pdfplumber
    except Exception:
        return BoletoExtractionResult(
            valor=Decimal("0"),
            boleto_disponivel=False,
            valor_disponivel=False,
            mensagem="Nao foi possivel processar o PDF do boleto",
        )

    page_texts: list[str] = []

    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            for page in pdf.pages:
                for extracted in (page.extract_text(layout=True), page.extract_text()):
                    if not extracted:
                        continue
                    normalized = _normalize_pdf_text(extracted)
                    if normalized:
                        page_texts.append(normalized)
    except Exception:
        return BoletoExtractionResult(
            valor=Decimal("0"),
            boleto_disponivel=False,
            valor_disponivel=False,
            mensagem="Nao foi possivel ler o PDF do boleto",
        )

    full_text = "\n".join(page_texts)
    if _is_paid_debt_pdf(full_text):
        return BoletoExtractionResult(
            valor=Decimal("0"),
            boleto_disponivel=True,
            valor_disponivel=False,
            mensagem="Divida quitada identificada no PDF",
            fonte_valor="quitada",
            divida_quitada=True,
        )

    has_boleto = _has_boleto_markers(full_text)
    if not has_boleto:
        return BoletoExtractionResult(
            valor=Decimal("0"),
            boleto_disponivel=False,
            valor_disponivel=False,
            mensagem="Boleto e valor ainda nao estao disponiveis",
        )

    value = None
    for page_text in page_texts:
        if not _has_boleto_markers(page_text):
            continue
        if _is_paid_debt_pdf(page_text):
            return BoletoExtractionResult(
                valor=Decimal("0"),
                boleto_disponivel=True,
                valor_disponivel=False,
                mensagem="Divida quitada identificada no PDF",
                fonte_valor="quitada",
                divida_quitada=True,
            )
        value = _extract_boleto_document_value(page_text)
        if value is not None:
            break

    if value is None:
        value = _extract_boleto_document_value(full_text)

    if value is None:
        return BoletoExtractionResult(
            valor=Decimal("0"),
            boleto_disponivel=True,
            valor_disponivel=False,
            mensagem="Boleto disponivel, mas o valor do documento nao foi encontrado",
            fonte_valor="boleto",
        )

    return BoletoExtractionResult(
        valor=value,
        boleto_disponivel=True,
        valor_disponivel=True,
        mensagem="Valor do documento do boleto encontrado",
        fonte_valor="valor_do_documento",
    )


def _extract_boleto_document_value(full_text: str) -> Decimal | None:
    normalized_text = _normalize_pdf_text(full_text)
    patterns = [
        r"VALOR\s+DO\s+DOCUMENTO[\s:.-]*R?\$?\s*(\d{1,3}(?:\.\d{3})*,\d{2})",
        r"VALOR\s+DOCUMENTO[\s:.-]*R?\$?\s*(\d{1,3}(?:\.\d{3})*,\d{2})",
        r"1\s*-\s*\(\+\)\s*VALOR\s+DO\s+DOCUMENTO[\s:.-]*R?\$?\s*(\d{1,3}(?:\.\d{3})*,\d{2})",
        r"1\s*-\s*\(\+\)\s*VALOR\s+DOCUMENTO[\s:.-]*R?\$?\s*(\d{1,3}(?:\.\d{3})*,\d{2})",
        r"(\d{1,3}(?:\.\d{3})*,\d{2})[\s:.-]*1\s*-\s*\(\+\)\s*VALOR\s+DO\s+DOCUMENTO",
        r"(\d{1,3}(?:\.\d{3})*,\d{2})[\s:.-]*1\s*-\s*\(\+\)\s*VALOR\s+DOCUMENTO",
        r"QUANTIDADE\s+VALOR[\s:.-]*R?\$?\s*(\d{1,3}(?:\.\d{3})*,\d{2})",
    ]

    for pattern in patterns:
        match = re.search(pattern, normalized_text, flags=re.IGNORECASE | re.MULTILINE)
        if not match:
            continue
        value = _parse_boleto_amount(match.group(1))
        if value is not None:
            return value

    line_candidates = [
        line.strip()
        for line in normalized_text.splitlines()
        if "VALOR DO DOCUMENTO" in line or "VALOR DOCUMENTO" in line or "QUANTIDADE VALOR" in line
    ]
    for line in line_candidates:
        amount_match = re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2})", line)
        if not amount_match:
            continue
        value = _parse_boleto_amount(amount_match.group(1))
        if value is not None:
            return value

    window_pattern = re.compile(
        r"(?:VALOR\s+(?:DO\s+)?DOCUMENTO|QUANTIDADE\s+VALOR)(.{0,160})",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in window_pattern.finditer(normalized_text):
        window = match.group(1)
        amount_match = re.search(r"(\d{1,3}(?:\.\d{3})*,\d{2})", window)
        if not amount_match:
            continue
        value = _parse_boleto_amount(amount_match.group(1))
        if value is not None:
            return value

    return None


def _has_boleto_markers(full_text: str) -> bool:
    boleto_markers = [
        "BANCO DO BRASIL",
        "GRU - COBRANCA",
        "GRU COBRANCA",
        "VALOR DO DOCUMENTO",
        "VALOR DOCUMENTO",
        "LINHA DIGITAVEL",
        "PAGAVEL EM QUALQUER BANCO",
    ]
    return any(marker in full_text for marker in boleto_markers)


def _is_paid_debt_pdf(full_text: str) -> bool:
    if "QUITADA" not in full_text:
        return False

    paid_markers = [
        "SITUACAO DA DIVIDA",
        "DADOS REFERENTES AOS PAGAMENTOS REALIZADOS",
        "EXTRATO DE PAGAMENTOS",
        "DATA DE PAGAMENTO",
        "SALDO DO PAGAMENTO",
        "SALDO RESIDUAL",
        "SALDO CORRIGIDO PARA PAGAMENTO",
        "QUANTIDADE DE PAGAMENTOS REALIZADOS",
    ]
    has_payment_context = any(marker in full_text for marker in paid_markers)
    if not has_payment_context:
        return False

    if re.search(r"SITUACAO\s+DA\s+DIVIDA[\s:.-]*QUITADA", full_text, flags=re.IGNORECASE):
        return True

    if re.search(r"SITUACAO[\s:.-]*QUITADA", full_text, flags=re.IGNORECASE) and "SALDO DO PAGAMENTO" in full_text:
        return True

    if "QUITADA" in full_text and ("SALDO RESIDUAL" in full_text or "QUANTIDADE DE PAGAMENTOS REALIZADOS" in full_text):
        return True

    if re.search(r"SALDO\s+(?:DO\s+PAGAMENTO|RESIDUAL)[\s:.-]*R?\$?\s*0,00", full_text, flags=re.IGNORECASE):
        return True

    return False


def _normalize_pdf_text(value: str) -> str:
    folded = unicodedata.normalize("NFKD", value)
    ascii_text = "".join(char for char in folded if not unicodedata.combining(char))
    normalized_lines = [" ".join(line.upper().split()) for line in ascii_text.splitlines()]
    return "\n".join(line for line in normalized_lines if line)


def _normalize_lookup_text(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", _normalize_pdf_text(value))


def _parse_boleto_amount(value: str) -> Decimal | None:
    normalized = (value or "").replace(".", "").replace(",", ".").strip()
    try:
        parsed = Decimal(normalized)
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed > Decimal("0") else None


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
            boleto_disponivel=True,
            valor_disponivel=True,
            mensagem_valor="Valor do documento do boleto encontrado",
            fonte_valor="valor_do_documento",
            status_carteira="ativa_com_boleto",
            ja_teve_boleto=True,
        ),
        FineRecord(
            tipo_fiscalizacao="Infraestrutura Rodoviaria",
            auto_infracao="FRM00022026",
            numero_processo="50501.000002/2026-22",
            autuado="FRUTAMINA - COMERCIAL AGRICOLA LTDA.",
            situacao="Processo em andamento",
            data_auto="22/03/2026",
            valor_multa=Decimal("0"),
            boleto_disponivel=False,
            valor_disponivel=False,
            mensagem_valor="Boleto e valor ainda nao estao disponiveis",
            status_carteira="ativa_sem_boleto",
            ja_teve_boleto=False,
        ),
    ]
