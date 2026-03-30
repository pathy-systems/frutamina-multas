(function () {
  function byId(id) {
    return document.getElementById(id);
  }

  function parseJsonScript(id, fallback) {
    const node = byId(id);
    if (!node) {
      return fallback;
    }

    try {
      return JSON.parse(node.textContent || "");
    } catch (error) {
      console.error("Falha ao ler JSON inicial.", error);
      return fallback;
    }
  }

  function escapeHtml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function trailMarkup(decisionTrail) {
    if (!decisionTrail || !decisionTrail.length) {
      return '<div class="empty-state compact-empty">Sem trilha registrada ainda.</div>';
    }

    return `
      <ul class="decision-list">
        ${decisionTrail.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
      </ul>
    `;
  }

  function buildLookupKey(auto, processo) {
    return `${auto || ""}__${processo || ""}`;
  }

  function historyIcon() {
    return `
      <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
        <path d="M6 4h9l3 3v13H6z" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linejoin="round"></path>
        <path d="M15 4v3h3" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linejoin="round"></path>
        <path d="M9 12h6M9 16h6M9 8h3" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"></path>
      </svg>
    `;
  }

  function paidIcon() {
    return `
      <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
        <circle cx="12" cy="12" r="8.2" fill="none" stroke="currentColor" stroke-width="1.8"></circle>
        <path d="M8.5 12.3l2.2 2.2 4.8-5" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"></path>
      </svg>
    `;
  }

  function buildPdfDocument(rows) {
    const printableRows = rows.map((item) => `
      <tr>
        <td>${escapeHtml(item.auto)}</td>
        <td>${escapeHtml(item.tipo)}</td>
        <td>${escapeHtml(item.processo)}</td>
        <td>${escapeHtml(item.situacao)}</td>
        <td>${escapeHtml(item.dataAuto)}</td>
        <td>${escapeHtml(item.valorDisponivel ? item.valor : (item.mensagemValor || "Sem valor"))}</td>
        <td>${escapeHtml(item.statusCarteiraLabel || "")}</td>
      </tr>
    `).join("");

    return `
      <!DOCTYPE html>
      <html lang="pt-BR">
      <head>
        <meta charset="utf-8">
        <title>Multas ANTT</title>
        <style>
          body { font-family: Arial, sans-serif; margin: 24px; color: #102340; }
          h1 { margin: 0 0 8px; font-size: 24px; }
          p { margin: 0 0 16px; color: #5f6e86; }
          table { width: 100%; border-collapse: collapse; }
          th, td { border: 1px solid #d9e2f0; padding: 10px 12px; text-align: left; vertical-align: top; }
          th { background: #f4f7fd; font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em; }
          td { font-size: 13px; }
        </style>
      </head>
      <body>
        <h1>Multas ANTT</h1>
        <p>Exportado do painel com ${rows.length} multa(s).</p>
        <table>
          <thead>
            <tr>
              <th>Auto</th>
              <th>Tipo</th>
              <th>Processo</th>
              <th>Situacao</th>
              <th>Data</th>
              <th>Valor / Status</th>
              <th>Status interno</th>
            </tr>
          </thead>
          <tbody>${printableRows}</tbody>
        </table>
      </body>
      </html>
    `;
  }

  function findReviewNoteField(key) {
    return Array.from(reviewList.querySelectorAll("textarea[data-key]")).find((node) => node.dataset.key === key) || null;
  }

  const dashboardRoot = byId("finesTableBody");
  if (!dashboardRoot) {
    return;
  }

  let payload = parseJsonScript("initialDashboardPayload", {
    summary: {
      total_fines: 0,
      total_value: "R$ 0,00",
      available_boleto_count: 0,
      pending_boleto_count: 0,
      review_count: 0,
      manual_quitada_count: 0,
      new_count: 0,
      active_types: 0,
      updated_at: "Sem sincronizacao"
    },
    new_fines: [],
    type_counts: [],
    portfolio_status_counts: [],
    top_fines: [],
    agent_status: {
      status: "offline",
      statusLabel: "Sem sinal",
      message: "Aguardando heartbeat do agente.",
      current_job_id: "",
      last_seen_at: "",
      agent_name: ""
    },
    review_items: [],
    paid_items: [],
    fines: []
  });
  let syncSnapshot = parseJsonScript("initialSyncSnapshot", {
    status: "idle",
    message: "Pronto para sincronizar."
  });
  const appMeta = parseJsonScript("appMeta", {
    syncMode: "embedded",
    databaseEnabled: false,
    recentJobs: []
  });
  const syncMode = appMeta.syncMode || "embedded";
  let recentJobs = appMeta.recentJobs || [];
  let lastSuccessKey = syncSnapshot.last_success_at || "";
  let currentHistoryTarget = null;
  let recentNewKeys = new Set((payload.new_fines || []).map((item) => buildLookupKey(item.auto, item.processo)));
  let hasLoadedOnce = false;

  const searchInput = byId("searchInput");
  const typeFilter = byId("typeFilter");
  const portfolioStatusFilter = byId("portfolioStatusFilter");
  const syncButton = byId("syncButton");
  const cancelSyncButton = byId("cancelSyncButton");
  const statusDot = byId("syncStatusDot");
  const statusTitle = byId("syncStatusTitle");
  const statusMessage = byId("syncStatusMessage");
  const jobsList = byId("jobsList");
  const reviewList = byId("reviewList");
  const paidList = byId("paidList");
  const historyPanel = byId("historyPanel");
  const agentStatusCard = byId("agentStatusCard");
  const newFinesBanner = byId("newFinesBanner");
  const newFinesBannerTitle = byId("newFinesBannerTitle");
  const newFinesBannerText = byId("newFinesBannerText");
  const newFinesToast = byId("newFinesToast");
  const tableExportButton = byId("tableExportButton");
  const tableExportMenu = byId("tableExportMenu");
  const exportPdfButton = byId("exportPdfButton");
  const exportCsvButton = byId("exportCsvButton");

  function findFine(auto, processo) {
    const key = buildLookupKey(auto, processo);
    return (
      payload.fines.find((item) => buildLookupKey(item.auto, item.processo) === key) ||
      payload.review_items.find((item) => buildLookupKey(item.auto, item.processo) === key) ||
      null
    );
  }

  function updateSummary() {
    byId("totalFinesValue").textContent = payload.summary.total_fines;
    byId("totalValueValue").textContent = payload.summary.total_value;
    byId("totalValueHint").textContent =
      `${payload.summary.available_boleto_count || 0} boleto(s) com valor encontrado | ` +
      `${payload.summary.review_count || 0} em revisao | ` +
      `${payload.summary.manual_quitada_count || 0} paga(s) manualmente`;
    byId("activeTypesValue").textContent = payload.summary.active_types;
    byId("newFinesValue").textContent = payload.summary.new_count || 0;
    byId("newFinesHint").textContent = payload.summary.new_count
      ? `${payload.summary.new_count} multa(s) ainda em janela de destaque`
      : "Nenhuma multa nova destacada";
    byId("updatedAtValue").textContent = payload.summary.updated_at;
  }

  function renderNewFinesBanner() {
    if (!newFinesBanner) {
      return;
    }

    const items = payload.new_fines || [];
    const count = payload.summary.new_count || 0;
    newFinesBanner.classList.toggle("banner-hidden", count === 0);
    if (!count) {
      return;
    }

    const preview = items.slice(0, 4).map((item) => item.auto).filter(Boolean);
    newFinesBannerTitle.textContent = `${count} multa(s) nova(s) em destaque`;
    newFinesBannerText.textContent = preview.length
      ? `Autos recentes: ${preview.join(", ")}${count > preview.length ? "..." : ""}.`
      : "A sincronizacao encontrou novas multas na carteira.";
  }

  function showNewFinesToast(items) {
    if (!newFinesToast || !items.length) {
      return;
    }

    const preview = items.slice(0, 3).map((item) => item.auto).filter(Boolean);
    newFinesToast.textContent = preview.length
      ? `Nova(s) multa(s): ${preview.join(", ")}${items.length > preview.length ? "..." : ""}`
      : "Novas multas identificadas na sincronizacao.";
    newFinesToast.hidden = false;
    newFinesToast.classList.add("toast-visible");
    window.clearTimeout(showNewFinesToast._timer);
    showNewFinesToast._timer = window.setTimeout(() => {
      newFinesToast.classList.remove("toast-visible");
      window.setTimeout(() => {
        newFinesToast.hidden = true;
      }, 260);
    }, 5000);
  }

  function updateTypeFilter() {
    const currentValue = typeFilter.value;
    typeFilter.innerHTML = '<option value="">Todos</option>';
    payload.type_counts.forEach((item) => {
      const option = document.createElement("option");
      option.value = item.name;
      option.textContent = item.name;
      typeFilter.appendChild(option);
    });
    typeFilter.value = currentValue;
  }

  function updatePortfolioStatusFilter() {
    const currentValue = portfolioStatusFilter.value;
    portfolioStatusFilter.innerHTML = '<option value="">Todos</option>';
    payload.portfolio_status_counts.forEach((item) => {
      const option = document.createElement("option");
      option.value = item.status;
      option.textContent = `${item.label} (${item.count})`;
      portfolioStatusFilter.appendChild(option);
    });
    portfolioStatusFilter.value = currentValue;
  }

  function filteredFines() {
    const term = (searchInput.value || "").trim().toLowerCase();
    const type = typeFilter.value;
    const portfolioStatus = portfolioStatusFilter.value;

    return payload.fines.filter((item) => {
      if (type && item.tipo !== type) {
        return false;
      }

      if (portfolioStatus && item.statusCarteira !== portfolioStatus) {
        return false;
      }

      if (!term) {
        return true;
      }

      const haystack = [
        item.auto,
        item.tipo,
        item.processo,
        item.autuado,
        item.situacao,
        item.dataAuto,
        item.statusCarteiraLabel,
        item.mensagemValor
      ]
        .join(" ")
        .toLowerCase();
      return haystack.includes(term);
    });
  }

  function renderTable() {
    const rows = filteredFines();
    byId("tableCount").textContent = `${rows.length} multa(s) exibida(s)`;

    if (!rows.length) {
      dashboardRoot.innerHTML = `
        <tr>
          <td colspan="8"><div class="empty-state">Nenhuma multa encontrada para o filtro aplicado.</div></td>
        </tr>
      `;
      return;
    }

    dashboardRoot.innerHTML = rows.map((item) => {
      const pdf = item.pdfUrl
        ? `<a class="pdf-link" href="${escapeHtml(item.pdfUrl)}" target="_blank" rel="noreferrer">Abrir PDF</a>`
        : `<span class="pdf-link pdf-link-disabled">Sem PDF</span>`;
      const newBadge = item.isNew
        ? `<span class="new-badge">Nova</span>`
        : "";

      const valueCell = item.valorDisponivel
        ? `
          <div class="value-cell">
            <strong>${escapeHtml(item.valor)}</strong>
            <span class="cell-muted">${escapeHtml(item.statusCarteiraLabel || "Ativa com boleto")} | Valor do documento</span>
          </div>
        `
        : `
          <div class="value-cell">
            <span class="value-pill ${item.boletoDisponivel ? "value-pill-warning" : "value-pill-muted"}">
              ${escapeHtml(item.mensagemValor || "Boleto e valor ainda nao estao disponiveis")}
            </span>
            <span class="cell-muted">${escapeHtml(item.statusCarteiraLabel || "Aguardando boleto")}</span>
          </div>
        `;

      return `
        <tr class="${item.isNew ? "table-row-new" : ""}">
          <td>
            <div class="cell-title">
              <div class="cell-inline">
                <strong>${escapeHtml(item.auto)}</strong>
                ${newBadge}
              </div>
              <span class="cell-muted">${escapeHtml(item.autuado)}</span>
            </div>
          </td>
          <td>${escapeHtml(item.tipo)}</td>
          <td>${escapeHtml(item.processo)}</td>
          <td>
            <div class="cell-title">
              <span class="badge">${escapeHtml(item.situacao)}</span>
              <span class="cell-muted">${escapeHtml(item.statusCarteiraLabel || "")}</span>
            </div>
          </td>
          <td>${escapeHtml(item.dataAuto)}</td>
          <td>${valueCell}</td>
          <td>${pdf}</td>
          <td>
            <div class="table-actions">
              <button
                class="button button-secondary icon-button"
                type="button"
                data-action="open-history"
                title="Abrir analise"
                aria-label="Abrir analise"
                data-auto="${escapeHtml(item.auto)}"
                data-processo="${escapeHtml(item.processo)}">
                ${historyIcon()}
                <span class="sr-only">Abrir analise</span>
              </button>
              <button
                class="button button-secondary icon-button icon-button-danger"
                type="button"
                data-action="mark-paid"
                title="Marcar paga"
                aria-label="Marcar paga"
                data-auto="${escapeHtml(item.auto)}"
                data-processo="${escapeHtml(item.processo)}">
                ${paidIcon()}
                <span class="sr-only">Marcar paga</span>
              </button>
            </div>
          </td>
        </tr>
      `;
    }).join("");
  }

  function setExportMenuOpen(isOpen) {
    if (!tableExportMenu || !tableExportButton) {
      return;
    }
    tableExportMenu.hidden = !isOpen;
    tableExportButton.setAttribute("aria-expanded", isOpen ? "true" : "false");
    tableExportButton.classList.toggle("is-open", isOpen);
    if (!isOpen) {
      tableExportButton.blur();
    }
  }

  function exportFilteredTableAsPdf() {
    const rows = filteredFines();
    if (!rows.length) {
      alert("Nao ha multas visiveis para exportar.");
      return;
    }

    const popup = window.open("", "_blank", "noopener,noreferrer,width=1100,height=800");
    if (!popup) {
      alert("O navegador bloqueou a janela de exportacao. Permita pop-ups para gerar o PDF.");
      return;
    }

    popup.document.open();
    popup.document.write(buildPdfDocument(rows));
    popup.document.close();
    popup.focus();
    window.setTimeout(() => {
      popup.print();
    }, 250);
  }

  function renderTypeCards() {
    const root = byId("typeCards");
    if (!payload.type_counts.length) {
      root.innerHTML = '<div class="empty-state">Nenhum tipo ativo no momento.</div>';
      return;
    }

    const maxCount = Math.max(...payload.type_counts.map((item) => item.count), 1);
    root.innerHTML = payload.type_counts.map((item) => `
      <article class="type-card">
        <div class="type-head">
          <strong>${escapeHtml(item.name)}</strong>
          <span>${item.count} multa(s)</span>
        </div>
        <div class="type-bar"><span style="width:${Math.max(12, Math.round((item.count / maxCount) * 100))}%"></span></div>
      </article>
    `).join("");
  }

  function renderTopFines() {
    const root = byId("topFines");
    if (!payload.top_fines.length) {
      root.innerHTML = '<div class="empty-state">Nenhum boleto com valor encontrado ainda.</div>';
      return;
    }

    root.innerHTML = payload.top_fines.map((item) => `
      <article class="top-fine-card">
        <strong>${escapeHtml(item.valor)}</strong>
        <h3>${escapeHtml(item.auto)}</h3>
        <p>${escapeHtml(item.tipo)}</p>
        <span>${escapeHtml(item.situacao)}</span>
      </article>
    `).join("");
  }

  function renderAgentStatus() {
    if (!agentStatusCard) {
      return;
    }

    const status = syncSnapshot.agent_status || payload.agent_status || {};
    const tone = status.online
      ? "agent-online"
      : ((status.status || "").toLowerCase() === "error" ? "agent-error" : "agent-offline");
    const lastSeen = status.last_seen_at || "Sem heartbeat";
    const message = status.message || "Aguardando heartbeat do agente.";
    const jobInfo = status.current_job_id
      ? `<span>Job atual: ${escapeHtml(status.current_job_id)}</span>`
      : "<span>Job atual: nenhum</span>";

    agentStatusCard.innerHTML = `
      <div class="agent-status-header">
        <span class="agent-ping ${tone}"></span>
        <div>
          <strong>${escapeHtml(status.statusLabel || "Sem sinal")}</strong>
          <p>${escapeHtml(status.agent_name || "Agente local")}</p>
        </div>
      </div>
      <div class="agent-status-body">
        <p>${escapeHtml(message)}</p>
        <div class="agent-meta">
          <span>Estado: ${escapeHtml(status.status || "offline")}</span>
          ${jobInfo}
          <span>Ultimo sinal: ${escapeHtml(lastSeen)}</span>
        </div>
      </div>
    `;
  }

  function renderSyncStatus() {
    const status = syncSnapshot.status || "idle";
    statusDot.className = `status-dot status-${status}`;
    statusTitle.textContent = ({
      idle: "Pronto",
      queued: "Na fila",
      running: "Sincronizando",
      canceled: "Cancelado",
      success: "Concluido",
      error: "Falha"
    })[status] || "Pronto";

    let detail = syncSnapshot.message || "Pronto para sincronizar.";
    if (syncSnapshot.last_success_at) {
      detail += ` Ultimo sucesso: ${syncSnapshot.last_success_at}.`;
    }
    statusMessage.textContent = detail;
    syncButton.disabled = status === "running" || status === "queued";
    cancelSyncButton.classList.toggle("button-hidden", !(status === "running" || status === "queued"));
    cancelSyncButton.disabled = !(status === "running" || status === "queued");

    if (status === "running") {
      syncButton.textContent = syncMode === "agent" ? "Agente sincronizando..." : "Sincronizando...";
    } else if (status === "queued") {
      syncButton.textContent = "Solicitacao na fila";
    } else {
      syncButton.textContent = syncMode === "agent" ? "Solicitar leitura agora" : "Ler multas agora";
    }
  }

  function renderJobs() {
    if (!jobsList) {
      return;
    }

    if (!recentJobs.length) {
      jobsList.innerHTML = '<div class="empty-state">Nenhuma solicitacao registrada ainda.</div>';
      return;
    }

    jobsList.innerHTML = recentJobs.map((job) => `
      <article class="job-card">
        <div class="job-header">
          <strong>${escapeHtml(job.status || "pending")}</strong>
          <span>${escapeHtml(job.requested_at || "")}</span>
        </div>
        <p>${escapeHtml(job.message || "")}</p>
        <div class="job-meta">
          <span>Solicitado por: ${escapeHtml(job.requested_by || "-")}</span>
          <span>Agente: ${escapeHtml(job.runner_name || "-")}</span>
        </div>
      </article>
    `).join("");
  }

  function renderReviewList() {
    if (!reviewList) {
      return;
    }

    if (!payload.review_items.length) {
      reviewList.innerHTML = '<div class="empty-state">Nenhuma multa em revisao no momento.</div>';
      return;
    }

    reviewList.innerHTML = payload.review_items.map((item) => {
      const key = buildLookupKey(item.auto, item.processo);
      return `
        <article class="review-card">
          <div class="review-card-head">
            <div>
              <div class="cell-inline">
                <strong>${escapeHtml(item.auto)}</strong>
                ${item.isNew ? '<span class="new-badge">Nova</span>' : ""}
              </div>
              <p>${escapeHtml(item.tipo)} | ${escapeHtml(item.processo)}</p>
            </div>
            <span class="value-pill value-pill-warning">${escapeHtml(item.statusCarteiraLabel)}</span>
          </div>
          <p class="review-message">${escapeHtml(item.mensagemValor || "Sem observacao.")}</p>
          ${trailMarkup(item.decisionTrail || [])}
          <label class="field">
            <span>Nota manual</span>
            <textarea class="review-note" data-key="${escapeHtml(key)}" rows="3" placeholder="Descreva a decisao manual">${escapeHtml(item.manualOverrideNote || "")}</textarea>
          </label>
          <div class="review-actions">
            <button class="button button-secondary mini-button" type="button" data-action="open-history" data-auto="${escapeHtml(item.auto)}" data-processo="${escapeHtml(item.processo)}">Historico</button>
            <button class="button button-secondary mini-button" type="button" data-review-action="manter_ativa" data-auto="${escapeHtml(item.auto)}" data-processo="${escapeHtml(item.processo)}">Manter ativa</button>
            <button class="button button-secondary mini-button" type="button" data-review-action="revisar" data-auto="${escapeHtml(item.auto)}" data-processo="${escapeHtml(item.processo)}">Revisar</button>
            <button class="button button-danger mini-button" type="button" data-review-action="marcar_quitada" data-auto="${escapeHtml(item.auto)}" data-processo="${escapeHtml(item.processo)}">Marcar paga</button>
            <button class="button button-secondary mini-button" type="button" data-review-action="limpar_override" data-auto="${escapeHtml(item.auto)}" data-processo="${escapeHtml(item.processo)}">Limpar</button>
          </div>
        </article>
      `;
    }).join("");
  }

  function renderPaidList() {
    if (!paidList) {
      return;
    }

    if (!payload.paid_items.length) {
      paidList.innerHTML = '<div class="empty-state">Nenhuma multa marcada manualmente como paga.</div>';
      return;
    }

    paidList.innerHTML = payload.paid_items.map((item) => `
      <article class="review-card paid-card">
        <div class="review-card-head">
          <div>
            <div class="cell-inline">
              <strong>${escapeHtml(item.auto)}</strong>
              ${item.isNew ? '<span class="new-badge">Nova</span>' : ""}
            </div>
            <p>${escapeHtml(item.tipo)} | ${escapeHtml(item.processo)}</p>
          </div>
          <span class="value-pill value-pill-muted">Paga manualmente</span>
        </div>
        <p class="review-message">${escapeHtml(item.manualOverrideNote || item.mensagemValor || "Marcada manualmente como paga.")}</p>
        ${trailMarkup(item.decisionTrail || [])}
        <div class="review-actions">
          <button class="button button-secondary mini-button" type="button" data-action="open-history" data-auto="${escapeHtml(item.auto)}" data-processo="${escapeHtml(item.processo)}">Historico</button>
          <button class="button button-secondary mini-button" type="button" data-paid-action="limpar_override" data-auto="${escapeHtml(item.auto)}" data-processo="${escapeHtml(item.processo)}">Remover marcacao</button>
        </div>
      </article>
    `).join("");
  }

  async function openHistory(auto, processo) {
    if (!historyPanel) {
      return;
    }

    currentHistoryTarget = { auto, processo };
    const item = findFine(auto, processo);
    historyPanel.className = "history-panel";
    historyPanel.innerHTML = '<div class="empty-state">Carregando historico da multa selecionada...</div>';

    try {
      const response = await fetch(`/api/fine-history?auto=${encodeURIComponent(auto || "")}&processo=${encodeURIComponent(processo || "")}`, {
        credentials: "same-origin"
      });
      if (!response.ok) {
        throw new Error("Nao foi possivel carregar o historico.");
      }

      const historyPayload = await response.json();
      const current = item || historyPayload.history[0] || {};
      const history = historyPayload.history || [];
      const currentStatus = current.statusCarteiraLabel || current.statusCarteira || "Em acompanhamento";

      historyPanel.innerHTML = `
        <div class="history-header">
          <div>
            <strong>${escapeHtml(current.auto || auto || "Sem auto")}</strong>
            <p>${escapeHtml(current.tipo || "")} | ${escapeHtml(current.processo || processo || "")}</p>
          </div>
          <span class="value-pill ${current.statusCarteira === "quitada_confirmada" ? "value-pill-muted" : "value-pill-warning"}">
            ${escapeHtml(currentStatus)}
          </span>
        </div>
        <p class="history-highlight">${escapeHtml(current.mensagemValor || current.message || "Sem observacao registrada.")}</p>
        <section class="history-block">
          <h3>Trilha de decisao atual</h3>
          ${trailMarkup(current.decisionTrail || [])}
        </section>
        <section class="history-block">
          <h3>Acao manual</h3>
          <label class="field">
            <span>Nota manual</span>
            <textarea id="historyNoteInput" rows="4" placeholder="Explique a decisao manual">${escapeHtml(current.manualOverrideNote || "")}</textarea>
          </label>
          <div class="review-actions">
            <button class="button button-secondary mini-button" type="button" data-history-action="manter_ativa" data-auto="${escapeHtml(auto)}" data-processo="${escapeHtml(processo)}">Manter ativa</button>
            <button class="button button-secondary mini-button" type="button" data-history-action="revisar" data-auto="${escapeHtml(auto)}" data-processo="${escapeHtml(processo)}">Marcar revisar</button>
            <button class="button button-danger mini-button" type="button" data-history-action="marcar_quitada" data-auto="${escapeHtml(auto)}" data-processo="${escapeHtml(processo)}">Marcar paga</button>
            <button class="button button-secondary mini-button" type="button" data-history-action="limpar_override" data-auto="${escapeHtml(auto)}" data-processo="${escapeHtml(processo)}">Limpar override</button>
          </div>
        </section>
        <section class="history-block">
          <h3>Linha do tempo</h3>
          <div class="history-timeline">
            ${history.length ? history.map((entry) => `
              <article class="history-item">
                <div class="history-item-head">
                  <strong>${escapeHtml(entry.createdAt || "")}</strong>
                  <span>${escapeHtml(entry.actor || "sistema")}</span>
                </div>
                <p>${escapeHtml(entry.message || "Sem mensagem.")}</p>
                <span class="history-badge">${escapeHtml(entry.statusCarteiraLabel || entry.statusCarteira || "")}</span>
                ${trailMarkup(entry.decisionTrail || [])}
              </article>
            `).join("") : '<div class="empty-state compact-empty">Nenhum evento historico registrado ainda.</div>'}
          </div>
        </section>
      `;
    } catch (error) {
      historyPanel.innerHTML = `<div class="empty-state">Falha ao carregar o historico: ${escapeHtml(error.message || error)}</div>`;
    }
  }

  async function refreshDashboardData(options) {
    const settings = options || {};
    const previousNewKeys = recentNewKeys;
    const response = await fetch("/api/dashboard-data", { credentials: "same-origin" });
    if (!response.ok) {
      return;
    }
    payload = await response.json();
    const currentNewItems = payload.new_fines || [];
    recentNewKeys = new Set(currentNewItems.map((item) => buildLookupKey(item.auto, item.processo)));
    const newlyArrived = currentNewItems.filter((item) => !previousNewKeys.has(buildLookupKey(item.auto, item.processo)));
    updateSummary();
    renderNewFinesBanner();
    updateTypeFilter();
    updatePortfolioStatusFilter();
    renderTable();
    renderTypeCards();
    renderTopFines();
    renderReviewList();
    renderPaidList();
    renderAgentStatus();
    if (hasLoadedOnce && newlyArrived.length) {
      showNewFinesToast(newlyArrived);
    }
    hasLoadedOnce = true;
    if (currentHistoryTarget && settings.keepHistory !== false) {
      await openHistory(currentHistoryTarget.auto, currentHistoryTarget.processo);
    }
  }

  async function refreshSyncStatus() {
    const response = await fetch("/api/sync-status", { credentials: "same-origin" });
    if (!response.ok) {
      return;
    }
    syncSnapshot = await response.json();
    recentJobs = syncSnapshot.jobs || recentJobs;
    renderSyncStatus();
    renderJobs();
    renderAgentStatus();
    if (syncSnapshot.status === "success" && syncSnapshot.last_success_at !== lastSuccessKey) {
      lastSuccessKey = syncSnapshot.last_success_at || "";
      await refreshDashboardData();
    }
  }

  async function startSync() {
    const response = await fetch("/api/sync-start", {
      method: "POST",
      credentials: "same-origin"
    });

    if (response.ok || response.status === 202) {
      await refreshSyncStatus();
      return;
    }

    const errorPayload = await response.json().catch(() => ({ message: "Falha ao iniciar sincronizacao." }));
    alert(errorPayload.message || errorPayload.error || "Falha ao iniciar sincronizacao.");
  }

  async function cancelSync() {
    const response = await fetch("/api/sync-cancel", {
      method: "POST",
      credentials: "same-origin"
    });

    if (response.ok) {
      await refreshSyncStatus();
      return;
    }

    const errorPayload = await response.json().catch(() => ({ message: "Falha ao cancelar sincronizacao." }));
    alert(errorPayload.message || errorPayload.error || "Falha ao cancelar sincronizacao.");
  }

  async function submitManualReview(auto, processo, action, note) {
    const normalizedNote = action === "limpar_override" ? "" : (note || "");
    const response = await fetch("/api/fines/review", {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json; charset=utf-8"
      },
      body: JSON.stringify({
        auto,
        processo,
        action,
        note: normalizedNote
      })
    });

    if (!response.ok) {
      const errorPayload = await response.json().catch(() => ({ message: "Falha ao registrar a revisao manual." }));
      alert(errorPayload.message || errorPayload.error || "Falha ao registrar a revisao manual.");
      return;
    }

    await refreshDashboardData();
    await refreshSyncStatus();
  }

  async function markFineAsPaid(auto, processo, note) {
    await submitManualReview(auto, processo, "marcar_quitada", note || "");
  }

  dashboardRoot.addEventListener("click", async (event) => {
    const historyButton = event.target.closest("[data-action='open-history']");
    if (historyButton) {
      await openHistory(historyButton.dataset.auto || "", historyButton.dataset.processo || "");
      return;
    }

    const paidButton = event.target.closest("[data-action='mark-paid']");
    if (!paidButton) {
      return;
    }
    const auto = paidButton.dataset.auto || "";
    const processo = paidButton.dataset.processo || "";
    const note = window.prompt("Observacao opcional para marcar esta multa como paga:") || "";
    await markFineAsPaid(auto, processo, note);
  });

  if (tableExportButton && tableExportMenu) {
    tableExportButton.addEventListener("click", (event) => {
      event.stopPropagation();
      setExportMenuOpen(tableExportMenu.hidden);
    });

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        setExportMenuOpen(false);
      }
    });

    document.addEventListener("click", (event) => {
      if (!tableExportMenu.hidden && !event.target.closest(".export-menu-shell")) {
        setExportMenuOpen(false);
      }
    });
  }

  if (exportPdfButton) {
    exportPdfButton.addEventListener("click", () => {
      setExportMenuOpen(false);
      exportFilteredTableAsPdf();
    });
  }

  if (exportCsvButton) {
    exportCsvButton.addEventListener("click", () => {
      setExportMenuOpen(false);
      window.location.href = "/export/csv";
    });
  }

  reviewList.addEventListener("click", async (event) => {
    const historyButton = event.target.closest("[data-action='open-history']");
    if (historyButton) {
      await openHistory(historyButton.dataset.auto || "", historyButton.dataset.processo || "");
      return;
    }

    const actionButton = event.target.closest("[data-review-action]");
    if (!actionButton) {
      return;
    }

    const auto = actionButton.dataset.auto || "";
    const processo = actionButton.dataset.processo || "";
    const key = buildLookupKey(auto, processo);
    const noteField = findReviewNoteField(key);
    const action = actionButton.dataset.reviewAction || "";
    if (action === "limpar_override" && noteField) {
      noteField.value = "";
    }
    const note = noteField ? noteField.value : "";
    await submitManualReview(auto, processo, action, note);
    await openHistory(auto, processo);
  });

  if (paidList) {
    paidList.addEventListener("click", async (event) => {
      const historyButton = event.target.closest("[data-action='open-history']");
      if (historyButton) {
        await openHistory(historyButton.dataset.auto || "", historyButton.dataset.processo || "");
        return;
      }

      const actionButton = event.target.closest("[data-paid-action]");
      if (!actionButton) {
        return;
      }

      await submitManualReview(
        actionButton.dataset.auto || "",
        actionButton.dataset.processo || "",
        actionButton.dataset.paidAction || "",
        ""
      );
    });
  }

  historyPanel.addEventListener("click", async (event) => {
    const actionButton = event.target.closest("[data-history-action]");
    if (!actionButton) {
      return;
    }

    const noteField = byId("historyNoteInput");
    const action = actionButton.dataset.historyAction || "";
    if (action === "limpar_override" && noteField) {
      noteField.value = "";
    }
    await submitManualReview(
      actionButton.dataset.auto || "",
      actionButton.dataset.processo || "",
      action,
      noteField ? noteField.value : ""
    );
    await openHistory(actionButton.dataset.auto || "", actionButton.dataset.processo || "");
  });

  searchInput.addEventListener("input", renderTable);
  typeFilter.addEventListener("change", renderTable);
  portfolioStatusFilter.addEventListener("change", renderTable);
  syncButton.addEventListener("click", startSync);
  cancelSyncButton.addEventListener("click", cancelSync);

  updateSummary();
  renderNewFinesBanner();
  updateTypeFilter();
  updatePortfolioStatusFilter();
  renderTable();
  renderTypeCards();
  renderTopFines();
  renderReviewList();
  renderPaidList();
  renderSyncStatus();
  renderJobs();
  renderAgentStatus();
  window.setInterval(refreshSyncStatus, 4000);
})();
