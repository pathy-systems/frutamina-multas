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

  const dashboardRoot = byId("finesTableBody");
  if (!dashboardRoot) {
    return;
  }

  let payload = parseJsonScript("initialDashboardPayload", {
    summary: { total_fines: 0, total_value: "R$ 0,00", active_types: 0, updated_at: "Sem sincronizacao" },
    type_counts: [],
    top_fines: [],
    fines: []
  });
  let syncSnapshot = parseJsonScript("initialSyncSnapshot", {
    status: "idle",
    message: "Pronto para sincronizar."
  });
  let lastSuccessKey = syncSnapshot.last_success_at || "";

  const searchInput = byId("searchInput");
  const typeFilter = byId("typeFilter");
  const syncButton = byId("syncButton");
  const statusDot = byId("syncStatusDot");
  const statusTitle = byId("syncStatusTitle");
  const statusMessage = byId("syncStatusMessage");

  function updateSummary() {
    byId("totalFinesValue").textContent = payload.summary.total_fines;
    byId("totalValueValue").textContent = payload.summary.total_value;
    byId("activeTypesValue").textContent = payload.summary.active_types;
    byId("updatedAtValue").textContent = payload.summary.updated_at;
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

  function filteredFines() {
    const term = (searchInput.value || "").trim().toLowerCase();
    const type = typeFilter.value;
    return payload.fines.filter((item) => {
      if (type && item.tipo !== type) {
        return false;
      }

      if (!term) {
        return true;
      }

      const haystack = [item.auto, item.tipo, item.processo, item.autuado, item.situacao, item.dataAuto]
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
          <td colspan="7"><div class="empty-state">Nenhuma multa encontrada para o filtro aplicado.</div></td>
        </tr>
      `;
      return;
    }

    dashboardRoot.innerHTML = rows.map((item) => {
      const pdf = item.pdfUrl
        ? `<a class="pdf-link" href="${escapeHtml(item.pdfUrl)}" target="_blank" rel="noreferrer">Abrir PDF</a>`
        : `<span class="pdf-link pdf-link-disabled">Sem PDF</span>`;

      return `
        <tr>
          <td>
            <div class="cell-title">
              <strong>${escapeHtml(item.auto)}</strong>
              <span class="cell-muted">${escapeHtml(item.autuado)}</span>
            </div>
          </td>
          <td>${escapeHtml(item.tipo)}</td>
          <td>${escapeHtml(item.processo)}</td>
          <td><span class="badge">${escapeHtml(item.situacao)}</span></td>
          <td>${escapeHtml(item.dataAuto)}</td>
          <td>${escapeHtml(item.valor)}</td>
          <td>${pdf}</td>
        </tr>
      `;
    }).join("");
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
      root.innerHTML = '<div class="empty-state">Nenhuma multa em destaque ainda.</div>';
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

  function renderSyncStatus() {
    const status = syncSnapshot.status || "idle";
    statusDot.className = `status-dot status-${status}`;
    statusTitle.textContent = ({
      idle: "Pronto",
      running: "Sincronizando",
      success: "Concluido",
      error: "Falha"
    })[status] || "Pronto";

    let detail = syncSnapshot.message || "Pronto para sincronizar.";
    if (syncSnapshot.last_success_at) {
      detail += ` Ultimo sucesso: ${syncSnapshot.last_success_at}.`;
    }
    statusMessage.textContent = detail;
    syncButton.disabled = status === "running";
    syncButton.textContent = status === "running" ? "Sincronizando..." : "Ler multas agora";
  }

  async function refreshDashboardData() {
    const response = await fetch("/api/dashboard-data", { credentials: "same-origin" });
    if (!response.ok) {
      return;
    }
    payload = await response.json();
    updateSummary();
    updateTypeFilter();
    renderTable();
    renderTypeCards();
    renderTopFines();
  }

  async function refreshSyncStatus() {
    const response = await fetch("/api/sync-status", { credentials: "same-origin" });
    if (!response.ok) {
      return;
    }
    syncSnapshot = await response.json();
    renderSyncStatus();
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

    const payload = await response.json().catch(() => ({ message: "Falha ao iniciar sincronizacao." }));
    alert(payload.message || payload.error || "Falha ao iniciar sincronizacao.");
  }

  searchInput.addEventListener("input", renderTable);
  typeFilter.addEventListener("change", renderTable);
  syncButton.addEventListener("click", startSync);

  updateSummary();
  updateTypeFilter();
  renderTable();
  renderTypeCards();
  renderTopFines();
  renderSyncStatus();
  window.setInterval(refreshSyncStatus, 4000);
})();
