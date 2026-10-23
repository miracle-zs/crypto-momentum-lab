/** Marketplace tabs and table quick-filter wiring. */

function wireMarketTab(tab) {
  if (!tab || tab.dataset.marketWired === "true") return;
  tab.dataset.marketWired = "true";
  tab.addEventListener("click", () => {
    const board = tab.closest("[data-market-board]");
    if (!board) return;
    applyMarketView(board, tab.dataset.marketView);
  });
  tab.addEventListener("keydown", (event) => {
    if (!["ArrowRight", "ArrowLeft", "Home", "End"].includes(event.key)) return;
    const board = tab.closest("[data-market-board]");
    const tabs = Array.from(board?.querySelectorAll("[data-market-view]") || []);
    const current = tabs.indexOf(tab);
    if (current < 0 || tabs.length === 0) return;
    const next = event.key === "Home"
      ? 0
      : event.key === "End"
        ? tabs.length - 1
        : (current + (event.key === "ArrowRight" ? 1 : -1) + tabs.length) % tabs.length;
    tabs[next].focus();
    applyMarketView(board, tabs[next].dataset.marketView);
    event.preventDefault();
  });
}

function applyMarketView(board, view = "rankings") {
  if (!board) return;
  board.querySelectorAll("[data-market-view]").forEach((candidate) => {
    const active = candidate.dataset.marketView === view;
    candidate.classList.toggle("is-active", active);
    candidate.setAttribute("aria-selected", String(active));
    candidate.tabIndex = active ? 0 : -1;
  });
  board.querySelectorAll("[data-market-panel]").forEach((panel) => {
    panel.hidden = panel.dataset.marketPanel !== view;
  });
}

export function wireMarketViews(root, selectedView = null) {
  const board = root?.querySelector("[data-market-board]");
  if (!board) return;
  board.querySelectorAll("[data-market-view]").forEach(wireMarketTab);
  if (selectedView) applyMarketView(board, selectedView);
}

export function wireTableFilters(root) {
  if (!root) return;
  root.querySelectorAll(".search-box").forEach((box) => {
    if (box.dataset.wired === "true") return;
    box.dataset.wired = "true";
    const input = box.querySelector("input[data-filter-table]");
    const clearBtn = box.querySelector(".search-clear");
    if (!input) return;
    const update = () => {
      const query = input.value.trim().toLowerCase();
      if (clearBtn) clearBtn.hidden = !query;
      const targetSelector = input.dataset.filterTable;
      const container = targetSelector
        ? box.closest(targetSelector) || root.querySelector(targetSelector)
        : box.closest(".block, .card, .market-panel");
      if (!container) return;
      const rows = container.querySelectorAll("tbody tr");
      rows.forEach((row) => {
        const text = row.textContent.toLowerCase();
        row.hidden = query.length > 0 && !text.includes(query);
      });
    };
    input.addEventListener("input", update);
    clearBtn?.addEventListener("click", () => {
      input.value = "";
      update();
      input.focus();
    });
  });
}

export function wireClickToCopy(root) {
  if (!root) return;
  const elements = root.querySelectorAll("td.cut, td.sym, code.notranslate, [data-copy-target]");
  elements.forEach((el) => {
    if (el.dataset.copyWired === "true") return;
    el.dataset.copyWired = "true";
    el.style.cursor = "pointer";
    if (!el.title) el.title = "点击复制到剪贴板";
    el.addEventListener("click", async (e) => {
      e.stopPropagation();
      const textToCopy = el.dataset.copyValue || el.textContent.trim();
      if (!textToCopy || textToCopy === "—") return;
      try {
        await navigator.clipboard.writeText(textToCopy);
        const originalTitle = el.title;
        el.title = "已复制 ✓";
        el.classList.add("copied-pulse");
        setTimeout(() => {
          el.title = originalTitle;
          el.classList.remove("copied-pulse");
        }, 1500);
      } catch {
        // clipboard fallback ignored
      }
    });
  });
}

export function wireTableSorting(root) {
  if (!root) return;
  const tables = root.querySelectorAll(".data-table");
  tables.forEach((table) => {
    const headers = table.querySelectorAll("thead th");
    headers.forEach((th, colIdx) => {
      if (th.dataset.sortWired === "true") return;
      th.dataset.sortWired = "true";
      th.tabIndex = 0;
      th.style.cursor = "pointer";
      th.title = "点击或按 Enter/空格排序";
      th.addEventListener("click", () => {
        const tbody = table.querySelector("tbody");
        if (!tbody) return;
        const rows = Array.from(tbody.querySelectorAll("tr"));
        if (rows.length <= 1) return;
        const currentAsc = th.dataset.sortOrder === "asc";
        const newOrder = currentAsc ? "desc" : "asc";
        headers.forEach((h) => {
          delete h.dataset.sortOrder;
          h.removeAttribute("aria-sort");
          const indicator = h.querySelector(".sort-indicator");
          if (indicator) indicator.remove();
        });
        th.dataset.sortOrder = newOrder;
        th.setAttribute("aria-sort", newOrder === "asc" ? "ascending" : "descending");
        const indicator = document.createElement("span");
        indicator.className = "sort-indicator";
        indicator.textContent = newOrder === "asc" ? " ▲" : " ▼";
        th.appendChild(indicator);

        rows.sort((a, b) => {
          const cellA = a.children[colIdx]?.textContent.trim().replace(/[$,%]/g, "") || "";
          const cellB = b.children[colIdx]?.textContent.trim().replace(/[$,%]/g, "") || "";
          const numA = Number(cellA);
          const numB = Number(cellB);
          if (!isNaN(numA) && !isNaN(numB)) {
            return newOrder === "asc" ? numA - numB : numB - numA;
          }
          return newOrder === "asc" ? cellA.localeCompare(cellB) : cellB.localeCompare(cellA);
        });
        rows.forEach((row) => tbody.appendChild(row));
      });
      th.addEventListener("keydown", (event) => {
        if (event.key !== "Enter" && event.key !== " ") return;
        event.preventDefault();
        th.click();
      });
    });
  });
}

export function wireGlobalShortcuts(shell, poller) {
  window.addEventListener("keydown", (e) => {
    const tag = document.activeElement?.tagName?.toLowerCase();
    if (tag === "input" || tag === "textarea" || tag === "select" || document.activeElement?.isContentEditable) {
      if (e.key === "Escape") {
        document.activeElement.blur();
      }
      return;
    }

    if (e.key >= "1" && e.key <= "5" && !e.ctrlKey && !e.metaKey && !e.altKey) {
      const opsViews = ["overview", "risk", "account", "reports", "performance"];
      const targetView = opsViews[Number(e.key) - 1];
      if (targetView && shell?.viewIds?.has(targetView)) {
        e.preventDefault();
        shell.setWorkspace("ops");
        shell.selectView(targetView, { userInitiated: true });
      }
    } else if ((e.key === "r" || e.key === "R") && !e.ctrlKey && !e.metaKey) {
      e.preventDefault();
      poller?.poll(true);
    } else if (e.key === "/" && !e.ctrlKey && !e.metaKey) {
      e.preventDefault();
      const visibleInput = document.querySelector(".card.is-active-view .search-input, .search-input");
      visibleInput?.focus();
    }
  });
}
