/** Marketplace tabs and table quick-filter wiring. */

export function wireMarketTab(tab) {
  if (!tab || tab.dataset.marketWired === "true") return;
  tab.dataset.marketWired = "true";
  tab.addEventListener("click", () => {
    const board = tab.closest("[data-market-board]");
    if (!board) return;
    applyMarketView(board, tab.dataset.marketView);
  });
}

export function applyMarketView(board, view = "rankings") {
  if (!board) return;
  board.querySelectorAll("[data-market-view]").forEach((candidate) => {
    const active = candidate.dataset.marketView === view;
    candidate.classList.toggle("is-active", active);
    candidate.setAttribute("aria-selected", String(active));
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
