/** Workspace / view navigation shell. */

const WORKSPACES = Object.freeze({
  ops: {
    label: "运行控制",
    kicker: "OPERATIONS",
    purpose: "监控与账户控制",
    defaultView: "overview",
  },
  data: {
    label: "策略与数据",
    kicker: "ANALYTICS",
    purpose: "策略回放与数据流",
    defaultView: "strategy",
  },
});

const VIEW_PURPOSES = Object.freeze({
  overview: "系统状态与心跳监控",
  risk: "风控闸门与待核订单",
  account: "资金权益与持仓对账",
  reports: "运行会话与状态迁移",
  performance: "链路时延与系统性能监控",
  strategy: "策略对比与收益走势",
  universe: "UTC 动量排名与监控池",
  collector: "数据采集与窗口归档",
});

export function createShell({ onNavigate }) {
  const navLinks = new Map(
    Array.from(document.querySelectorAll(".nav a")).map((link) => [link.dataset.nav, link]),
  );
  const workspaceTabs = Array.from(document.querySelectorAll("[data-workspace-tab]"));
  const workspaceNavs = Array.from(document.querySelectorAll("[data-workspace-nav]"));
  const viewCards = Array.from(document.querySelectorAll("main .card"));
  const viewIds = new Set(viewCards.map((card) => card.id));
  const workspaceForView = new Map(
    viewCards.map((card) => [card.id, card.dataset.workspace || "ops"]),
  );

  function storedWorkspace() {
    try {
      const value = window.localStorage?.getItem("cml-dashboard-workspace");
      return Object.prototype.hasOwnProperty.call(WORKSPACES, value) ? value : "ops";
    } catch {
      return "ops";
    }
  }

  function normalizedView(value) {
    const id = String(value || "").replace(/^#/, "");
    return viewIds.has(id) ? id : "overview";
  }

  function viewLabel(id) {
    const card = document.getElementById(id);
    return card?.dataset.viewTitle || navLinks.get(id)?.dataset.label || id;
  }

  function syncWorkspace(workspace) {
    const key = WORKSPACES[workspace] ? workspace : "ops";
    const meta = WORKSPACES[key];
    document.body.dataset.activeWorkspace = key;
    document.body.dataset.workspace = key;
    workspaceTabs.forEach((tab) => {
      const active = tab.dataset.workspaceTab === key;
      tab.classList.toggle("is-active", active);
      tab.setAttribute("aria-selected", String(active));
    });
    workspaceNavs.forEach((nav) => {
      const active = nav.dataset.workspaceNav === key;
      nav.hidden = !active;
      nav.setAttribute("aria-hidden", String(!active));
    });
    const kicker = document.getElementById("active-workspace-kicker");
    if (kicker) kicker.textContent = `${meta.label} / ${meta.kicker}`;
  }

  function updateViewMeta(id) {
    const meta = WORKSPACES[workspaceForView.get(id)] || WORKSPACES.ops;
    const activeLabel = document.getElementById("active-view-label");
    if (activeLabel) activeLabel.textContent = viewLabel(id);
    const purpose = document.getElementById("active-view-purpose");
    if (purpose) purpose.textContent = VIEW_PURPOSES[id] || meta.purpose;
    const kicker = document.getElementById("active-workspace-kicker");
    if (kicker) kicker.textContent = `${meta.label} / ${meta.kicker}`;
  }

  function selectView(value, { updateHistory = true, userInitiated = false } = {}) {
    const id = normalizedView(value);
    const currentId = document.body.dataset.activeView;
    const currentCard = currentId ? document.getElementById(currentId) : null;
    // A repeated select (hashchange after pushState, poll-driven resize, etc.)
    // must not flip card visibility or dispatch window resize — that reflows the
    // page and reads as "auto jump".
    if (currentId === id && currentCard && !currentCard.hidden && !userInitiated) {
      onNavigate?.(id);
      return;
    }
    syncWorkspace(workspaceForView.get(id) || "ops");
    viewCards.forEach((card) => {
      const active = card.id === id;
      card.hidden = !active;
      card.classList.toggle("is-active-view", active);
    });
    navLinks.forEach((link, linkId) => {
      const active = linkId === id;
      link.classList.toggle("active", active);
      if (active) link.setAttribute("aria-current", "page");
      else link.removeAttribute("aria-current");
    });
    updateViewMeta(id);
    document.body.dataset.activeView = id;
    const skipLink = document.querySelector(".skip-link");
    if (skipLink) skipLink.href = `#${id}`;
    const workspace = WORKSPACES[workspaceForView.get(id)] || WORKSPACES.ops;
    document.title = `CML · ${workspace.label} · ${viewLabel(id)} · Flight Deck`;
    if (updateHistory && window.location.hash !== `#${id}`) {
      window.history.pushState(null, "", `#${id}`);
    }
    if (userInitiated && window.scrollY > 0) {
      window.scrollTo({
        top: 0,
        behavior: window.matchMedia?.("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth",
      });
    }
    const activeLink = navLinks.get(id);
    if (userInitiated && activeLink && window.innerWidth <= 1023 && !activeLink.closest("[hidden]")) {
      activeLink.scrollIntoView({ block: "nearest", inline: "center" });
    }
    onNavigate?.(id);
    requestAnimationFrame(() => window.dispatchEvent(new Event("resize")));
  }

  function setWorkspace(value, { selectDefault = true, updateHistory = true } = {}) {
    const key = WORKSPACES[value] ? value : "ops";
    syncWorkspace(key);
    try {
      window.localStorage?.setItem("cml-dashboard-workspace", key);
    } catch {
      // Storage can be unavailable in private or embedded browser contexts.
    }
    const current = normalizedView(document.body.dataset.activeView || window.location.hash);
    if (selectDefault && workspaceForView.get(current) !== key) {
      selectView(WORKSPACES[key].defaultView, { updateHistory });
    } else if (current) {
      updateViewMeta(current);
    }
  }

  navLinks.forEach((link, id) => {
    link.addEventListener("click", (event) => {
      if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
      event.preventDefault();
      selectView(id, { userInitiated: true });
    });
  });

  workspaceTabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      setWorkspace(tab.dataset.workspaceTab);
    });
  });

  window.addEventListener("popstate", () => selectView(window.location.hash, { updateHistory: false }));
  // Empty / unknown hash must NOT snap the UI to overview — that was an
  // "auto jump" when something cleared or rewrote location.hash.
  window.addEventListener("hashchange", () => {
    const raw = String(window.location.hash || "").replace(/^#/, "");
    if (!raw || !viewIds.has(raw)) return;
    selectView(raw, { updateHistory: false });
  });

  return {
    viewIds,
    selectView,
    setWorkspace,
    storedWorkspace,
    initialView() {
      const initialHash = String(window.location.hash || "").replace(/^#/, "");
      return viewIds.has(initialHash)
        ? initialHash
        : WORKSPACES[storedWorkspace()].defaultView;
    },
  };
}
