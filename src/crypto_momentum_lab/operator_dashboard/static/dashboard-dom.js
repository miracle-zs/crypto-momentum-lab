function fragmentFromHtml(ownerDocument, html) {
  const template = ownerDocument.createElement("template");
  template.innerHTML = html;
  return template.content;
}

let lastUserInteractionTime = 0;
let currentRenderGeneration = 0;

if (typeof window !== "undefined") {
  const markUserInteraction = () => {
    lastUserInteractionTime = Date.now();
  };
  window.addEventListener("wheel", markUserInteraction, { passive: true });
  window.addEventListener("touchmove", markUserInteraction, { passive: true });
  window.addEventListener("keydown", (e) => {
    if (["ArrowUp", "ArrowDown", "PageUp", "PageDown", "Home", "End", " "].includes(e.key)) {
      markUserInteraction();
    }
  }, { passive: true });
}

export function captureViewState(root) {
  const doc = root.ownerDocument || root;
  const view = doc?.defaultView;
  const scrollingElement = doc?.scrollingElement || doc?.documentElement || doc?.body;
  const pageX = view?.scrollX ?? scrollingElement?.scrollLeft ?? 0;
  const pageY = view?.scrollY ?? scrollingElement?.scrollTop ?? 0;

  // Track active focus identity
  const activeEl = doc?.activeElement;
  const focusIdentity = activeEl ? {
    accountIndex: activeEl.dataset?.accountIndex || null,
    liveAccountLabel: activeEl.dataset?.liveAccountLabel || null,
    tabId: activeEl.id || null,
    stateKey: activeEl.dataset?.stateKey || null,
  } : null;

  // Track reading anchor if supported
  let anchor = null;
  if (typeof doc?.elementFromPoint === "function" && view?.innerHeight) {
    const candidate = doc.elementFromPoint(Math.min(200, (view.innerWidth || 400) / 2), 100);
    const anchorEl = candidate?.closest?.("[data-account-index], [data-live-account-label], tr[data-row-key], [data-state-key], .card, h2, h3");
    if (anchorEl && typeof anchorEl.getBoundingClientRect === "function") {
      const rect = anchorEl.getBoundingClientRect();
      anchor = {
        selector: anchorEl.id ? `#${anchorEl.id}` : null,
        stateKey: anchorEl.dataset?.stateKey || null,
        accountIndex: anchorEl.dataset?.accountIndex || null,
        liveAccountLabel: anchorEl.dataset?.liveAccountLabel || null,
        rowKey: anchorEl.dataset?.rowKey || null,
        topOffset: rect.top,
      };
    }
  }

  return {
    pageX,
    pageY,
    capturedAt: Date.now(),
    focusIdentity,
    anchor,
    containers: Array.from(root.querySelectorAll ? root.querySelectorAll(".table-scroll") : []).map((container) => ({
      key: container.dataset?.stateKey || null,
      left: container.scrollLeft,
      top: container.scrollTop,
    })),
    disclosures: Array.from(root.querySelectorAll ? root.querySelectorAll("details") : []).map((details) => ({
      key: details.dataset?.stateKey || null,
      open: details.open,
    })),
  };
}

export function restoreViewState(root, state) {
  if (!state) return;
  const doc = root.ownerDocument || root;
  const view = doc?.defaultView;

  // 1. Restore disclosures
  const disclosureStates = new Map(
    (state.disclosures || []).filter((saved) => saved.key).map((saved) => [saved.key, saved]),
  );
  (root.querySelectorAll ? root.querySelectorAll("details") : []).forEach((details, index) => {
    const saved = details.dataset?.stateKey
      ? disclosureStates.get(details.dataset.stateKey)
      : (state.disclosures || [])[index];
    if (saved) details.open = saved.open;
  });

  // 2. Restore horizontal/vertical container scrolls
  const containerStates = new Map(
    (state.containers || []).filter((saved) => saved.key).map((saved) => [saved.key, saved]),
  );
  (root.querySelectorAll ? root.querySelectorAll(".table-scroll") : []).forEach((container, index) => {
    const saved = container.dataset?.stateKey
      ? containerStates.get(container.dataset.stateKey)
      : (state.containers || [])[index];
    if (!saved) return;
    container.scrollLeft = saved.left;
    container.scrollTop = saved.top;
  });

  // 3. Check if user scrolled since snapshot
  const userInteracted = state.capturedAt && state.capturedAt < lastUserInteractionTime;
  if (!userInteracted) {
    const scrollingElement = doc?.scrollingElement || doc?.documentElement || doc?.body;
    let targetY = state.pageY;

    // Check reading anchor compensation
    if (state.anchor) {
      let targetAnchor = null;
      if (state.anchor.stateKey) targetAnchor = doc.querySelector?.(`[data-state-key="${state.anchor.stateKey}"]`);
      else if (state.anchor.accountIndex) targetAnchor = doc.querySelector?.(`[data-account-index="${state.anchor.accountIndex}"]`);
      else if (state.anchor.liveAccountLabel) targetAnchor = doc.querySelector?.(`[data-live-account-label="${state.anchor.liveAccountLabel}"]`);
      else if (state.anchor.rowKey) targetAnchor = doc.querySelector?.(`[data-row-key="${state.anchor.rowKey}"]`);
      else if (state.anchor.selector) targetAnchor = doc.querySelector?.(state.anchor.selector);

      if (targetAnchor && typeof targetAnchor.getBoundingClientRect === "function") {
        const currentRect = targetAnchor.getBoundingClientRect();
        const diff = currentRect.top - state.anchor.topOffset;
        if (Math.abs(diff) > 2) {
          targetY = Math.max(0, state.pageY + diff);
        }
      }
    }

    if (scrollingElement) {
      const maxScroll = Math.max(0, (scrollingElement.scrollHeight || 0) - (view?.innerHeight || 0));
      targetY = Math.min(targetY, maxScroll);
    }

    const previousBehavior = doc?.documentElement?.style?.scrollBehavior;
    if (doc?.documentElement?.style) doc.documentElement.style.scrollBehavior = "auto";

    if (view && typeof view.scrollTo === "function") {
      try {
        view.scrollTo({ left: state.pageX, top: targetY, behavior: "instant" });
      } catch {
        view.scrollTo(state.pageX, state.pageY);
      }
    }
    if (scrollingElement) {
      if (scrollingElement.scrollTop !== targetY) scrollingElement.scrollTop = targetY;
      if (scrollingElement.scrollLeft !== state.pageX) scrollingElement.scrollLeft = state.pageX;
    }
    if (doc?.documentElement?.style) doc.documentElement.style.scrollBehavior = previousBehavior;
  }

  // 4. Restore active focus if applicable, ALWAYS with preventScroll: true
  if (state.focusIdentity) {
    let focusTarget = null;
    if (state.focusIdentity.accountIndex) {
      focusTarget = root.querySelector?.(`[data-account-index="${state.focusIdentity.accountIndex}"]`);
    } else if (state.focusIdentity.liveAccountLabel) {
      focusTarget = root.querySelector?.(`[data-live-account-label="${state.focusIdentity.liveAccountLabel}"]`);
    } else if (state.focusIdentity.stateKey) {
      focusTarget = root.querySelector?.(`[data-state-key="${state.focusIdentity.stateKey}"]`);
    } else if (state.focusIdentity.tabId) {
      focusTarget = root.querySelector?.(`#${state.focusIdentity.tabId}`);
    }
    if (focusTarget && typeof focusTarget.focus === "function") {
      focusTarget.focus({ preventScroll: true });
    }
  }
}

export function replaceChildrenFromHtml(root, html) {
  const generation = ++currentRenderGeneration;
  const state = captureViewState(root);
  const previousMinHeight = root.style.minHeight;
  if (root.offsetHeight > 0) {
    root.style.minHeight = `${root.offsetHeight}px`;
  }
  root.replaceChildren(fragmentFromHtml(root.ownerDocument, html));
  void root.offsetHeight;
  restoreViewState(root, state);
  const view = root.ownerDocument?.defaultView;
  if (typeof view?.requestAnimationFrame === "function") {
    view.requestAnimationFrame(() => {
      if (generation !== currentRenderGeneration) return;
      root.style.minHeight = previousMinHeight;
      restoreViewState(root, state);
    });
  } else {
    root.style.minHeight = previousMinHeight;
  }
}

export function replaceElementFromHtml(element, html) {
  const doc = element.ownerDocument;
  const root = doc?.body || doc?.documentElement || element;
  const state = captureViewState(root);
  element.replaceWith(fragmentFromHtml(doc, html));
  restoreViewState(root, state);
}
