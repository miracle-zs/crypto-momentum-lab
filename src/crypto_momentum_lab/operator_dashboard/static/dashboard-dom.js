function fragmentFromHtml(ownerDocument, html) {
  const template = ownerDocument.createElement("template");
  template.innerHTML = html;
  return template.content;
}

let userInteractionVersion = 0;

if (typeof window !== "undefined") {
  const markUserInteraction = () => {
    userInteractionVersion += 1;
  };
  window.addEventListener("wheel", markUserInteraction, { passive: true });
  window.addEventListener("pointerdown", markUserInteraction, { passive: true });
  window.addEventListener("mousedown", markUserInteraction, { passive: true });
  window.addEventListener("touchstart", markUserInteraction, { passive: true });
  window.addEventListener("touchmove", markUserInteraction, { passive: true });
  window.addEventListener("click", markUserInteraction, { passive: true });
  window.addEventListener("keydown", (e) => {
    markUserInteraction();
  }, { passive: true });
}

function ownsFocus(root, doc, activeEl) {
  if (!activeEl) return false;
  if (root === doc || root === doc?.body || root === doc?.documentElement) return true;
  return typeof root?.contains === "function" && root.contains(activeEl);
}

function isFixedOrSticky(el, view) {
  let cur = el;
  while (cur && cur !== cur.ownerDocument?.body && cur !== cur.ownerDocument?.documentElement) {
    if (cur.classList?.contains("topbar") || cur.classList?.contains("sidebar")) return true;
    const style = view?.getComputedStyle?.(cur);
    if (style && (style.position === "sticky" || style.position === "fixed")) return true;
    cur = cur.parentElement;
  }
  return false;
}

function elementKey(node) {
  if (node?.nodeType !== 1) return null;
  for (const attribute of [
    "id",
    "data-state-key",
    "data-echart-id",
    "data-row-key",
    "data-live-account-label",
    "data-account-equity-range",
    "data-live-account-metrics-range",
  ]) {
    const value = node.getAttribute(attribute);
    if (value) return `${attribute}:${value}`;
  }
  return null;
}

function compatibleNode(left, right) {
  return left?.nodeType === right?.nodeType
    && (left?.nodeType !== 1 || left.tagName === right.tagName);
}

function isRuntimeMarker(attributeName) {
  return attributeName === "data-echart-mounted"
    || (attributeName.startsWith("data-") && attributeName.endsWith("-wired"));
}

function reconcileNode(current, next) {
  if (!compatibleNode(current, next)) {
    current.replaceWith(next);
    return next;
  }
  if (current.nodeType === 3) {
    if (current.nodeValue !== next.nodeValue) current.nodeValue = next.nodeValue;
    return current;
  }

  // ECharts owns the full live surface, including runtime attributes such as
  // its instance id and the generated SVG subtree. Reconcile only the keyed
  // shell around it, then refresh the existing instance from its new payload.
  if (current.matches?.(".echart-surface")) return current;

  const preserveDisclosureState = current.tagName === "DETAILS";
  for (const attribute of Array.from(current.attributes)) {
    if (preserveDisclosureState && attribute.name === "open") continue;
    if (isRuntimeMarker(attribute.name)) continue;
    if (!next.hasAttribute(attribute.name)) current.removeAttribute(attribute.name);
  }
  for (const attribute of Array.from(next.attributes)) {
    if (preserveDisclosureState && attribute.name === "open") continue;
    if (current.getAttribute(attribute.name) !== attribute.value) {
      current.setAttribute(attribute.name, attribute.value);
    }
  }

  reconcileChildren(current, next);
  return current;
}

function reconcileChildren(parent, templateParent) {
  const oldChildren = Array.from(parent.childNodes);
  const newChildren = Array.from(templateParent.childNodes);
  const oldByKey = new Map(
    oldChildren
      .map((node) => [elementKey(node), node])
      .filter(([key]) => key),
  );
  const used = new Set();

  newChildren.forEach((next, index) => {
    const key = elementKey(next);
    let current = key ? oldByKey.get(key) : oldChildren[index];
    if (current && (used.has(current) || (key && elementKey(current) !== key))) {
      current = null;
    }
    if (current && !key && elementKey(current)) current = null;
    if (current && !compatibleNode(current, next)) current = null;
    if (!current && !key) {
      current = oldChildren.find((candidate) => (
        !used.has(candidate)
        && !elementKey(candidate)
        && compatibleNode(candidate, next)
      )) || null;
    }

    if (!current) {
      current = next;
      parent.insertBefore(current, parent.childNodes[index] || null);
    } else {
      current = reconcileNode(current, next);
      const reference = parent.childNodes[index] || null;
      if (current !== reference) parent.insertBefore(current, reference);
    }
    used.add(current);
  });

  oldChildren.forEach((node) => {
    if (!used.has(node) && node.parentNode === parent) parent.removeChild(node);
  });
}

export function captureViewState(root) {
  const doc = root.ownerDocument || root;
  const view = doc?.defaultView;
  const scrollingElement = doc?.scrollingElement || doc?.documentElement || doc?.body;
  const pageX = view?.scrollX ?? scrollingElement?.scrollLeft ?? 0;
  const pageY = view?.scrollY ?? scrollingElement?.scrollTop ?? 0;

  // Track active focus identity only for user-editable text inputs.
  // Never capture or re-focus static buttons/cards on background polling,
  // which causes WebKit/Safari to scroll off-screen focused buttons back into view (jumping to top).
  const activeEl = doc?.activeElement;
  const isEditable = Boolean(
    activeEl && (
      activeEl.tagName === "INPUT" ||
      activeEl.tagName === "TEXTAREA" ||
      activeEl.isContentEditable
    )
  );
  const focusIdentity = (isEditable && ownsFocus(root, doc, activeEl)) ? {
    isEditable: true,
    tabId: activeEl.id || null,
    stateKey: activeEl.dataset?.stateKey || null,
  } : null;

  // Track reading anchor if supported
  let anchor = null;
  if (typeof doc?.elementFromPoint === "function" && view?.innerHeight) {
    const topbar = doc.querySelector?.(".topbar");
    const topbarBottom = topbar?.getBoundingClientRect?.().bottom || 120;
    const sampleY = Math.min((view.innerHeight || 800) - 50, Math.max(topbarBottom + 30, 180));
    const sampleX = Math.min(300, (view.innerWidth || 600) / 2);
    const candidate = doc.elementFromPoint(sampleX, sampleY);
    const anchorEl = candidate?.closest?.("[data-account-index], [data-live-account-label], tr[data-row-key], [data-state-key], .card, h2, h3");
    if (anchorEl && !isFixedOrSticky(anchorEl, view) && typeof anchorEl.getBoundingClientRect === "function") {
      const selector = anchorEl.id ? `#${anchorEl.id}` : null;
      const stateKey = anchorEl.dataset?.stateKey || null;
      const accountIndex = anchorEl.dataset?.accountIndex || null;
      const liveAccountLabel = anchorEl.dataset?.liveAccountLabel || null;
      const rowKey = anchorEl.dataset?.rowKey || null;
      if (selector || stateKey || accountIndex || liveAccountLabel || rowKey) {
        const rect = anchorEl.getBoundingClientRect();
        if (rect.width > 0 || rect.height > 0) {
          anchor = {
            selector,
            stateKey,
            accountIndex,
            liveAccountLabel,
            rowKey,
            topOffset: rect.top,
          };
        }
      }
    }
  }

  return {
    pageX,
    pageY,
    capturedAt: Date.now(),
    interactionVersion: userInteractionVersion,
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
  if (root?.isConnected === false) return;
  const doc = root.ownerDocument || root;
  const view = doc?.defaultView;
  if (state.interactionVersion != null && state.interactionVersion !== userInteractionVersion) {
    const scrollingElement = doc?.scrollingElement || doc?.documentElement || doc?.body;
    const currentY = view?.scrollY ?? scrollingElement?.scrollTop ?? 0;
    if (currentY !== 0 || state.pageY <= 0) return;
  }

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

  // If this root is currently inside a hidden container (e.g. background polling of an inactive tab),
  // never alter window-level scroll!
  const isHiddenRoot = Boolean(
    root.closest?.("[hidden]") ||
    (typeof root.offsetParent !== "undefined" && root.offsetParent === null && root !== doc?.body && root !== doc?.documentElement)
  );
  if (isHiddenRoot) return;

  // 3. Compensate from the current position so independent roots can release
  // their height locks in the same frame without replaying one stale scrollY.
  const scrollingElement = doc?.scrollingElement || doc?.documentElement || doc?.body;
  const currentY = view?.scrollY ?? scrollingElement?.scrollTop ?? 0;
  let targetY = view?.scrollY ?? scrollingElement?.scrollTop ?? state.pageY;
  if (targetY === 0 && state.pageY > 0) {
    targetY = state.pageY;
  }
  let resolvedAnchor = false;

  if (state.anchor) {
    let targetAnchor = null;
    if (state.anchor.stateKey) targetAnchor = doc.querySelector?.(`[data-state-key="${state.anchor.stateKey}"]`);
    else if (state.anchor.accountIndex) targetAnchor = doc.querySelector?.(`[data-account-index="${state.anchor.accountIndex}"]`);
    else if (state.anchor.liveAccountLabel) targetAnchor = doc.querySelector?.(`[data-live-account-label="${state.anchor.liveAccountLabel}"]`);
    else if (state.anchor.rowKey) targetAnchor = doc.querySelector?.(`[data-row-key="${state.anchor.rowKey}"]`);
    else if (state.anchor.selector) targetAnchor = doc.querySelector?.(state.anchor.selector);

    if (targetAnchor && !isFixedOrSticky(targetAnchor, view) && typeof targetAnchor.getBoundingClientRect === "function") {
      const currentRect = targetAnchor.getBoundingClientRect();
      const isVisible = (currentRect.width > 0 || currentRect.height > 0) &&
        (targetAnchor.offsetParent !== null || targetAnchor === doc?.body || targetAnchor === doc?.documentElement);
      if (isVisible) {
        const diff = currentRect.top - state.anchor.topOffset;
        const candidateY = Math.max(0, currentY + (Math.abs(diff) > 2 ? diff : 0));
        // Defend against anchor calculation collapsing scroll to top
        if (state.pageY > 20 && candidateY <= 20) {
          targetY = state.pageY;
        } else if (state.pageY > 100 && candidateY < state.pageY * 0.5) {
          targetY = state.pageY;
        } else if (currentY === 0 && state.pageY > 0) {
          targetY = Math.max(0, state.pageY + (Math.abs(diff) > 2 ? diff : 0));
        } else {
          targetY = candidateY;
        }
        resolvedAnchor = true;
      }
    }
  }

  let applyVerticalScroll = resolvedAnchor;
  if (!resolvedAnchor && state.layoutRelease?.rootAboveAnchorPoint
      && typeof root?.getBoundingClientRect === "function") {
    const currentHeight = root.getBoundingClientRect().height;
    const ownHeightDelta = currentHeight - state.layoutRelease.rootHeight;
    const nativeScrollDelta = currentY - state.layoutRelease.pageY;
    const remainingDelta = ownHeightDelta - nativeScrollDelta;
    if (Math.abs(remainingDelta) > 2) {
      targetY = Math.max(0, currentY + remainingDelta);
      applyVerticalScroll = true;
    }
  }

  // Fallback: If no anchor survived and the browser collapsed scroll to top (currentY === 0)
  // while the captured scroll position was non-zero, restore state.pageY to prevent jumping to top.
  if (!applyVerticalScroll && currentY === 0 && state.pageY > 0) {
    targetY = state.pageY;
    applyVerticalScroll = true;
  }

  // Absolute defense: If the user was scrolled down (state.pageY > 20),
  // targetY MUST NEVER collapse to 0 or near 0 on a background poll!
  if (state.pageY > 20 && targetY <= 20) {
    targetY = state.pageY;
    applyVerticalScroll = true;
  }

  if (applyVerticalScroll && scrollingElement) {
    const maxScroll = Math.max(0, (scrollingElement.scrollHeight || 0) - (view?.innerHeight || 0));
    if (maxScroll > 0) {
      targetY = Math.min(targetY, maxScroll);
    }
  }

  if (applyVerticalScroll) {
    const previousBehavior = doc?.documentElement?.style?.scrollBehavior;
    if (doc?.documentElement?.style) doc.documentElement.style.scrollBehavior = "auto";
    if (view && typeof view.scrollTo === "function") {
      try {
        view.scrollTo({ left: state.pageX, top: targetY, behavior: "instant" });
      } catch {
        view.scrollTo(state.pageX, targetY);
      }
    }
    if (scrollingElement) {
      if (scrollingElement.scrollTop !== targetY) scrollingElement.scrollTop = targetY;
      if (scrollingElement.scrollLeft !== state.pageX) scrollingElement.scrollLeft = state.pageX;
    }
    if (doc?.documentElement?.style) doc.documentElement.style.scrollBehavior = previousBehavior;
  } else if (scrollingElement && scrollingElement.scrollLeft !== state.pageX) {
    // With no surviving semantic anchor, leave vertical scroll to the browser's
    // native scroll anchoring. A no-op window.scrollTo here suppresses its
    // compensation for later roots changing height in the same frame.
    scrollingElement.scrollLeft = state.pageX;
  }

  // 4. Restore focus ONLY if the active element was an editable text input.
  if (state.focusIdentity && state.focusIdentity.isEditable) {
    let focusTarget = null;
    if (state.focusIdentity.stateKey) {
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
  updateChildrenFromHtml(root, html, false);
}

export function patchChildrenFromHtml(root, html) {
  updateChildrenFromHtml(root, html, true);
}

function updateChildrenFromHtml(root, html, patch) {
  const generation = ((root.__renderGeneration || 0) + 1);
  root.__renderGeneration = generation;
  const state = captureViewState(root);
  const content = fragmentFromHtml(root.ownerDocument, html);

  if (patch) {
    reconcileChildren(root, content);
    restoreViewState(root, state);
    return;
  }

  const previousMinHeight = root.__previousMinHeight !== undefined ? root.__previousMinHeight : root.style.minHeight;
  root.__previousMinHeight = previousMinHeight;
  if (root.offsetHeight > 0) {
    root.style.minHeight = `${root.offsetHeight}px`;
  }
  root.replaceChildren(content);
  void root.offsetHeight;
  restoreViewState(root, state);
  const view = root.ownerDocument?.defaultView;
  if (typeof view?.requestAnimationFrame === "function") {
    view.requestAnimationFrame(() => {
      if (root.__renderGeneration !== generation) return;
      const beforeReleaseRect = root.getBoundingClientRect?.();
      const layoutRelease = beforeReleaseRect ? {
        pageY: view.scrollY ?? root.ownerDocument?.scrollingElement?.scrollTop ?? state.pageY,
        rootHeight: beforeReleaseRect.height,
        rootAboveAnchorPoint: beforeReleaseRect.bottom <= 100,
      } : null;
      root.style.minHeight = previousMinHeight;
      delete root.__previousMinHeight;
      void root.offsetHeight;
      restoreViewState(root, layoutRelease ? { ...state, layoutRelease } : state);
    });
  } else {
    root.style.minHeight = previousMinHeight;
    delete root.__previousMinHeight;
  }
}

export function replaceElementFromHtml(element, html) {
  const doc = element.ownerDocument;
  const root = doc?.body || doc?.documentElement || element;
  const state = captureViewState(root);
  element.replaceWith(fragmentFromHtml(doc, html));
  restoreViewState(root, state);
}
