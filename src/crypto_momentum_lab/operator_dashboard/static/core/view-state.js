/**
 * Capture / restore reading position, disclosures, table scroll, and
 * editable-input focus across a DOM mutation. Window-scroll policy for
 * collapse-to-top lives in core/scroll-keep.js — do not add absolute pageY
 * snap-back here either (it causes "jump forward" when content above shrinks).
 */

import {
  getUserInteractionVersion,
  isUserScrolling,
} from "./scroll-keep.js";

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

export function captureViewState(root) {
  const doc = root.ownerDocument || root;
  const view = doc?.defaultView;
  const scrollingElement = doc?.scrollingElement || doc?.documentElement || doc?.body;
  const pageX = view?.scrollX ?? scrollingElement?.scrollLeft ?? 0;
  const pageY = view?.scrollY ?? scrollingElement?.scrollTop ?? 0;

  // Track active focus identity only for user-editable text inputs.
  // Never capture or re-focus static buttons/cards on background polling,
  // which causes WebKit/Safari to scroll off-screen focused buttons back into view.
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
    interactionVersion: getUserInteractionVersion(),
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
  // Only a live scroll gesture may veto restoration. A prior click/keydown
  // must not: that is exactly when background polls were jumping the page.
  if (isUserScrolling()) return;

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

  // Hidden section roots must never move window scroll.
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

  // Collapse-to-top only: never force a mid-page absolute pageY.
  if (!applyVerticalScroll && currentY === 0 && state.pageY > 0) {
    targetY = state.pageY;
    applyVerticalScroll = true;
  }
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
    // Leave vertical scroll to native anchoring; only sync horizontal.
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
