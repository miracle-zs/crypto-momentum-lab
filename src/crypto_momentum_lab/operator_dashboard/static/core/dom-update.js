/**
 * HTML replace / patch orchestration: reconcile + view-state + scroll guard.
 * Scroll policy is delegated to core/scroll-keep.js (collapse-to-top only).
 */

import { fragmentFromHtml, reconcileChildren } from "./dom-reconcile.js";
import { captureViewState, restoreViewState } from "./view-state.js";
import { createScrollGuard } from "./scroll-keep.js";

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
  // Batch-level lock: only repairs a collapse to the top after DOM writes.
  // Partial offset changes are left to restoreViewState anchors and native
  // scroll anchoring so the page never "jumps forward" through content.
  const scrollGuard = createScrollGuard();

  if (patch) {
    const previousMinHeight = root.style.minHeight;
    if (root.offsetHeight > 0) {
      root.style.minHeight = `${root.offsetHeight}px`;
    }
    reconcileChildren(root, content);
    restoreViewState(root, state);
    scrollGuard.restore();
    const view = root.ownerDocument?.defaultView;
    if (typeof view?.requestAnimationFrame === "function") {
      view.requestAnimationFrame(() => {
        if (root.__renderGeneration !== generation) return;
        root.style.minHeight = previousMinHeight;
        void root.offsetHeight;
        restoreViewState(root, state);
        scrollGuard.restore();
        setTimeout(() => {
          if (root.__renderGeneration !== generation) return;
          restoreViewState(root, state);
          scrollGuard.restore();
        }, 50);
      });
    } else {
      root.style.minHeight = previousMinHeight;
    }
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
  scrollGuard.restore();
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
      scrollGuard.restore();
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
  const scrollGuard = createScrollGuard();
  element.replaceWith(fragmentFromHtml(doc, html));
  restoreViewState(root, state);
  scrollGuard.restore();
}
