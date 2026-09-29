/**
 * Window scroll intent and jump repair.
 *
 * Invariants:
 * 1. Do not snap back for a modest drop (user scroll-up / native anchoring).
 * 2. Repair collapse to top (currentY <= 20) and large height-collapse clamps
 *    (pageY large, currentY near the new maxScroll after content shrank).
 * 3. A live wheel/touch veto only applies to modest moves — a 2000px jump to
 *    maxScroll cannot be intentional user scrolling.
 */

let userInteractionVersion = 0;
let lastScrollIntentAt = 0;
const SCROLL_INTENT_WINDOW_MS = 350;
const HEIGHT_COLLAPSE_MIN_PAGE_Y = 200;
const HEIGHT_COLLAPSE_RATIO = 0.25;

function markScrollIntent() {
  lastScrollIntentAt = Date.now();
  userInteractionVersion += 1;
}

function markUserInteraction() {
  userInteractionVersion += 1;
}

if (typeof window !== "undefined") {
  window.addEventListener("wheel", markScrollIntent, { passive: true });
  window.addEventListener("touchmove", markScrollIntent, { passive: true });
  window.addEventListener("pointerdown", markUserInteraction, { passive: true });
  window.addEventListener("mousedown", markUserInteraction, { passive: true });
  window.addEventListener("touchstart", markUserInteraction, { passive: true });
  window.addEventListener("click", markUserInteraction, { passive: true });
  window.addEventListener("keydown", markUserInteraction, { passive: true });
}

export function getUserInteractionVersion() {
  return userInteractionVersion;
}

export function isUserScrolling(now = Date.now()) {
  return now - lastScrollIntentAt < SCROLL_INTENT_WINDOW_MS;
}

function applyScrollTop(view, doc, scrollingElement, pageX, targetY) {
  const previousBehavior = doc?.documentElement?.style?.scrollBehavior;
  if (doc?.documentElement?.style) doc.documentElement.style.scrollBehavior = "auto";
  try {
    view.scrollTo({ left: pageX, top: targetY, behavior: "instant" });
  } catch {
    view.scrollTo(pageX, targetY);
  }
  if (scrollingElement) {
    if (scrollingElement.scrollTop !== targetY) scrollingElement.scrollTop = targetY;
    if (scrollingElement.scrollLeft !== pageX) scrollingElement.scrollLeft = pageX;
  }
  if (doc?.documentElement?.style) doc.documentElement.style.scrollBehavior = previousBehavior;
}

/**
 * Guard window scroll across a batch of DOM mutations (background poll).
 */
export function createScrollGuard() {
  const view = typeof window !== "undefined" ? window : null;
  const doc = view?.document;
  const scrollingElement = doc?.scrollingElement || doc?.documentElement || doc?.body;
  const pageX = view?.scrollX ?? scrollingElement?.scrollLeft ?? 0;
  const pageY = view?.scrollY ?? scrollingElement?.scrollTop ?? 0;
  const pageScrollHeight = scrollingElement?.scrollHeight || 0;
  return {
    pageX,
    pageY,
    pageScrollHeight,
    restore({ force = false } = {}) {
      if (!view) return pageY;
      const currentY = view.scrollY ?? scrollingElement?.scrollTop ?? 0;
      const currentHeight = scrollingElement?.scrollHeight || 0;
      const maxScroll = Math.max(0, currentHeight - (view.innerHeight || 0));
      const collapsedToTop = pageY > 20 && currentY <= 20;
      // Content shrank and the browser clamped us to the new bottom
      // (field log: 3331→153, 2474→153 — same maxScroll).
      const clampedByHeightCollapse = (
        pageY >= HEIGHT_COLLAPSE_MIN_PAGE_Y
        && currentY < pageY * HEIGHT_COLLAPSE_RATIO
        && currentY <= maxScroll + 2
      );
      const largeDrop = pageY - currentY > 500;
      const gestureVeto = !force && isUserScrolling() && !largeDrop && !clampedByHeightCollapse;
      if (gestureVeto) return currentY;
      if (!force && !collapsedToTop && !clampedByHeightCollapse) return currentY;

      // Pin document height so pageY is reachable even if chart slots are
      // briefly empty; release after the next frame(s).
      const pinHeight = Math.max(pageScrollHeight, currentHeight);
      const body = doc?.body;
      let pinned = false;
      if ((collapsedToTop || clampedByHeightCollapse) && body?.style && pinHeight > 0) {
        body.style.minHeight = `${pinHeight}px`;
        pinned = true;
      }
      const targetY = pinHeight > 0 ? Math.min(pageY, Math.max(0, pinHeight - (view.innerHeight || 0))) : pageY;
      applyScrollTop(view, doc, scrollingElement, pageX, force ? pageY : targetY);
      if (pinned) {
        const release = () => {
          if (body?.style?.minHeight) {
            body.style.minHeight = "";
          }
        };
        if (typeof view.requestAnimationFrame === "function") {
          view.requestAnimationFrame(() => {
            view.requestAnimationFrame(release);
          });
        } else {
          setTimeout(release, 50);
        }
      }
      return force ? pageY : targetY;
    },
  };
}
