/**
 * Window scroll intent and collapse-to-top repair.
 *
 * Invariants (also asserted in tests/frontend/dashboard-modules.test.mjs):
 * 1. Never scrollTo(pageY) merely because currentY < pageY and currentY > 20.
 *    That fights user scroll-up and native scroll anchoring ("page jumps forward").
 * 2. Only repair a true collapse: pageY > 20 && currentY <= 20.
 * 3. While a real scroll gesture is in flight, restore() is a no-op unless force.
 */

let userInteractionVersion = 0;
let lastScrollIntentAt = 0;
const SCROLL_INTENT_WINDOW_MS = 350;

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

/**
 * Guard window scroll across a batch of DOM mutations (background poll).
 * Only repairs a collapse to the top — see module invariants above.
 */
export function createScrollGuard() {
  const view = typeof window !== "undefined" ? window : null;
  const doc = view?.document;
  const scrollingElement = doc?.scrollingElement || doc?.documentElement || doc?.body;
  const pageX = view?.scrollX ?? scrollingElement?.scrollLeft ?? 0;
  const pageY = view?.scrollY ?? scrollingElement?.scrollTop ?? 0;
  return {
    pageX,
    pageY,
    restore({ force = false } = {}) {
      if (!view) return pageY;
      if (!force && isUserScrolling()) return view.scrollY ?? 0;
      const currentY = view.scrollY ?? scrollingElement?.scrollTop ?? 0;
      const collapsed = pageY > 20 && currentY <= 20;
      if (!force && !collapsed) return currentY;
      const maxScroll = Math.max(
        0,
        (scrollingElement?.scrollHeight || 0) - (view.innerHeight || 0),
      );
      const targetY = maxScroll > 0 ? Math.min(pageY, maxScroll) : pageY;
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
      return targetY;
    },
  };
}
