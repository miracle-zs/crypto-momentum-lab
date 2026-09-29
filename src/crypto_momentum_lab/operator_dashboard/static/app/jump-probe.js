/**
 * Lightweight jump diagnostics. Enable with localStorage cml-scroll-debug=1.
 * Logs scroll/view jumps so a browser console paste can identify the cause.
 */

const FLAG = "cml-scroll-debug";
const JUMP_PX = 40;

let lastY = typeof window !== "undefined" ? window.scrollY : 0;
let lastView = "";
let lastLoggedAt = 0;

function enabled() {
  try {
    return window.localStorage?.getItem(FLAG) === "1";
  } catch {
    return false;
  }
}

function record(kind, detail) {
  const entry = {
    t: new Date().toISOString(),
    kind,
    scrollY: Math.round(window.scrollY),
    view: document.body?.dataset?.activeView || "",
    hash: window.location.hash,
    ...detail,
  };
  if (typeof window !== "undefined") {
    window.__cmlJumpLog = window.__cmlJumpLog || [];
    window.__cmlJumpLog.push(entry);
    if (window.__cmlJumpLog.length > 200) window.__cmlJumpLog.shift();
    if (enabled()) {
      console.warn("[cml-jump]", entry);
    }
  }
  return entry;
}

export function installJumpProbe() {
  if (typeof window === "undefined") return;
  lastY = window.scrollY;
  lastView = document.body?.dataset?.activeView || "";

  window.addEventListener("scroll", () => {
    const y = window.scrollY;
    const delta = y - lastY;
    if (Math.abs(delta) >= JUMP_PX) {
      const now = Date.now();
      if (now - lastLoggedAt > 400) {
        lastLoggedAt = now;
        record("scroll", { from: Math.round(lastY), to: Math.round(y), delta: Math.round(delta) });
      }
    }
    lastY = y;
  }, { passive: true });

  setInterval(() => {
    const view = document.body?.dataset?.activeView || "";
    if (view && lastView && view !== lastView) {
      record("view", { from: lastView, to: view });
    }
    lastView = view;
    lastY = window.scrollY;
  }, 500);
}
