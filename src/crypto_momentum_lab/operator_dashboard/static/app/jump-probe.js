/**
 * Jump diagnostics. Always records to window.__cmlJumpLog.
 * console.warn only for AUTO jumps (not while the user is scrolling).
 * Force verbose: localStorage cml-scroll-debug=1
 * Dump: copy(JSON.stringify(window.__cmlJumpLog.slice(-30), null, 2))
 */

import { isUserScrolling } from "../core/scroll-keep.js";

const FLAG = "cml-scroll-debug";
const JUMP_PX = 30;

let lastY = typeof window !== "undefined" ? window.scrollY : 0;
let lastView = "";
let lastLoggedAt = 0;

function verbose() {
  try {
    return window.localStorage?.getItem(FLAG) === "1";
  } catch {
    return false;
  }
}

function record(kind, detail) {
  const userScroll = isUserScrolling();
  const entry = {
    t: new Date().toISOString(),
    kind,
    userScroll,
    scrollY: Math.round(window.scrollY),
    view: document.body?.dataset?.activeView || "",
    hash: window.location.hash,
    ...detail,
  };
  window.__cmlJumpLog = window.__cmlJumpLog || [];
  window.__cmlJumpLog.push(entry);
  if (window.__cmlJumpLog.length > 200) window.__cmlJumpLog.shift();
  // Auto jumps (no live wheel/touch) are the ones we care about.
  if (!userScroll || verbose()) {
    console.warn("[cml-jump]", entry);
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
      if (now - lastLoggedAt > 300) {
        lastLoggedAt = now;
        record("scroll", {
          from: Math.round(lastY),
          to: Math.round(y),
          delta: Math.round(delta),
        });
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
  }, 400);
}
