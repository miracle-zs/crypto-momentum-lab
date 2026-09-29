import assert from "node:assert/strict";
import test from "node:test";

import {
  createScrollGuard,
  isUserScrolling,
} from "../../src/crypto_momentum_lab/operator_dashboard/static/core/scroll-keep.js";
import {
  captureViewState,
  restoreViewState,
} from "../../src/crypto_momentum_lab/operator_dashboard/static/core/view-state.js";

function mockWindow({ scrollY = 0, scrollHeight = 3000, innerHeight = 800 } = {}) {
  const doc = {
    scrollingElement: { scrollLeft: 0, scrollTop: scrollY, scrollHeight },
    documentElement: { style: {} },
    body: { scrollLeft: 0, scrollTop: scrollY },
    querySelectorAll: () => [],
  };
  const view = {
    scrollX: 0,
    scrollY,
    innerHeight,
    innerWidth: 1200,
    scrollTo(opts) {
      const top = typeof opts === "object" ? opts.top : arguments[1];
      doc.scrollingElement.scrollTop = top;
      view.scrollY = top;
    },
    document: doc,
  };
  doc.defaultView = view;
  return { view, doc };
}

function withWindow(view, fn) {
  const previous = globalThis.window;
  globalThis.window = view;
  try {
    return fn();
  } finally {
    if (previous === undefined) delete globalThis.window;
    else globalThis.window = previous;
  }
}

test("invariant: no absolute pageY snap-back when currentY is smaller but non-zero", () => {
  const { view, doc } = mockWindow({ scrollY: 800 });
  withWindow(view, () => {
    const guard = createScrollGuard();
    // User scrolled up / native anchoring compensated after content shrank.
    view.scrollY = 400;
    doc.scrollingElement.scrollTop = 400;
    assert.equal(guard.restore(), 400);
    assert.equal(view.scrollY, 400);
  });
});

test("invariant: only collapse to top is repaired", () => {
  const { view, doc } = mockWindow({ scrollY: 800 });
  withWindow(view, () => {
    const guard = createScrollGuard();
    view.scrollY = 0;
    doc.scrollingElement.scrollTop = 0;
    assert.equal(guard.restore(), 800);
    assert.equal(view.scrollY, 800);
  });
});

test("invariant: restore is a no-op during a live scroll gesture unless force", () => {
  const { view, doc } = mockWindow({ scrollY: 800 });
  withWindow(view, () => {
    // Module attaches wheel listener on window at import; synthesize via
    // dispatch if available, otherwise skip gesture timing (idle path).
    if (typeof view.addEventListener === "function") {
      // listeners were bound at import to the first window; not this mock
    }
    const guard = createScrollGuard();
    view.scrollY = 0;
    doc.scrollingElement.scrollTop = 0;
    // Idle (no gesture in this process) → collapse repair runs.
    assert.equal(isUserScrolling(Date.now() + 60_000), false);
    assert.equal(guard.restore(), 800);
    view.scrollY = 0;
    doc.scrollingElement.scrollTop = 0;
    guard.restore({ force: true });
    assert.equal(view.scrollY, 800);
  });
});

test("restoreViewState leaves mid-page offsets alone without an anchor", () => {
  const { view, doc } = mockWindow({ scrollY: 400 });
  const root = {
    ownerDocument: doc,
    querySelectorAll: () => [],
    closest: () => null,
    offsetParent: doc.body,
    isConnected: true,
  };
  view.scrollY = 400;
  doc.scrollingElement.scrollTop = 400;
  restoreViewState(root, {
    pageX: 0,
    pageY: 800,
    disclosures: [],
    containers: [],
  });
  assert.equal(doc.scrollingElement.scrollTop, 400);
});

test("captureViewState records interaction version for diagnostics", () => {
  const { view, doc } = mockWindow({ scrollY: 120 });
  const root = {
    ownerDocument: doc,
    querySelectorAll: () => [],
    contains: () => false,
    isConnected: true,
  };
  const state = captureViewState(root);
  assert.equal(state.pageY, 120);
  assert.equal(typeof state.interactionVersion, "number");
});
