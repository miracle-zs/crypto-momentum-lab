/**
 * Compatibility facade. Prefer importing from core/scroll-keep.js,
 * core/dom-reconcile.js, core/view-state.js, or core/dom-update.js directly.
 */

export {
  createScrollGuard,
  getUserInteractionVersion,
  isUserScrolling,
} from "./core/scroll-keep.js";

export {
  captureViewState,
  restoreViewState,
} from "./core/view-state.js";

export {
  fragmentFromHtml,
  reconcileChildren,
} from "./core/dom-reconcile.js";

export {
  patchChildrenFromHtml,
  replaceChildrenFromHtml,
  replaceElementFromHtml,
} from "./core/dom-update.js";
