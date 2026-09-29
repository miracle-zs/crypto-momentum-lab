/**
 * DOM facade used by sections and the poller. Implementation is in core/.
 */

export {
  createScrollGuard,
  isUserScrolling,
} from "./core/scroll-keep.js";

export {
  captureViewState,
  restoreViewState,
} from "./core/view-state.js";

export {
  patchChildrenFromHtml,
  replaceChildrenFromHtml,
  replaceElementFromHtml,
} from "./core/dom-update.js";
