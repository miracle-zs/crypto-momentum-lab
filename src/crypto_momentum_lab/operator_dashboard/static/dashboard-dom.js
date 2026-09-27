function fragmentFromHtml(ownerDocument, html) {
  const template = ownerDocument.createElement("template");
  template.innerHTML = html;
  return template.content;
}

export function captureViewState(root) {
  const doc = root.ownerDocument;
  const view = doc?.defaultView;
  const pageX = view?.scrollX ?? doc?.documentElement?.scrollLeft ?? doc?.body?.scrollLeft ?? 0;
  const pageY = view?.scrollY ?? doc?.documentElement?.scrollTop ?? doc?.body?.scrollTop ?? 0;
  return {
    pageX,
    pageY,
    containers: Array.from(root.querySelectorAll(".table-scroll")).map((container) => ({
      key: container.dataset.stateKey || null,
      left: container.scrollLeft,
      top: container.scrollTop,
    })),
    disclosures: Array.from(root.querySelectorAll("details")).map((details) => ({
      key: details.dataset.stateKey || null,
      open: details.open,
    })),
  };
}

export function restoreViewState(root, state) {
  if (!state) return;
  const doc = root.ownerDocument;
  const view = doc?.defaultView;
  const disclosureStates = new Map(
    state.disclosures.filter((saved) => saved.key).map((saved) => [saved.key, saved]),
  );
  root.querySelectorAll("details").forEach((details, index) => {
    const saved = details.dataset.stateKey
      ? disclosureStates.get(details.dataset.stateKey)
      : state.disclosures[index];
    if (saved) details.open = saved.open;
  });

  const containerStates = new Map(
    state.containers.filter((saved) => saved.key).map((saved) => [saved.key, saved]),
  );
  root.querySelectorAll(".table-scroll").forEach((container, index) => {
    const saved = container.dataset.stateKey
      ? containerStates.get(container.dataset.stateKey)
      : state.containers[index];
    if (!saved) return;
    container.scrollLeft = saved.left;
    container.scrollTop = saved.top;
  });

  const documentElement = doc?.documentElement;
  const body = doc?.body;
  const previousBehavior = documentElement?.style?.scrollBehavior;
  if (documentElement?.style) documentElement.style.scrollBehavior = "auto";
  if (view && typeof view.scrollTo === "function") {
    try {
      view.scrollTo({ left: state.pageX, top: state.pageY, behavior: "instant" });
    } catch {
      view.scrollTo(state.pageX, state.pageY);
    }
  }
  if (documentElement && (documentElement.scrollTop !== state.pageY || documentElement.scrollLeft !== state.pageX)) {
    documentElement.scrollTop = state.pageY;
    documentElement.scrollLeft = state.pageX;
  }
  if (body && (body.scrollTop !== state.pageY || body.scrollLeft !== state.pageX)) {
    body.scrollTop = state.pageY;
    body.scrollLeft = state.pageX;
  }
  if (documentElement?.style) documentElement.style.scrollBehavior = previousBehavior;
}

export function replaceChildrenFromHtml(root, html) {
  const state = captureViewState(root);
  const previousMinHeight = root.style.minHeight;
  if (root.offsetHeight > 0) {
    root.style.minHeight = `${root.offsetHeight}px`;
  }
  root.replaceChildren(fragmentFromHtml(root.ownerDocument, html));
  void root.offsetHeight;
  restoreViewState(root, state);
  const view = root.ownerDocument.defaultView;
  if (typeof view?.requestAnimationFrame === "function") {
    view.requestAnimationFrame(() => {
      root.style.minHeight = previousMinHeight;
      restoreViewState(root, state);
    });
  } else {
    root.style.minHeight = previousMinHeight;
  }
}

export function replaceElementFromHtml(element, html) {
  element.replaceWith(fragmentFromHtml(element.ownerDocument, html));
}
