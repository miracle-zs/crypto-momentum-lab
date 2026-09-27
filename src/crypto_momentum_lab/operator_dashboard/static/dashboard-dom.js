function fragmentFromHtml(ownerDocument, html) {
  const template = ownerDocument.createElement("template");
  template.innerHTML = html;
  return template.content;
}

export function captureViewState(root) {
  const view = root.ownerDocument.defaultView;
  return {
    pageX: view.scrollX,
    pageY: view.scrollY,
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
  const view = root.ownerDocument.defaultView;
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

  const documentElement = root.ownerDocument.documentElement;
  const previousBehavior = documentElement.style.scrollBehavior;
  documentElement.style.scrollBehavior = "auto";
  view.scrollTo(state.pageX, state.pageY);
  if (typeof view.scrollTo === "function" && (view.scrollX !== state.pageX || view.scrollY !== state.pageY)) {
    try {
      view.scrollTo({ left: state.pageX, top: state.pageY, behavior: "instant" });
    } catch {
      // Ignore browsers lacking scrollTo options
    }
  }
  documentElement.style.scrollBehavior = previousBehavior;
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
