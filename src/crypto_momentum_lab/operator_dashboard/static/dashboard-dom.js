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
  documentElement.style.scrollBehavior = previousBehavior;
}

export function replaceChildrenFromHtml(root, html) {
  const state = captureViewState(root);
  root.replaceChildren(fragmentFromHtml(root.ownerDocument, html));
  restoreViewState(root, state);
}

export function replaceElementFromHtml(element, html) {
  element.replaceWith(fragmentFromHtml(element.ownerDocument, html));
}
