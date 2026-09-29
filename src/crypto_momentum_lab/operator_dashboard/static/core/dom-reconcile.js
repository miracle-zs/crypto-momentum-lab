/**
 * Keyed DOM reconciliation. No window-scroll policy lives here —
 * callers pair this with core/view-state.js and core/scroll-keep.js.
 */

export function fragmentFromHtml(ownerDocument, html) {
  const template = ownerDocument.createElement("template");
  template.innerHTML = html;
  return template.content;
}

function elementKey(node) {
  if (node?.nodeType !== 1) return null;
  for (const attribute of [
    "id",
    "data-state-key",
    "data-echart-id",
    "data-row-key",
    "data-live-account-label",
    "data-account-equity-range",
    "data-live-account-metrics-range",
  ]) {
    const value = node.getAttribute(attribute);
    if (value) return `${attribute}:${value}`;
  }
  return null;
}

function compatibleNode(left, right) {
  return left?.nodeType === right?.nodeType
    && (left?.nodeType !== 1 || left.tagName === right.tagName);
}

function isRuntimeMarker(attributeName) {
  return attributeName === "data-echart-mounted"
    || (attributeName.startsWith("data-") && attributeName.endsWith("-wired"));
}

function reconcileNode(current, next) {
  if (!compatibleNode(current, next)) {
    current.replaceWith(next);
    return next;
  }
  if (current.nodeType === 3) {
    if (current.nodeValue !== next.nodeValue) current.nodeValue = next.nodeValue;
    return current;
  }

  // ECharts owns the full live surface, including runtime attributes such as
  // its instance id and the generated SVG subtree. Reconcile only the keyed
  // shell around it, then refresh the existing instance from its new payload.
  if (current.matches?.(".echart-surface")) return current;

  const preserveDisclosureState = current.tagName === "DETAILS";
  for (const attribute of Array.from(current.attributes)) {
    if (preserveDisclosureState && attribute.name === "open") continue;
    if (isRuntimeMarker(attribute.name)) continue;
    if (!next.hasAttribute(attribute.name)) current.removeAttribute(attribute.name);
  }
  for (const attribute of Array.from(next.attributes)) {
    if (preserveDisclosureState && attribute.name === "open") continue;
    if (current.getAttribute(attribute.name) !== attribute.value) {
      current.setAttribute(attribute.name, attribute.value);
    }
  }

  reconcileChildren(current, next);
  return current;
}

export function reconcileChildren(parent, templateParent) {
  const oldChildren = Array.from(parent.childNodes);
  const newChildren = Array.from(templateParent.childNodes);
  const oldByKey = new Map(
    oldChildren
      .map((node) => [elementKey(node), node])
      .filter(([key]) => key),
  );
  const used = new Set();

  newChildren.forEach((next, index) => {
    const key = elementKey(next);
    let current = key ? oldByKey.get(key) : oldChildren[index];
    if (current && (used.has(current) || (key && elementKey(current) !== key))) {
      current = null;
    }
    if (current && !key && elementKey(current)) current = null;
    if (current && !compatibleNode(current, next)) current = null;
    if (!current && !key) {
      current = oldChildren.find((candidate) => (
        !used.has(candidate)
        && !elementKey(candidate)
        && compatibleNode(candidate, next)
      )) || null;
    }

    if (!current) {
      current = next;
      parent.insertBefore(current, parent.childNodes[index] || null);
    } else {
      current = reconcileNode(current, next);
      const reference = parent.childNodes[index] || null;
      if (current !== reference) parent.insertBefore(current, reference);
    }
    used.add(current);
  });

  oldChildren.forEach((node) => {
    if (!used.has(node) && node.parentNode === parent) parent.removeChild(node);
  });
}
