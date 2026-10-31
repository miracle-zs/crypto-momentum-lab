import test from "node:test";
import assert from "node:assert/strict";
import { wireMetricFilters } from "../../src/crypto_momentum_lab/operator_dashboard/static/app/wire-widgets.js";

test("metric selection survives a data refresh and remains interactive", () => {
  const previousWindow = globalThis.window;
  globalThis.window = { dispatchEvent() {} };
  try {
    const cards = ["equity", "margin", "drawdown"].map((category) => ({
      dataset: { metricCategory: category }, hidden: false,
    }));
    const buttons = ["all", "equity", "margin", "drawdown"].map((filter) => ({
      dataset: { metricFilter: filter },
      classList: { toggle(_name, active) { this.active = active; } },
      setAttribute(_name, value) { this.selected = value; },
      addEventListener(_name, callback) { this.click = callback; },
    }));
    const block = { querySelectorAll: () => cards };
    const tabs = {
      dataset: {}, querySelectorAll: () => buttons, closest: () => block,
    };
    const root = { dataset: {}, querySelectorAll: () => [tabs] };
    wireMetricFilters(root);
    buttons[2].click();
    assert.deepEqual(cards.map((card) => card.hidden), [true, false, true]);

    // A DOM patch restores the template's default attributes before rewiring.
    cards.forEach((card) => { card.hidden = false; });
    wireMetricFilters(root);
    assert.deepEqual(cards.map((card) => card.hidden), [true, false, true]);
    assert.equal(buttons[2].selected, "true");
    buttons[0].click();
    assert.deepEqual(cards.map((card) => card.hidden), [false, false, false]);
  } finally {
    globalThis.window = previousWindow;
  }
});
