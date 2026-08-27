import {
  esc,
  money,
  num,
  percent,
  price,
  statusClass,
} from "./dashboard-formatters.js";

export const pill = (status) => `<span class="pill ${statusClass(status)}"><i></i>${esc(status || "UNKNOWN")}</span>`;

export const sideTag = (side) => side === "long"
  ? '<span class="side-tag long">多</span>'
  : '<span class="side-tag short">空</span>';

export const signalEvidence = (row) => {
  const features = row.features || {};
  const referencePrices = row.reference_prices || {};
  const parts = [];
  const add = (label, value) => {
    if (value == null || value === "") return;
    parts.push(`<span class="signal-evidence-item"><b>${esc(label)}</b>${esc(value)}</span>`);
  };
  const moneyValue = (value) => money(value);
  const percentValue = (value, digits = 2) => percent(value, digits);

  if (features.liquidation_notional != null) {
    add("清算", `${moneyValue(features.liquidation_notional)} / ${num(features.liquidation_count, 0)} 笔`);
  }
  if (features.range_width_pct != null) add("压缩区间", percentValue(features.range_width_pct, 2));
  if (features.impulse_return_pct != null) add("冲击收益", percentValue(features.impulse_return_pct, 2));
  if (features.notional_intensity != null) add("成交强度", `${num(features.notional_intensity, 2)}x`);
  if (features.breakout_distance_pct != null) add("突破距离", percentValue(features.breakout_distance_pct, 2));
  if (features.aggressive_imbalance != null) add("主动不平衡", percentValue(features.aggressive_imbalance, 1));
  const tradeNotional = features.trade_notional ?? features.impulse_trade_notional ?? features.cluster_trade_notional;
  if (tradeNotional != null) add("成交额", moneyValue(tradeNotional));
  if (features.aggressive_buy_notional != null) add("主动买", moneyValue(features.aggressive_buy_notional));
  if (features.aggressive_sell_notional != null) add("主动卖", moneyValue(features.aggressive_sell_notional));
  if (features.breakout_price != null) add("触发价", price(features.breakout_price));
  else if (referencePrices.breakout_level != null) add("触发价", price(referencePrices.breakout_level));
  if (!parts.length) return "—";
  return `<div class="signal-evidence">${parts.join("")}</div>`;
};

export const tile = (label, value, sub = "", cls = "") =>
  `<div class="tile"><label>${esc(label)}</label><strong class="${cls}">${esc(value)}</strong><small>${esc(sub)}</small></div>`;

export const emptyBox = (text = "暂无数据", hint = "") =>
  `<div class="empty"><span>${esc(text)}</span>${hint ? `<small>${esc(hint)}</small>` : ""}</div>`;

export const blockTitle = (title, eyebrow, aside = "") =>
  `<div class="block-title"><div><b>${esc(title)}</b><small>${esc(eyebrow)}</small></div>${aside ? `<span>${aside}</span>` : ""}</div>`;

export const disclosure = (
  title,
  eyebrow,
  content,
  aside = "",
  { open = false, stateKey = "" } = {},
) => {
  const state = stateKey ? ` data-state-key="${esc(stateKey)}"` : "";
  return `<details class="block secondary disclosure"${state}${open ? " open" : ""}>
    <summary>${blockTitle(title, eyebrow, aside)}<span class="disclosure-hint">查看详情</span></summary>
    <div class="disclosure-body">${content}</div>
  </details>`;
};

export function dataTable(columns, rows, options = {}) {
  if (!rows?.length) return emptyBox(options.emptyText || "暂无数据");
  const head = columns.map((column) =>
    `<th class="${column.align === "right" ? "ta-r" : ""}">${esc(column.label)}</th>`).join("");
  const body = rows.map((row) => `<tr>${columns.map((column) => {
    const raw = column.value ? column.value(row) : row[column.key];
    const classes = [
      column.align === "right" ? "ta-r num" : "",
      typeof column.cls === "function" ? column.cls(row) : column.cls || "",
    ].filter(Boolean).join(" ");
    return `<td class="${classes}">${column.html ? raw : esc(raw)}</td>`;
  }).join("")}</tr>`).join("");
  const stateKey = options.stateKey ? ` data-state-key="${esc(options.stateKey)}"` : "";
  return `<div class="table-scroll${options.tall ? " tall" : ""}"${stateKey}><table class="data-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}
