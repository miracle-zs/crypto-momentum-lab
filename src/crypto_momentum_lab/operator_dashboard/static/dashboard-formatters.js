import { DISPLAY_TIME_ZONE } from "./dashboard-config.js";

const DISPLAY_TIME_FORMATTER = new Intl.DateTimeFormat("en-CA", {
  timeZone: DISPLAY_TIME_ZONE,
  calendar: "gregory",
  numberingSystem: "latn",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hourCycle: "h23",
});

const UNCERTAIN_STATUSES = new Set(["UNKNOWN", "STALE", "DOWN", "NO DATA", "NO-DATA"]);

export const esc = (value) => String(value ?? "—").replace(/[&<>'"]/g, (char) => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;",
}[char]));

export const statusSlug = (status) => String(status || "UNKNOWN")
  .trim()
  .replace(/[^A-Za-z]+/g, "-")
  .toUpperCase();

export const statusClass = (status) => `status-${statusSlug(status)}`;
export const normalizedStatus = (status) => String(status || "").trim().toUpperCase();
export const hasUncertainStatus = (status) => UNCERTAIN_STATUSES.has(normalizedStatus(status));

const displayTimeParts = (value) => {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return Object.fromEntries(
    DISPLAY_TIME_FORMATTER.formatToParts(parsed)
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value]),
  );
};

export const asNumber = (value) => {
  if (value == null || value === "") return null;
  const parsed = Number(value);
  return Number.isNaN(parsed) ? null : parsed;
};

export const num = (value, digits = 2) => {
  const parsed = asNumber(value);
  return parsed == null ? "—" : parsed.toLocaleString("en-US", {
    minimumFractionDigits: digits, maximumFractionDigits: digits,
  });
};

export const price = (value) => {
  const parsed = asNumber(value);
  if (parsed == null) return value == null ? "—" : String(value);
  const magnitude = Math.abs(parsed);
  const digits = magnitude >= 1000 ? 2 : magnitude >= 10 ? 3 : magnitude >= 0.1 ? 4 : 6;
  return num(parsed, digits);
};

export const money = (value) => {
  const parsed = asNumber(value);
  return parsed == null ? "—" : `${parsed < 0 ? "−" : ""}$${num(Math.abs(parsed))}`;
};

export const signedMoney = (value) => {
  const parsed = asNumber(value);
  return parsed == null ? "—" : `${parsed > 0 ? "+" : parsed < 0 ? "−" : ""}$${num(Math.abs(parsed))}`;
};

export const percent = (value, digits = 2) => {
  const parsed = asNumber(value);
  return parsed == null ? "—" : `${(parsed * 100).toFixed(digits)}%`;
};

export const signedPercent = (value, digits = 2) => {
  const parsed = asNumber(value);
  return parsed == null ? "—" : `${parsed > 0 ? "+" : ""}${(parsed * 100).toFixed(digits)}%`;
};

export const pnlClass = (value) => {
  const parsed = asNumber(value);
  return parsed == null || parsed === 0 ? "" : parsed > 0 ? "pos" : "neg";
};

export const timeOnly = (value) => {
  if (!value) return "—";
  const parts = displayTimeParts(value);
  return parts ? `${parts.hour}:${parts.minute}:${parts.second}` : String(value);
};

export const dayTime = (value) => {
  if (!value) return "—";
  const parts = displayTimeParts(value);
  return parts
    ? `${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second}`
    : String(value);
};

export const elapsedTime = (start, end) => {
  const startAt = new Date(start).getTime();
  const endAt = new Date(end).getTime();
  if (!Number.isFinite(startAt) || !Number.isFinite(endAt) || endAt < startAt) return "—";
  const minutes = Math.max(0, Math.round((endAt - startAt) / 60000));
  const hours = Math.floor(minutes / 60);
  const remainder = minutes % 60;
  if (hours === 0) return `${remainder} 分钟`;
  return remainder ? `${hours} 小时 ${remainder} 分` : `${hours} 小时`;
};

export const relAge = (seconds) => {
  const parsed = asNumber(seconds);
  if (parsed == null) return "未知";
  if (parsed < 60) return `${Math.round(parsed)} 秒前`;
  if (parsed < 3600) return `${Math.floor(parsed / 60)} 分前`;
  if (parsed < 86400) return `${Math.floor(parsed / 3600)} 小时前`;
  return `${Math.floor(parsed / 86400)} 天前`;
};

export const relToNow = (value) => {
  if (!value) return "未知";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "未知";
  const delta = (Date.now() - parsed.getTime()) / 1000;
  return delta >= 0 ? relAge(delta) : `${Math.round(-delta / 60)} 分后`;
};

export const liveHeartbeatAge = (service) => {
  if (!service) return null;
  const observedAt = new Date(service.observed_at || "").getTime();
  if (Number.isFinite(observedAt)) {
    return Math.max(0, (Date.now() - observedAt) / 1000);
  }
  return asNumber(service.age_seconds);
};

export const liveHeartbeatStatus = (age) => {
  if (age == null) return "UNKNOWN";
  return age <= 120 ? "FRESH" : "STALE";
};

export const shortHash = (value) => {
  const hash = String(value || "").trim();
  return hash.length > 8 ? `${hash.slice(0, 8)}…` : hash || "—";
};
