import { DISPLAY_TIME_ZONE_LABEL } from "../dashboard-config.js";
import {
  asNumber,
  dayTime,
  esc,
  relAge,
  relToNow,
  statusClass,
  statusSlug,
} from "../dashboard-formatters.js";
import { blockTitle, emptyBox, pill, tile } from "../dashboard-ui.js";

export function renderOverview(data) {
  const lease = data.active_lease;
  const services = data.services || [];
  const haltCount = data.active_halt_count || 0;
  const tiles = `<div class="tile-grid">
    ${tile("数据库", data.database_status || "UNKNOWN", "PostgreSQL 只读连接", statusSlug(data.database_status) === "READY" ? "pos" : "warn")}
    ${tile("活跃停机", haltCount, haltCount ? "入场信号已被阻断" : "无全局停机", haltCount ? "neg" : "")}
    ${tile("交易租约", lease?.strategy_name || "无租约", lease ? `持有者 ${lease.owner || "未知"}` : "当前无进程持有交易权", "txt")}
    ${tile("租约到期", lease?.expires_at ? relToNow(lease.expires_at) : "—", lease?.expires_at ? `${dayTime(lease.expires_at)} ${DISPLAY_TIME_ZONE_LABEL}` : "")}
  </div>`;
  const serviceRows = services.map((service) => {
    const age = service.age_seconds;
    const freshness = age == null ? 0 : Math.max(6, 100 - Math.min(100, (age / 120) * 100));
    return `<div class="service-row">
      <b>${esc(service.name)}</b>
      <span class="service-meter"><i data-service-meter="${esc(service.name)}" class="${statusClass(service.status)}" style="width:${freshness.toFixed(0)}%"></i></span>
      <span class="service-age num" data-service-age="${esc(service.name)}">${age == null ? "未知" : relAge(age)}</span>
      ${pill(service.status)}
    </div>`;
  }).join("");
  const body = `${tiles}
    ${blockTitle("服务心跳", "SERVICE HEARTBEATS", `<span class="num muted">${services.length} 个进程</span>`)}
    <div class="service-list">${serviceRows || emptyBox("尚未观察到任何服务心跳")}</div>`;
  return [haltCount ? "HALTED" : data.database_status, body];
}

export function updateOverviewDynamic(section, data) {
  if (!section || !data) return;
  const services = new Map(
    (data.services || []).map((service) => [String(service.name), service]),
  );
  section.querySelectorAll("[data-service-age]").forEach((element) => {
    const service = services.get(element.dataset.serviceAge);
    if (!service) return;
    const observedAt = new Date(service.observed_at || "").getTime();
    const age = Number.isFinite(observedAt)
      ? Math.max(0, (Date.now() - observedAt) / 1000)
      : asNumber(service.age_seconds);
    element.textContent = age == null ? "未知" : relAge(age);
    const meter = Array.from(section.querySelectorAll("[data-service-meter]")).find(
      (candidate) => candidate.dataset.serviceMeter === element.dataset.serviceAge,
    );
    if (meter && age != null) {
      const freshness = Math.max(6, 100 - Math.min(100, (age / 120) * 100));
      meter.style.width = `${freshness.toFixed(0)}%`;
    }
  });
}
