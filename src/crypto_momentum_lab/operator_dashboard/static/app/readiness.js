import {
  asNumber,
  hasUncertainStatus,
  normalizedStatus,
  statusClass,
} from "../dashboard-formatters.js";
import { readinessStatusForSection } from "../dashboard-readiness.js";
import {
  SAFETY_SECTIONS,
  getLiveMode,
  latestSectionData,
  latestSectionError,
  markSectionFailed,
  markSectionStored,
  readingReliable,
} from "./section-state.js";

export function globalReadinessModel(now = Date.now()) {
  const overview = latestSectionData.get("overview");
  const risk = latestSectionData.get("risk");
  const account = latestSectionData.get("account");
  const snapshots = [...latestSectionData.values()].filter(Boolean);
  if (!snapshots.length) {
    return {
      status: "UNKNOWN",
      detail: "等待关键服务数据",
      uncertain: "—",
      halts: "—",
      ambiguous: "—",
      reconciliation: "—",
    };
  }

  const overviewReliable = readingReliable("overview", now);
  const riskReliable = readingReliable("risk", now);
  const accountReliable = readingReliable("account", now);
  const services = overviewReliable ? overview?.services || [] : [];
  const expiredSafetySections = [...latestSectionData.keys()].filter(
    (id) => SAFETY_SECTIONS.has(id) && !readingReliable(id, now),
  ).length;
  const staleSafetySections = [...latestSectionError.keys()]
    .filter((id) => SAFETY_SECTIONS.has(id))
    .length;
  const staleNote = staleSafetySections > 0 ? " · 读数可能过期" : "";
  const uncertainSections = [...latestSectionData.entries()]
    .filter(([id, data]) => (
      SAFETY_SECTIONS.has(id)
      && readingReliable(id, now)
      && hasUncertainStatus(readinessStatusForSection(id, data))
    ))
    .length;
  const uncertainServices = services.filter((service) => hasUncertainStatus(service.status)).length;
  const uncertain = uncertainSections + uncertainServices;
  const ambiguous = riskReliable ? risk?.ambiguous_orders?.length || 0 : 0;
  const accountSnapshots = accountReliable
    ? Array.isArray(account?.accounts)
      ? account.accounts
      : account
        ? [account]
        : []
    : [];
  const haltedAccounts = accountSnapshots.filter(
    (snapshot) => normalizedStatus(snapshot.status) === "HALTED",
  ).length;
  const activeHalts = Math.max(
    overviewReliable ? asNumber(overview?.active_halt_count) || 0 : 0,
    riskReliable ? risk?.active_halts?.length || 0 : 0,
    haltedAccounts,
  );
  const mismatch = accountSnapshots.reduce(
    (total, snapshot) => total + (asNumber(snapshot.reconciliation?.mismatch_count) || 0),
    0,
  );
  const accountStatus = normalizedStatus(account?.status);
  let reconciliation = "—";
  if (accountReliable) {
    reconciliation = hasUncertainStatus(accountStatus)
      ? "UNKNOWN"
      : haltedAccounts > 0
        ? `${haltedAccounts} 停止`
        : mismatch != null && mismatch > 0
          ? `${mismatch} 差异`
          : accountSnapshots.length > 1
            ? "READY"
            : String(account.reconciliation?.status || "READY").toUpperCase();
  }

  const hasRisk = Boolean(risk);
  const hasAccount = Boolean(account);
  const hasOverview = Boolean(overview);
  const hasLiveReadings = riskReliable && accountReliable;
  const liveMode = getLiveMode();

  let status = "READY";
  let detail = "关键读数正常";
  if (!hasOverview) {
    status = "UNKNOWN";
    detail = "等待系统总览数据";
  } else if (!overviewReliable) {
    status = "STALE";
    detail = "系统总览读数已过期 · 尚未确认安全";
  } else if (activeHalts > 0) {
    status = "BLOCKED";
    detail = `存在活跃停机 · 新入场已被阻断${staleNote}`;
  } else if (ambiguous > 0) {
    status = "REVIEW";
    detail = `存在未决订单 · 需要交易所对账${staleNote}`;
  } else if (mismatch != null && mismatch > 0) {
    status = "REVIEW";
    detail = `账户对账存在差异 · 暂不视为安全${staleNote}`;
  } else if (liveMode === "LIVE" && !hasLiveReadings) {
    status = "UNKNOWN";
    const missing = [
      !riskReliable ? (hasRisk ? "风险读数已过期" : "风险数据") : null,
      !accountReliable ? (hasAccount ? "账户读数已过期" : "账户数据") : null,
    ].filter(Boolean);
    detail = `实盘会话缺少新鲜读数 · ${missing.join("、")}`;
  } else if (uncertain > 0) {
    status = "UNKNOWN";
    detail = `${uncertain} 个关键读数需要确认`;
  } else if (expiredSafetySections > 0) {
    status = "STALE";
    detail = `${expiredSafetySections} 个关键分区读数已过期 · 尚未确认安全`;
  } else if (staleSafetySections > 0) {
    status = "STALE";
    detail = `${staleSafetySections} 个关键分区刷新失败 · 尚未确认安全`;
  } else if (liveMode === "LIVE") {
    detail = "实盘链路运行中 · 关键读数正常";

  } else {
    detail = "无实盘会话 · 只读安全";
  }

  const readingsExpired = expiredSafetySections > 0;
  return {
    status,
    detail,
    uncertain: String(uncertain),
    halts: readingsExpired && activeHalts === 0 ? "—" : String(activeHalts),
    ambiguous: readingsExpired && ambiguous === 0 ? "—" : String(ambiguous),
    reconciliation: readingsExpired && reconciliation === "READY" ? "—" : reconciliation,
  };
}

export function renderGlobalReadiness() {
  const strip = document.getElementById("readiness-strip");
  if (!strip) return;
  const model = globalReadinessModel();
  strip.className = `readiness-strip ${statusClass(model.status)}`;
  const values = {
    "global-readiness": model.status,
    "global-readiness-detail": model.detail,
    "global-uncertain": model.uncertain,
    "global-halts": model.halts,
    "global-ambiguous": model.ambiguous,
    "global-reconciliation": model.reconciliation,
  };
  Object.entries(values).forEach(([id, value]) => {
    const element = document.getElementById(id);
    if (element) element.textContent = value;
  });

  const hud = document.getElementById("emergency-triage-hud");
  if (hud) {
    const isCrisis = model.status === "BLOCKED" || model.status === "REVIEW";
    if (isCrisis) {
      hud.hidden = false;
      hud.className = `emergency-triage-hud ${model.status === "REVIEW" ? "is-review" : ""}`;
      const isHalt = Number(model.halts) > 0;
      const isAmbiguous = Number(model.ambiguous) > 0;
      const label = isHalt ? "活跃停机警报" : isAmbiguous ? "未决订单待核" : "风控态势注意";
      hud.innerHTML = `
        <div class="emergency-triage-lead">
          <strong>${label}</strong>
          <span>${model.detail}</span>
        </div>
        <div class="emergency-triage-actions">
          <a href="#risk" class="emergency-triage-btn ${isHalt ? "danger" : ""}">查看风控详情</a>
        </div>
      `;
    } else {
      hud.hidden = true;
      hud.innerHTML = "";
    }
  }
}

export function updateGlobalState(id, data) {
  markSectionStored(id, data);
  renderGlobalReadiness();
}

export function markSectionError(id, reason) {
  markSectionFailed(id, reason);
  renderGlobalReadiness();
}
