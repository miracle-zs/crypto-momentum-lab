/**
 * Shared mutable dashboard app state (section snapshots, poll bookkeeping).
 * Single module instance — do not import via ?v= variants.
 */

export const SECTION_FETCH_TIMEOUT_MS = 12 * 1000;
export const SAFETY_READING_TTL_MS = 60 * 1000;
export const SAFETY_SECTIONS = new Set(["overview", "risk", "account", "strategy", "universe"]);

export const sectionInFlight = new Set();
export const lastSectionPollAt = new Map();
export const latestSectionData = new Map();
export const latestSectionError = new Map();
export const latestSectionUpdatedAt = new Map();
export const forcedSectionRefreshes = new Set();
export const sectionRenderKeys = new Map();

export let latestLiveService = null;
export let latestLiveMode = "UNKNOWN";

export function setLiveService(service) {
  latestLiveService = service || null;
}

export function setLiveMode(mode) {
  latestLiveMode = mode || "UNKNOWN";
}

export function getLiveMode() {
  return latestLiveMode;
}

export function getLiveService() {
  return latestLiveService;
}

export function readingReliable(id, now = Date.now()) {
  if (!latestSectionData.has(id)) return false;
  const updatedAt = latestSectionUpdatedAt.get(id);
  return updatedAt != null && now - updatedAt <= SAFETY_READING_TTL_MS;
}

export function markSectionStored(id, data) {
  latestSectionData.set(id, data);
  latestSectionError.delete(id);
  latestSectionUpdatedAt.set(id, Date.now());
  forcedSectionRefreshes.delete(id);
}

export function markSectionFailed(id, reason) {
  latestSectionError.set(id, { reason, at: Date.now() });
}

export function expireSectionReading(id) {
  latestSectionUpdatedAt.delete(id);
  forcedSectionRefreshes.add(id);
  lastSectionPollAt.delete(id);
}
