export function readinessStatusForSection(sectionId, data) {
  if (data?._cache_status === "STALE") {
    return "STALE";
  }
  if (sectionId === "overview") {
    return data?.database_status ?? data?.status;
  }
  return data?.status;
}

