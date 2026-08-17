export function readinessStatusForSection(sectionId, data) {
  if (sectionId === "overview") {
    return data?.database_status ?? data?.status;
  }
  return data?.status;
}
