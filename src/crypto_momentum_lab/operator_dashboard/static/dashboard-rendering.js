/**
 * Build the identity used to decide whether a section needs a DOM rebuild.
 * Heartbeat timestamps are refreshed frequently but do not change the
 * account/strategy structure that the user is interacting with.
 */
export function sectionRenderKey(id, data) {
  if (!data || typeof data !== "object") return String(data);
  const snapshot = { ...data };
  delete snapshot.generated_at;

  if (id === "overview") {
    if (Array.isArray(snapshot.services)) {
      snapshot.services = snapshot.services.map(({ age_seconds, observed_at, ...rest }) => rest);
    }
    if (Array.isArray(snapshot.account_statuses)) {
      snapshot.account_statuses = snapshot.account_statuses.map(
        ({ observed_at, lease_expires_at, ...rest }) => rest,
      );
    }
  }

  if (id === "risk") {
    delete snapshot.data_age_seconds;
    delete snapshot.observed_at;
  }

  if (id === "collector") {
    delete snapshot.checkpoint_age_seconds;
    delete snapshot.parquet_latest_age_seconds;
    delete snapshot.pending_spool_oldest_age_seconds;
  }

  if (id === "account") {
    // Structural identity only. status / readiness / strategy_state flip on
    // every sync cycle (syncing ↔ ready_readonly) and must NOT rebuild the
    // DOM — a full rebuild blanks chart slots and collapses document height,
    // which clamps scrollTop and reads as an auto jump to the top.
    const fields = [
      "account_label",
      "strategy_name",
      "mode",
      "environment",
    ];
    snapshot.accounts = (snapshot.accounts || []).map((account) =>
      Object.fromEntries(
        fields
          .filter((field) => account[field] !== undefined)
          .map((field) => [field, account[field]]),
      ),
    );
    delete snapshot.selected_account_label;
    delete snapshot.status;
  }

  if (id === "strategy") {
    const fields = [
      "run_id",
      "strategy_name",
      "exit_mode",
      "exit_label",
      "config_hash",
    ];
    snapshot.accounts = (snapshot.accounts || []).map((account) =>
      Object.fromEntries(
        fields
          .filter((field) => account[field] !== undefined)
          .map((field) => [field, account[field]]),
      ),
    );
    delete snapshot.status;
  }

  return JSON.stringify(snapshot);
}
