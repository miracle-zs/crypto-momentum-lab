function cloneSnapshot(data) {
  return JSON.parse(JSON.stringify(data));
}

/**
 * Build the identity used to decide whether a section needs a DOM rebuild.
 * Heartbeat timestamps are refreshed frequently but do not change the
 * account/strategy structure that the user is interacting with.
 */
export function sectionRenderKey(id, data) {
  const snapshot = cloneSnapshot(data);
  delete snapshot.generated_at;

  if (id === "overview") {
    for (const service of snapshot.services || []) {
      delete service.age_seconds;
      delete service.observed_at;
    }
    for (const account of snapshot.account_statuses || []) {
      delete account.observed_at;
      delete account.lease_expires_at;
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
    const fields = [
      "account_label",
      "status",
      "strategy_name",
      "strategy_state",
      "readiness",
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
  }

  if (id === "strategy") {
    const fields = [
      "run_id",
      "strategy_name",
      "exit_mode",
      "exit_label",
      "config_hash",
      "status",
    ];
    snapshot.accounts = (snapshot.accounts || []).map((account) =>
      Object.fromEntries(
        fields
          .filter((field) => account[field] !== undefined)
          .map((field) => [field, account[field]]),
      ),
    );
  }

  return JSON.stringify(snapshot);
}
