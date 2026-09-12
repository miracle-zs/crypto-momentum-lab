from pathlib import Path

import pytest

from crypto_momentum_lab.live_rollout.runtime_manifest import (
    RuntimeManifestError,
    load_live_runtime_manifest,
)


def test_checked_in_live_runtime_manifest_resolves_account_identity() -> None:
    manifest = load_live_runtime_manifest(
        Path("deploy/live-runtime.yaml"),
        environment={"CML_CODE_COMMIT": "a" * 40},
    )

    primary = manifest.account("primary")
    account_3 = manifest.account("account-3")

    assert primary.strategy == "orderflow_impulse"
    assert primary.image_commit == "a" * 40
    assert primary.migration_revision == "20260911_0036"
    assert primary.services == (
        "execution-account-live",
        "live-strategy",
    )
    assert account_3.session_id == "live-account-3-v1"
    assert account_3.lease_owner == "live-worker-account-3"
    assert account_3.migration_revision == "20260911_0036"
    assert len(primary.strategy_config_hash) == 64
    assert primary.strategy_config_hash != "unset"
    assert primary.execution_inputs.hedge_mode is True
    assert primary.execution_inputs.entry_long_only is True
    assert primary.execution_inputs.entry_leverage == 5
    assert primary.execution_inputs.margin_type == "CROSSED"
    assert primary.execution_inputs.exit_mode.value == "candle_15m"
    assert primary.execution_inputs.candle_grace_bars == 8
    assert primary.execution_inputs.persist_exchange_operations == (
        "cancel,submit"
    )


def test_runtime_manifest_expands_override_and_rejects_missing_required_value(
    tmp_path: Path,
) -> None:
    path = tmp_path / "runtime.yaml"
    path.write_text(
        """
schema_version: 1
runtime:
  image_commit: "${IMAGE_COMMIT:?IMAGE_COMMIT is required}"
  migration_revision: "20260911_0040"
accounts:
  - label: primary
    strategy: "${STRATEGY:-orderflow_impulse}"
    session_id: live-primary-v1
    lease_owner: live-worker
    profile_ref: profile.yaml
    limits_ref: postgres:risk_config/primary
    services: [live-strategy]
    strategy_config:
      impulse_window_buckets: 4
      confirmation_buckets: 1
      min_return_pct: 0.005
      min_imbalance: 0.30
      min_intensity: 1.5
      min_notional_5m_vs_30m: 1.50
      cooldown_buckets: 0
      entry_positive_gainer_top_count: 10
      require_price_above_ema5: false
      require_price_above_ema10: false
      entry_policy_mode: enforce
      entry_order_type: limit
      entry_limit_ttl_seconds: 900
""",
        encoding="utf-8",
    )

    manifest = load_live_runtime_manifest(
        path,
        environment={"IMAGE_COMMIT": "b" * 40, "STRATEGY": "custom"},
    )
    assert manifest.account("primary").strategy == "custom"

    with pytest.raises(RuntimeManifestError, match="IMAGE_COMMIT is required"):
        load_live_runtime_manifest(path, environment={})


def test_runtime_manifest_rejects_duplicate_account_labels(tmp_path: Path) -> None:
    path = tmp_path / "runtime.yaml"
    path.write_text(
        """
schema_version: 1
runtime:
  image_commit: "a"
  migration_revision: "b"
accounts:
  - label: primary
    strategy: orderflow_impulse
    session_id: session-1
    lease_owner: worker-1
    profile_ref: profile.yaml
    limits_ref: limits.yaml
    services: [live-strategy]
    strategy_config:
      impulse_window_buckets: 4
      confirmation_buckets: 1
      min_return_pct: 0.005
      min_imbalance: 0.30
      min_intensity: 1.5
      min_notional_5m_vs_30m: 1.50
      cooldown_buckets: 0
      entry_positive_gainer_top_count: 10
      require_price_above_ema5: false
      require_price_above_ema10: false
      entry_policy_mode: enforce
      entry_order_type: limit
      entry_limit_ttl_seconds: 900
  - label: primary
    strategy: orderflow_impulse
    session_id: session-2
    lease_owner: worker-2
    profile_ref: profile.yaml
    limits_ref: limits.yaml
    services: [live-strategy-2]
    strategy_config:
      impulse_window_buckets: 4
      confirmation_buckets: 1
      min_return_pct: 0.005
      min_imbalance: 0.30
      min_intensity: 1.5
      min_notional_5m_vs_30m: 1.50
      cooldown_buckets: 0
      entry_positive_gainer_top_count: 10
      require_price_above_ema5: false
      require_price_above_ema10: false
      entry_policy_mode: enforce
      entry_order_type: limit
      entry_limit_ttl_seconds: 900
""",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeManifestError, match="duplicate runtime account"):
        load_live_runtime_manifest(path, environment={})
