from pathlib import Path

import pytest
import yaml

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
    assert primary.migration_revision == "20260925_0043"
    assert primary.services == (
        "execution-account-live",
        "live-strategy",
    )
    assert account_3.session_id == "live-account-3-v1"
    assert account_3.lease_owner == "live-worker-account-3"
    assert account_3.migration_revision == "20260925_0043"
    assert len(primary.strategy_config_hash) == 64
    assert primary.strategy_config_hash != "unset"
    assert primary.execution_inputs.hedge_mode is True
    assert primary.execution_inputs.entry_long_only is True
    assert primary.execution_inputs.entry_leverage == 5
    assert primary.execution_inputs.margin_type == "CROSSED"
    assert primary.execution_inputs.exit_mode.value == "candle_15m"
    assert primary.execution_inputs.candle_grace_bars == 8
    assert primary.execution_inputs.persist_exchange_operations == ("cancel,submit")

    expected_profiles = {
        "primary": {
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "min_return_pct": "0.0075",
            "min_aggressive_imbalance": "0.30",
            "min_notional_intensity": "3.0",
            "min_notional_5m_vs_30m": "1.25",
            "cooldown_buckets": 0,
        },
        "account-2": {
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "min_return_pct": "0.0075",
            "min_aggressive_imbalance": "0.30",
            "min_notional_intensity": "3.0",
            "min_notional_5m_vs_30m": "1.25",
            "cooldown_buckets": 0,
        },
        "account-3": {
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "min_return_pct": "0.0075",
            "min_aggressive_imbalance": "0.30",
            "min_notional_intensity": "3.0",
            "min_notional_5m_vs_30m": "1.25",
            "cooldown_buckets": 0,
        },
        "account-4": {
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "min_return_pct": "0.0075",
            "min_aggressive_imbalance": "0.30",
            "min_notional_intensity": "3.0",
            "min_notional_5m_vs_30m": "1.25",
            "cooldown_buckets": 0,
        },
    }
    assert {
        label: manifest.account(label).strategy_inputs.profile.as_dict()
        for label in expected_profiles
    } == expected_profiles


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
    strategy: "${STRATEGY:-invalid_default_strategy}"
    session_id: live-primary-v1
    lease_owner: live-worker
    profile_ref: profile.yaml
    limits_ref: postgres:risk_config/primary
    services: [live-strategy]
    execution_config:
      target_notional: 100
      hedge_mode: true
      entry_long_only: true
      entry_leverage: 5
      margin_type: CROSSED
      exit_mode: candle_15m
      candle_grace_bars: 8
      candle_grace_decision_profit_pct: 0.001
      candle_grace_profit_pct: 0.0088
      persist_exchange_operations: submit,cancel
      max_concurrency_per_symbol: 2
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
        environment={"IMAGE_COMMIT": "b" * 40, "STRATEGY": "orderflow_impulse"},
    )
    assert manifest.account("primary").strategy == "orderflow_impulse"

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
    execution_config:
      target_notional: 100
      hedge_mode: true
      entry_long_only: true
      entry_leverage: 5
      margin_type: CROSSED
      exit_mode: candle_15m
      candle_grace_bars: 8
      candle_grace_decision_profit_pct: 0.001
      candle_grace_profit_pct: 0.0088
      persist_exchange_operations: submit,cancel
      max_concurrency_per_symbol: 2
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
    execution_config:
      target_notional: 100
      hedge_mode: true
      entry_long_only: true
      entry_leverage: 5
      margin_type: CROSSED
      exit_mode: candle_15m
      candle_grace_bars: 8
      candle_grace_decision_profit_pct: 0.001
      candle_grace_profit_pct: 0.0088
      persist_exchange_operations: submit,cancel
      max_concurrency_per_symbol: 2
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


def test_manifest_accepts_trading_configuration_without_compliance_metadata(tmp_path):
    document = _trading_manifest_document()
    path = tmp_path / "trading.yaml"
    path.write_text(yaml.safe_dump(document))
    manifest = load_live_runtime_manifest(path, environment={})
    assert manifest.account("primary").execution_inputs.candle_grace_bars == 8
    assert (
        manifest.account("account-4").strategy_inputs.entry_order_type.value == "limit"
    )


def test_runtime_manifest_requires_explicit_execution_config(tmp_path: Path) -> None:
    document = _trading_manifest_document()
    document["accounts"][0].pop("execution_config")
    path = tmp_path / "runtime.yaml"
    path.write_text(yaml.safe_dump(document))

    with pytest.raises(
        RuntimeManifestError, match="execution_config must be an object"
    ):
        load_live_runtime_manifest(path, environment={})


def test_runtime_manifest_requires_explicit_lease_owner(tmp_path: Path) -> None:
    document = _trading_manifest_document()
    document["accounts"][0].pop("lease_owner")
    path = tmp_path / "runtime.yaml"
    path.write_text(yaml.safe_dump(document))

    with pytest.raises(RuntimeManifestError, match=r"accounts\[0\]\.lease_owner"):
        load_live_runtime_manifest(path, environment={})


@pytest.mark.parametrize(
    "field",
    ["target_notional", "max_concurrency_per_symbol"],
)
def test_runtime_manifest_requires_explicit_sizing_fields(
    tmp_path: Path, field: str
) -> None:
    document = _trading_manifest_document()
    document["accounts"][0]["execution_config"].pop(field)
    path = tmp_path / "runtime.yaml"
    path.write_text(yaml.safe_dump(document))

    with pytest.raises(RuntimeManifestError, match=field):
        load_live_runtime_manifest(path, environment={})


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("entry_leverage", 5.5, "entry_leverage must be an integer"),
        ("hedge_mode", "yes", "hedge_mode must be a boolean"),
    ],
)
def test_runtime_manifest_rejects_coerced_execution_settings(
    tmp_path: Path, field: str, value: object, message: str
) -> None:
    document = _trading_manifest_document()
    document["accounts"][0]["execution_config"][field] = value
    path = tmp_path / "runtime.yaml"
    path.write_text(yaml.safe_dump(document))

    with pytest.raises(RuntimeManifestError, match=message):
        load_live_runtime_manifest(path, environment={})


@pytest.mark.parametrize("value", ["cross", "isolated", " CROSSED "])
def test_runtime_manifest_rejects_margin_type_aliases(
    tmp_path: Path, value: str
) -> None:
    document = _trading_manifest_document()
    document["accounts"][0]["execution_config"]["margin_type"] = value
    path = tmp_path / "runtime.yaml"
    path.write_text(yaml.safe_dump(document))

    with pytest.raises(RuntimeManifestError, match="margin_type"):
        load_live_runtime_manifest(path, environment={})


def _trading_manifest_document():
    document = yaml.safe_load(Path("deploy/live-runtime.yaml").read_text())
    document.pop("runtime")
    for account in document["accounts"]:
        account.pop("image_commit", None)
        account.pop("migration_revision", None)
    return document


def test_unknown_strategy_does_not_produce_unset_hash(tmp_path):
    source = Path("deploy/live-runtime.yaml").read_text()
    path = tmp_path / "runtime.yaml"
    path.write_text(source.replace("orderflow_impulse", "unknown_strategy"))
    with pytest.raises(RuntimeManifestError, match="unknown_strategy"):
        load_live_runtime_manifest(path, environment={"CML_CODE_COMMIT": "a" * 40})
