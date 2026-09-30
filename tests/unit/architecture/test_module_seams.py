"""Guard application seams against accidental eager database imports."""

import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "module",
    [
        "crypto_momentum_lab.live_rollout.order_identity_errors",
        "crypto_momentum_lab.live_rollout.shadow_preflight",
        "crypto_momentum_lab.live_rollout.session_state",
        "crypto_momentum_lab.live_rollout.lease_recovery",
        "crypto_momentum_lab.live_rollout.exit_channel_ports",
        "crypto_momentum_lab.live_rollout.exit_failure_policy",
        "crypto_momentum_lab.live_rollout.order_event_runtime",
        "crypto_momentum_lab.live_rollout.exit_event_coordinator",
        "crypto_momentum_lab.live_rollout.exit_event_ports",
        "crypto_momentum_lab.live_rollout.entry_orders",
        "crypto_momentum_lab.live_rollout.account_event_ports",
        "crypto_momentum_lab.live_rollout.telemetry_ports",
        "crypto_momentum_lab.live_rollout.control_plane",
        "crypto_momentum_lab.live_rollout.context",
        "crypto_momentum_lab.live_rollout.market_admission",
        "crypto_momentum_lab.live_rollout.risk_control",
        "crypto_momentum_lab.live_rollout.scheduled_controller",
        "crypto_momentum_lab.live_rollout.decision_facts",
        "crypto_momentum_lab.domain.execution.missing_order_rules",
        "crypto_momentum_lab.live_rollout.entry_runtime",
        "crypto_momentum_lab.live_rollout.entry_expectations",
        "crypto_momentum_lab.live_rollout.resource_ports",
        "crypto_momentum_lab.live_rollout.resource_lifecycle",
        "crypto_momentum_lab.strategy_runner.live_source",
        "crypto_momentum_lab.research_collector.source",
        "crypto_momentum_lab.research_collector",
        "crypto_momentum_lab.research_collector.models",
        "crypto_momentum_lab.live_rollout.hub_cursor",
        "crypto_momentum_lab.live_rollout.market_runtime_contracts",
        "crypto_momentum_lab.live_rollout.startup_recovery",
        "crypto_momentum_lab.domain.market.runtime_state_repository",
        "crypto_momentum_lab.domain.market.runtime_state_models",
        "crypto_momentum_lab.live_rollout.position_classification",
        "crypto_momentum_lab.operator_dashboard.ports",
        "crypto_momentum_lab.live_rollout.position_self_healing",
        "crypto_momentum_lab.domain.execution.position_repair",
        "crypto_momentum_lab.domain.execution.position_recovery",
        "crypto_momentum_lab.domain.execution.command_lifecycle",
        "crypto_momentum_lab.domain.execution.command_codec",
        "crypto_momentum_lab.domain.execution.command_repository",
        "crypto_momentum_lab.domain.execution.reservation_repository",
        "crypto_momentum_lab.domain.execution.evidence_models",
        "crypto_momentum_lab.domain.execution.evidence_rules",
        "crypto_momentum_lab.domain.execution.evidence_settlement",
        "crypto_momentum_lab.domain.execution.fill_attribution",
        "crypto_momentum_lab.domain.execution.evidence_lifecycle",
        "crypto_momentum_lab.domain.execution.cumulative_report",
        "crypto_momentum_lab.domain.execution.evidence_grouping",
        "crypto_momentum_lab.domain.execution.observation_models",
        "crypto_momentum_lab.domain.execution.durable_evidence",
        "crypto_momentum_lab.domain.execution.projection_codec",
        "crypto_momentum_lab.domain.execution.ports",
        "crypto_momentum_lab.domain.execution.position_context_ports",
        "crypto_momentum_lab.domain.execution.position_repair_models",
        "crypto_momentum_lab.domain.execution.evidence_digest",
    ],
)
def test_application_modules_import_without_storage(module: str) -> None:
    script = """
import importlib
import sys
from importlib.abc import MetaPathFinder
class StorageGuard(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if (fullname == 'sqlalchemy'
                or fullname.startswith('crypto_momentum_lab.persistence')):
            raise RuntimeError('application imported storage: ' + fullname)
sys.meta_path.insert(0, StorageGuard())
importlib.import_module(sys.argv[1])
"""
    result = subprocess.run(
        [sys.executable, "-c", script, module],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "package, forbidden",
    [
        (
            "crypto_momentum_lab.operator_dashboard",
            "crypto_momentum_lab.operator_dashboard.api",
        ),
        (
            "crypto_momentum_lab.strategy_runner",
            "crypto_momentum_lab.strategy_runner.live_source",
        ),
    ],
)
def test_package_import_does_not_load_application_adapters(
    package: str,
    forbidden: str,
) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import importlib, sys; importlib.import_module(sys.argv[1]); "
            "assert sys.argv[2] not in sys.modules",
            package,
            forbidden,
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "module",
    [
        "crypto_momentum_lab.domain.execution",
        "crypto_momentum_lab.domain.execution.order_state",
        "crypto_momentum_lab.domain.execution.command_models",
        "crypto_momentum_lab.domain.execution.position_ledger_models",
        "crypto_momentum_lab.domain.execution.observation_models",
        "crypto_momentum_lab.domain.execution.recovery_models",
        "crypto_momentum_lab.domain.execution.projection_codec",
        "crypto_momentum_lab.domain.execution.ports",
        "crypto_momentum_lab.domain.execution.position_context_ports",
        "crypto_momentum_lab.domain.execution.position_repair_models",
        "crypto_momentum_lab.domain.execution.evidence_digest",
        "crypto_momentum_lab.domain.execution.account_journal",
        "crypto_momentum_lab.domain.execution.position_book",
    ],
)
def test_execution_values_import_without_coordination_stack(module: str) -> None:
    script = """
import importlib
import sys
from importlib.abc import MetaPathFinder
class CoordinationGuard(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {
            'crypto_momentum_lab.domain.execution.execution_book',
            'crypto_momentum_lab.domain.execution.execution_coordinator',
            'crypto_momentum_lab.domain.execution.recovery_codec',
        } or fullname.startswith('crypto_momentum_lab.persistence') or (
            fullname == 'sqlalchemy' or fullname.startswith('sqlalchemy.')
            or fullname.startswith('crypto_momentum_lab.live_rollout')
        ):
            raise RuntimeError('value imported execution stack: ' + fullname)
sys.meta_path.insert(0, CoordinationGuard())
importlib.import_module(sys.argv[1])
"""
    result = subprocess.run(
        [sys.executable, "-c", script, module],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "module",
    [
        "crypto_momentum_lab.live_rollout.resource_lifecycle",
        "crypto_momentum_lab.live_rollout",
        "crypto_momentum_lab.live_rollout.market_runtime_contracts",
        "crypto_momentum_lab.live_rollout.runtime_supervisor",
        "crypto_momentum_lab.live_rollout.runtime_session",
    ],
)
def test_runtime_contract_consumers_import_without_execution_loop(module: str) -> None:
    script = """
import importlib
import sys
from importlib.abc import MetaPathFinder
class LoopGuard(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {
            'crypto_momentum_lab.live_rollout.daemon',
            'crypto_momentum_lab.live_rollout.market_loop',
            'sqlalchemy',
        } or fullname.startswith((
            'crypto_momentum_lab.persistence',
            'crypto_momentum_lab.execution_account',
        )):
            raise RuntimeError('contract consumer imported execution: ' + fullname)
sys.meta_path.insert(0, LoopGuard())
importlib.import_module(sys.argv[1])
"""
    result = subprocess.run(
        [sys.executable, "-c", script, module],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "module",
    [
        "crypto_momentum_lab.live_rollout.account_channel",
        "crypto_momentum_lab.live_rollout.exit_channels",
    ],
)
def test_account_exit_consumers_do_not_load_daemon(module: str) -> None:
    script = """
import importlib
import sys
from importlib.abc import MetaPathFinder
class DaemonGuard(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'crypto_momentum_lab.live_rollout.daemon':
            raise RuntimeError('consumer imported daemon implementation')
sys.meta_path.insert(0, DaemonGuard())
importlib.import_module(sys.argv[1])
"""
    result = subprocess.run(
        [sys.executable, "-c", script, module],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "module",
    [
        "crypto_momentum_lab.live_rollout.exit_channels",
        "crypto_momentum_lab.live_rollout.exit_channel_ports",
    ],
)
def test_exit_consumers_do_not_load_market_sources(module: str) -> None:
    script = """
import importlib
import sys
from importlib.abc import MetaPathFinder
class MarketSourceGuard(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {
            'crypto_momentum_lab.market_data.quote_hub',
            'crypto_momentum_lab.live_rollout.closed_candle_feed',
        }:
            raise RuntimeError('exit consumer imported market source: ' + fullname)
sys.meta_path.insert(0, MarketSourceGuard())
importlib.import_module(sys.argv[1])
"""
    result = subprocess.run(
        [sys.executable, "-c", script, module],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "module",
    [
        "crypto_momentum_lab.live_rollout.account_channel",
        "crypto_momentum_lab.live_rollout.account_event_ports",
    ],
)
def test_account_consumers_do_not_load_order_reconciliation(module: str) -> None:
    script = """
import importlib
import sys
from importlib.abc import MetaPathFinder
class ReconciliationGuard(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'crypto_momentum_lab.live_rollout.order_reconciliation':
            raise RuntimeError('account consumer imported reconciliation: ' + fullname)
sys.meta_path.insert(0, ReconciliationGuard())
importlib.import_module(sys.argv[1])
"""
    result = subprocess.run(
        [sys.executable, "-c", script, module],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


def test_account_channel_does_not_load_unrelated_hubs() -> None:
    script = """
import importlib
import sys
from importlib.abc import MetaPathFinder
class UnrelatedHubGuard(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {
            'crypto_momentum_lab.market_data.hub',
            'crypto_momentum_lab.market_data.quote_hub',
            'crypto_momentum_lab.execution_account.risk_control_hub',
            'crypto_momentum_lab.live_rollout.exit_channels',
        }:
            raise RuntimeError('account channel imported unrelated hub: ' + fullname)
sys.meta_path.insert(0, UnrelatedHubGuard())
importlib.import_module('crypto_momentum_lab.live_rollout.account_channel')
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


def test_volume_consumer_does_not_load_quote_hub() -> None:
    script = """
import importlib
import sys
from importlib.abc import MetaPathFinder
class QuoteHubGuard(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'crypto_momentum_lab.market_data.quote_hub':
            raise RuntimeError('volume consumer imported quote hub: ' + fullname)
sys.meta_path.insert(0, QuoteHubGuard())
importlib.import_module('crypto_momentum_lab.live_rollout.volume')
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


def test_entry_expectation_registrar_does_not_load_account_hub() -> None:
    script = """
import importlib
import sys
from importlib.abc import MetaPathFinder
class AccountHubGuard(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'crypto_momentum_lab.execution_account.hub':
            raise RuntimeError('registrar imported account hub: ' + fullname)
sys.meta_path.insert(0, AccountHubGuard())
importlib.import_module('crypto_momentum_lab.live_rollout.entry_expectations')
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


def test_scheduled_controller_does_not_load_execution_assembly() -> None:
    script = """
import importlib
import sys
from importlib.abc import MetaPathFinder
class AssemblyGuard(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {
            'crypto_momentum_lab.execution_account.orders.coordinator',
            'crypto_momentum_lab.live_rollout.context',
        }:
            raise RuntimeError('scheduled controller imported assembly: ' + fullname)
sys.meta_path.insert(0, AssemblyGuard())
importlib.import_module('crypto_momentum_lab.live_rollout.scheduled_controller')
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


def test_decision_facts_does_not_load_execution_book_or_context() -> None:
    script = """
import importlib
import sys
from importlib.abc import MetaPathFinder
class FactGuard(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {
            'crypto_momentum_lab.domain.execution.execution_book',
            'crypto_momentum_lab.live_rollout.context',
        }:
            raise RuntimeError('decision facts imported implementation: ' + fullname)
sys.meta_path.insert(0, FactGuard())
importlib.import_module('crypto_momentum_lab.live_rollout.decision_facts')
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "module",
    [
        "crypto_momentum_lab.persistence.postgres",
        "crypto_momentum_lab.persistence.postgres.journal_store_ports",
        "crypto_momentum_lab.persistence.postgres.reservation_store_ports",
        "crypto_momentum_lab.persistence.postgres.command_store_ports",
        "crypto_momentum_lab.persistence.postgres.position_repair_ports",
        "crypto_momentum_lab.persistence.postgres.account_fact_rows",
    ],
)
def test_postgres_contract_imports_without_storage_implementations(module: str) -> None:
    script = """
import importlib
import sys
from importlib.abc import MetaPathFinder
class StorageGuard(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        allowed = {
            'crypto_momentum_lab.persistence.postgres',
            'crypto_momentum_lab.persistence.postgres.journal_store_ports',
            'crypto_momentum_lab.persistence.postgres.reservation_store_ports',
            'crypto_momentum_lab.persistence.postgres.command_store_ports',
            'crypto_momentum_lab.persistence.postgres.position_repair_ports',
            'crypto_momentum_lab.persistence.postgres.account_fact_rows',
        }
        if (fullname == 'sqlalchemy' or fullname.startswith('sqlalchemy.')
            or (fullname.startswith('crypto_momentum_lab.persistence.postgres.')
                and fullname not in allowed)
            or fullname in {
                'crypto_momentum_lab.domain.execution.execution_book',
                'crypto_momentum_lab.domain.execution.execution_coordinator',
                'crypto_momentum_lab.domain.execution.recovery_codec',
            }):
            raise RuntimeError('contract imported storage implementation: ' + fullname)
sys.meta_path.insert(0, StorageGuard())
importlib.import_module(sys.argv[1])
"""
    result = subprocess.run(
        [sys.executable, "-c", script, module],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "module",
    [
        "crypto_momentum_lab.execution_account.snapshot_models",
        "crypto_momentum_lab.execution_account.snapshot_changes",
        "crypto_momentum_lab.execution_account.sync_models",
        "crypto_momentum_lab.execution_account.sync_ports",
        "crypto_momentum_lab.execution_account.client_compat",
        "crypto_momentum_lab.execution_account.fill_progress",
        "crypto_momentum_lab.execution_account.balance_history",
        "crypto_momentum_lab.execution_account.position_history",
        "crypto_momentum_lab.execution_account.reconciliation_records",
    ],
)
def test_account_snapshot_imports_without_sync_service(module):
    script = """
import importlib
import sys
from importlib.abc import MetaPathFinder
class SyncGuard(MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if (fullname == 'crypto_momentum_lab.execution_account.sync'
            or fullname == 'sqlalchemy'
            or fullname.startswith('crypto_momentum_lab.persistence')):
            raise RuntimeError('snapshot imported service: ' + fullname)
sys.meta_path.insert(0, SyncGuard())
importlib.import_module(sys.argv[1])
"""
    result = subprocess.run(
        [sys.executable, "-c", script, module],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
