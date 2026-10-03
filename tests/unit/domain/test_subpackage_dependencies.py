from __future__ import annotations

import ast
import subprocess
import sys
from collections import defaultdict
from dataclasses import replace
from pathlib import Path
from typing import get_type_hints

from crypto_momentum_lab.domain.operational.runtime_metadata import RuntimePlanMetadata
from crypto_momentum_lab.domain.strategy.paper_models import PaperTradingRunReport


def _is_type_checking(node: ast.AST) -> bool:
    if isinstance(node, ast.Name) and node.id == "TYPE_CHECKING":
        return True
    if isinstance(node, ast.Attribute) and node.attr == "TYPE_CHECKING":
        return True
    return False


class _ImportVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.in_tc = False
        self.runtime_imports: set[str] = set()
        self.type_imports: set[str] = set()

    def visit_If(self, node: ast.If) -> None:
        if _is_type_checking(node.test):
            old = self.in_tc
            self.in_tc = True
            for stmt in node.body:
                self.visit(stmt)
            self.in_tc = old
            for stmt in node.orelse:
                self.visit(stmt)
        else:
            self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> None:
        target = self.type_imports if self.in_tc else self.runtime_imports
        for alias in node.names:
            target.add(alias.name)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        target = self.type_imports if self.in_tc else self.runtime_imports
        if node.module:
            target.add(node.module)


def _build_domain_dependency_graphs() -> tuple[
    dict[str, set[str]], dict[str, set[str]]
]:
    domain_dir = (
        Path(__file__).resolve().parents[3] / "src" / "crypto_momentum_lab" / "domain"
    )
    subpackages = sorted(
        d.name
        for d in domain_dir.iterdir()
        if d.is_dir() and not d.name.startswith(("_", "."))
    )

    runtime_graph: dict[str, set[str]] = defaultdict(set)
    all_graph: dict[str, set[str]] = defaultdict(set)

    for sp in subpackages:
        sp_dir = domain_dir / sp
        for py_file in sp_dir.rglob("*.py"):
            with open(py_file, encoding="utf-8") as f:
                tree = ast.parse(f.read(), filename=str(py_file))
            visitor = _ImportVisitor()
            visitor.visit(tree)

            for imp in visitor.runtime_imports:
                if imp.startswith("crypto_momentum_lab.domain."):
                    parts = imp.split(".")
                    if len(parts) >= 3:
                        target_sp = parts[2]
                        if target_sp in subpackages and target_sp != sp:
                            runtime_graph[sp].add(target_sp)
                            all_graph[sp].add(target_sp)

            for imp in visitor.type_imports:
                if imp.startswith("crypto_momentum_lab.domain."):
                    parts = imp.split(".")
                    if len(parts) >= 3:
                        target_sp = parts[2]
                        if target_sp in subpackages and target_sp != sp:
                            all_graph[sp].add(target_sp)

    return dict(runtime_graph), dict(all_graph)


def _find_elementary_cycles(graph: dict[str, set[str]]) -> list[list[str]]:
    cycles: list[list[str]] = []
    nodes = sorted(list(set(graph.keys()) | {v for vs in graph.values() for v in vs}))

    def dfs(start: str, current: str, path: list[str], visited: set[str]) -> None:
        for nxt in sorted(graph.get(current, set())):
            if nxt == start:
                cycles.append(path + [start])
            elif nxt > start and nxt not in visited:
                visited.add(nxt)
                dfs(start, nxt, path + [nxt], visited)
                visited.remove(nxt)

    for start in nodes:
        dfs(start, start, [start], {start})
    return cycles


def test_domain_runtime_and_type_subpackage_graphs_have_zero_cycles() -> None:
    runtime_graph, all_graph = _build_domain_dependency_graphs()

    runtime_cycles = _find_elementary_cycles(runtime_graph)
    assert runtime_cycles == [], (
        f"Domain runtime subpackages contain cycles: {runtime_cycles}"
    )

    all_cycles = _find_elementary_cycles(all_graph)
    assert all_cycles == [], f"Domain type dependencies contain cycles: {all_cycles}"


def test_dependency_scanner_distinguishes_runtime_and_type_imports() -> None:
    visitor = _ImportVisitor()
    visitor.visit(
        ast.parse(
            "from crypto_momentum_lab.domain.strategy import OrderIntentCandidate\n"
            "if TYPE_CHECKING:\n"
            "    from crypto_momentum_lab.domain.runtime import RuntimePlan\n"
            "else:\n"
            "    import crypto_momentum_lab.domain.account\n"
        )
    )
    assert visitor.runtime_imports == {
        "crypto_momentum_lab.domain.strategy",
        "crypto_momentum_lab.domain.account",
    }
    assert visitor.type_imports == {"crypto_momentum_lab.domain.runtime"}
    assert _find_elementary_cycles({"a": {"b"}, "b": {"a"}}) == [["a", "b", "a"]]


def test_paper_models_clean_subprocess_import_isolation() -> None:
    script = (
        "import sys\n"
        "from crypto_momentum_lab.domain.strategy.paper_models "
        "import PaperTradingRunReport\n"
        "assert 'crypto_momentum_lab.domain.runtime.runtime_plan' "
        "not in sys.modules\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"Import isolation failed: {result.stderr}"


def test_paper_trading_run_report_runtime_plan_annotation() -> None:
    assert get_type_hints(PaperTradingRunReport)["runtime_plan"] == (
        RuntimePlanMetadata | None
    )


def test_paper_report_preserves_concrete_plan_and_persisted_artifacts() -> None:
    from crypto_momentum_lab.domain.runtime.runtime_plan import RuntimePlanCompiler
    from crypto_momentum_lab.persistence.postgres.strategy_run_repository import (
        strategy_run_report_rows,
    )
    from tests.unit.persistence.postgres.test_strategy_run_repository import (
        fixture_paper_report,
    )

    plan: RuntimePlanMetadata = RuntimePlanCompiler.compile(
        environment="paper",
        account_label="account-1",
    )
    base = fixture_paper_report()
    report = replace(base, runtime_plan=plan)
    assert report.runtime_plan is plan
    assert strategy_run_report_rows(report) == strategy_run_report_rows(base)
