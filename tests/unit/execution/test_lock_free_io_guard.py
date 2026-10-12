"""I2 guard: no I/O is awaited while a domain mutation lock is held.

The book's per-key lock exists for in-memory work only: staging a candidate
(consistent read) and publishing an already-committed candidate. Holding it
across an ``await`` would serialize network/database I/O behind it, so this
guard asserts the invariant structurally instead of trusting review.
"""

import ast
import pathlib

DOMAIN_EXECUTION = pathlib.Path("src/crypto_momentum_lab/domain/execution")
LOCK_ATTRIBUTES = ("_mutation_lock", "_process_mutation_lock")


def _lock_attribute(node: ast.expr) -> str | None:
    """Return the lock attribute a context expression acquires, if any."""
    inner = node.func if isinstance(node, ast.Call) else node
    if isinstance(inner, ast.Attribute) and inner.attr in LOCK_ATTRIBUTES:
        return inner.attr
    return None


def _lock_sections(path: pathlib.Path) -> list[tuple[int, str, list[str]]]:
    tree = ast.parse(path.read_text())
    sections: list[tuple[int, str, list[str]]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncWith):
            continue
        for item in node.items:
            attribute = _lock_attribute(item.context_expr)
            if attribute is None:
                continue
            awaits = [
                ast.unparse(child.value)
                for child in ast.walk(node)
                if isinstance(child, ast.Await)
            ]
            sections.append((node.lineno, attribute, awaits))
    return sections


def test_no_io_is_awaited_under_a_domain_lock() -> None:
    sections: list[tuple[str, int, str, list[str]]] = []
    for path in sorted(DOMAIN_EXECUTION.rglob("*.py")):
        sections.extend(
            (path.name, line, attribute, awaits)
            for line, attribute, awaits in _lock_sections(path)
        )

    offenders = [
        f"{name}:{line} holds {attribute} across await {awaited}"
        for name, line, attribute, awaits in sections
        for awaited in awaits
    ]
    assert offenders == []
