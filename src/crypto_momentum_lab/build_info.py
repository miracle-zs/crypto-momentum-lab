from __future__ import annotations

import os
import subprocess
from pathlib import Path

_INVALID_COMMIT_VALUES = frozenset({"", "unknown", "unversioned"})


def resolve_code_commit(
    *,
    required: bool = True,
    git_root: Path | None = None,
) -> str:
    """Resolve the immutable code revision recorded in an operational run."""
    configured = os.environ.get("CML_CODE_COMMIT", "").strip()
    if configured not in _INVALID_COMMIT_VALUES:
        _validate_commit(configured)
        return configured

    discovered = _discover_git_commit(git_root)
    if discovered is not None:
        return discovered
    if required:
        raise RuntimeError(
            "CML_CODE_COMMIT must be set to a deployed git commit "
            "when the runtime is not inside a git checkout"
        )
    return "unknown"


def _discover_git_commit(git_root: Path | None) -> str | None:
    root = git_root if git_root is not None else _find_git_root(Path.cwd())
    if root is None or not (root / ".git").exists():
        return None
    result = subprocess.run(
        ["git", "rev-parse", "--verify", "HEAD"],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    commit = result.stdout.strip()
    if commit in _INVALID_COMMIT_VALUES:
        return None
    _validate_commit(commit)
    return commit


def _find_git_root(start: Path) -> Path | None:
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def _validate_commit(commit: str) -> None:
    if not commit or len(commit) > 64 or any(char.isspace() for char in commit):
        raise RuntimeError(
            "CML_CODE_COMMIT must be a non-empty value of at most 64 characters"
        )
