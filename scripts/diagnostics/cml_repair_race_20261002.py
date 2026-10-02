"""Offline regression replay for the 2026-10-02 incident; no exchange or database connection.

Run from the repository root with the project Python.
Historical failing observations are preserved in docs/diagnostics.
"""
import pytest

if __name__ == "__main__":
    raise SystemExit(pytest.main(['tests/unit/execution/test_repair_publication_race.py', '-q', '-k', 'repair_commit']))
