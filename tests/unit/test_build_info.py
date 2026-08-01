from pathlib import Path

from crypto_momentum_lab.build_info import resolve_code_commit


def test_resolve_code_commit_prefers_explicit_build_value(monkeypatch) -> None:
    monkeypatch.setenv("CML_CODE_COMMIT", "abc1234")

    assert resolve_code_commit() == "abc1234"


def test_resolve_code_commit_rejects_unknown_build_value(monkeypatch) -> None:
    monkeypatch.setenv("CML_CODE_COMMIT", "unknown")

    try:
        resolve_code_commit(git_root=Path("/tmp/not-a-git-checkout"))
    except RuntimeError as error:
        assert "CML_CODE_COMMIT" in str(error)
    else:
        raise AssertionError("unknown build value must not be accepted")
