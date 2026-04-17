"""Tests for the first-run setup wizard.

Uses ``typer.testing.CliRunner`` so we can pipe stdin answers through the
interactive wizard without needing a real TTY. Tmp-path chdir keeps every
test in an isolated directory — no global ``.env`` bleed-through.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from publiminer.cli import app
from publiminer.commands.setup import (
    ENV_BYPASS_VAR,
    ensure_gitignored,
    env_is_complete,
    read_env_values,
    scaffold_yaml,
    wizard_should_run,
    write_env,
)

runner = CliRunner()


# ── Predicates ─────────────────────────────────────────────────────────


def test_env_is_complete_false_when_missing(tmp_path: Path) -> None:
    assert env_is_complete(tmp_path) is False


def test_env_is_complete_false_when_email_blank(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("PUBMED_EMAIL=\n", encoding="utf-8")
    assert env_is_complete(tmp_path) is False


def test_env_is_complete_true_with_email(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("PUBMED_EMAIL=user@example.com\n", encoding="utf-8")
    assert env_is_complete(tmp_path) is True


def test_wizard_should_run_respects_bypass(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    # No .env → would normally run
    assert wizard_should_run() is True
    monkeypatch.setenv(ENV_BYPASS_VAR, "1")
    assert wizard_should_run() is False


# ── Writers ────────────────────────────────────────────────────────────


def test_write_env_creates_file(tmp_path: Path) -> None:
    path = write_env(tmp_path, email="me@test.com", api_key="abc123")
    assert path.exists()
    values = read_env_values(tmp_path)
    assert values["PUBMED_EMAIL"] == "me@test.com"
    assert values["NCBI_API_KEY"] == "abc123"


def test_write_env_preserves_existing_keys(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text(
        "OPENROUTER_API_KEY=existing_secret\nOTHER=keep_me\n", encoding="utf-8"
    )
    write_env(tmp_path, email="me@test.com", api_key="abc123")
    values = read_env_values(tmp_path)
    assert values["OPENROUTER_API_KEY"] == "existing_secret"
    assert values["OTHER"] == "keep_me"
    assert values["PUBMED_EMAIL"] == "me@test.com"


def test_write_env_is_idempotent(tmp_path: Path) -> None:
    write_env(tmp_path, email="a@b.com", api_key="key1")
    write_env(tmp_path, email="a@b.com", api_key="key1")  # same values again
    # File still has exactly one entry per key (no duplication).
    content = (tmp_path / ".env").read_text(encoding="utf-8")
    assert content.count("PUBMED_EMAIL") == 1
    assert content.count("NCBI_API_KEY") == 1


def test_ensure_gitignored_appends(tmp_path: Path) -> None:
    gi = tmp_path / ".gitignore"
    gi.write_text("__pycache__/\n*.pyc\n", encoding="utf-8")
    assert ensure_gitignored(tmp_path) is True
    assert ".env" in gi.read_text(encoding="utf-8")


def test_ensure_gitignored_skips_when_present(tmp_path: Path) -> None:
    gi = tmp_path / ".gitignore"
    gi.write_text("__pycache__/\n.env\n", encoding="utf-8")
    assert ensure_gitignored(tmp_path) is False


def test_ensure_gitignored_skips_when_no_gitignore(tmp_path: Path) -> None:
    # No .gitignore → don't create one (might be an intentional omission)
    assert ensure_gitignored(tmp_path) is False


def test_scaffold_yaml_creates(tmp_path: Path) -> None:
    path = scaffold_yaml(tmp_path)
    assert path is not None
    assert path.exists()
    content = path.read_text(encoding="utf-8")
    assert "PubLiMiner" in content
    assert "query:" in content


def test_scaffold_yaml_respects_existing(tmp_path: Path) -> None:
    (tmp_path / "publiminer.yaml").write_text("custom: yes\n", encoding="utf-8")
    assert scaffold_yaml(tmp_path) is None
    # Existing file untouched.
    assert (tmp_path / "publiminer.yaml").read_text(encoding="utf-8") == "custom: yes\n"


# ── CLI integration ────────────────────────────────────────────────────


def test_cli_setup_writes_everything(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Drive the full wizard end-to-end via stdin piping."""
    monkeypatch.chdir(tmp_path)
    # Inputs in order:
    #   email, has-key-prompt (y), api_key, scaffold-yaml (Y), launch-ui (n)
    stdin = "me@example.com\ny\nsk-test-abc123\nY\nn\n"
    result = runner.invoke(app, ["setup"], input=stdin)
    assert result.exit_code == 0, result.stdout
    assert (tmp_path / ".env").exists()
    values = read_env_values(tmp_path)
    assert values["PUBMED_EMAIL"] == "me@example.com"
    assert values["NCBI_API_KEY"] == "sk-test-abc123"
    assert (tmp_path / "publiminer.yaml").exists()


def test_cli_setup_skip_api_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Wizard must accept 'no' for the API key and save a blank value."""
    monkeypatch.chdir(tmp_path)
    # Inputs: email, has-key-prompt (N), scaffold-yaml (Y), launch-ui (n)
    stdin = "me@example.com\nn\nY\nn\n"
    result = runner.invoke(app, ["setup"], input=stdin)
    assert result.exit_code == 0, result.stdout
    values = read_env_values(tmp_path)
    assert values["PUBMED_EMAIL"] == "me@example.com"
    # Empty key saved as empty string (explicit skip) — env_is_complete still True.
    assert env_is_complete(tmp_path) is True


def test_cli_setup_no_force_when_complete(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Re-running setup without --force on a complete env should no-op gracefully."""
    monkeypatch.chdir(tmp_path)
    write_env(tmp_path, email="existing@test.com", api_key="existing_key")
    # No input needed — wizard should early-return with a message.
    result = runner.invoke(app, ["setup"], input="")
    assert result.exit_code == 0, result.stdout
    # Value unchanged.
    assert read_env_values(tmp_path)["PUBMED_EMAIL"] == "existing@test.com"


def test_run_skips_wizard_with_bypass_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """PUBLIMINER_NO_WIZARD=1 must bypass the auto-trigger in `publiminer run`."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(ENV_BYPASS_VAR, "1")
    # No .env, no config, no stdin — run should try to load config (and probably
    # fail because no query is set) but NOT prompt for wizard input.
    result = runner.invoke(app, ["run", "--no-setup"], input="")
    # We don't assert success here — just that it didn't hang on a prompt.
    # (A hang would exceed CliRunner's default timeout and fail the test.)
    assert "email" not in result.stdout.lower() or "error" in result.stdout.lower()
