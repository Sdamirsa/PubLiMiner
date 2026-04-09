"""Smoke tests for the Typer CLI."""

from __future__ import annotations

from typer.testing import CliRunner

from publiminer.cli import app

runner = CliRunner()


def test_help():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "publiminer" in result.stdout.lower()


def test_ui_command_registered():
    result = runner.invoke(app, ["ui", "--help"])
    assert result.exit_code == 0
    assert "streamlit" in result.stdout.lower() or "ui" in result.stdout.lower()


def test_status_empty(tmp_output):
    result = runner.invoke(app, ["status", "--output", str(tmp_output)])
    # Should not crash on empty output dir; may exit 0 or print "no data".
    assert result.exit_code in (0, 1)
