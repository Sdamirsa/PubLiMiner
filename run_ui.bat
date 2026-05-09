@echo off
REM Launch the PubLiMiner Streamlit UI.
REM Double-click this file or run from any cmd prompt.
REM
REM Goes through the CLI entry point so the installed package's app.py is
REM used — works in both editable (uv sync) and installed (uv tool install) modes.

cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
uv run publiminer ui
pause
