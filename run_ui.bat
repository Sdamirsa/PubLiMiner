@echo off
REM Launch the PubLiMiner Streamlit UI.
REM Double-click this file or run from any cmd prompt.
REM
REM Uses ``uv run`` to invoke the project's managed venv (Python 3.14).
REM The previous ``py -3.11`` invocation broke after the move to uv —
REM matches the same fix already applied to run_nightly.bat (a884b90).

cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
uv run streamlit run src\publiminer\ui\app.py
pause
