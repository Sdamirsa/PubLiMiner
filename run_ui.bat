@echo off
REM Launch the PubLiMiner Streamlit UI.
REM Double-click this file or run from any cmd prompt.

cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
py -3.11 -m streamlit run src\publiminer\ui\app.py
pause
