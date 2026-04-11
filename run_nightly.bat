@echo off
REM PubLiMiner nightly runner — resumable, idempotent.
REM Output is tee'd to BOTH the console and nightly.log, so you see progress
REM live when launched from Code Runner / a terminal AND have a full log
REM when launched headless by Task Scheduler.

cd /d %~dp0
set PYTHONIOENCODING=utf-8
set PYTHONUNBUFFERED=1
set PUBLIMINER_PROGRESS=log

echo. >> nightly.log
echo ================================================================ >> nightly.log
echo [%date% %time%] Starting PubLiMiner nightly run >> nightly.log
echo ================================================================ >> nightly.log
echo [%date% %time%] Starting PubLiMiner nightly run

powershell -NoProfile -Command "$OutputEncoding = [System.Text.Encoding]::UTF8; [Console]::OutputEncoding = [System.Text.Encoding]::UTF8; uv run publiminer run --config publiminer.yaml 2>&1 | ForEach-Object { $_; Add-Content -Path nightly.log -Value $_ -Encoding utf8 }"

echo [%date% %time%] Done (exit %errorlevel%)
echo [%date% %time%] Done (exit %errorlevel%) >> nightly.log
