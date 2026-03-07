@echo off
setlocal enabledelayedexpansion

set PROJECT_DIR=C:\Users\drewa\Desktop\stfc\stfc_stat_tracker
set PYTHON_EXE=C:\Users\drewa\AppData\Local\Programs\Python\Python311\python.exe
set LOG_FILE=%PROJECT_DIR%\run_daily.log

echo ============================================ >> "%LOG_FILE%"
echo Run started: %date% %time% >> "%LOG_FILE%"
echo ============================================ >> "%LOG_FILE%"

cd /d "%PROJECT_DIR%"

echo Running API puller... >> "%LOG_FILE%"
"%PYTHON_EXE%" pull_api.py >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! neq 0 (
    echo ERROR: Scraper failed with exit code !ERRORLEVEL! >> "%LOG_FILE%"
    goto :end
)

REM Disabled — Discord bot handles alerts and daily report now
REM "%PYTHON_EXE%" send_hourly_alerts.py >> "%LOG_FILE%" 2>&1
REM "%PYTHON_EXE%" send_discord_notification.py >> "%LOG_FILE%" 2>&1

:end
echo Run finished: %date% %time% >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"
endlocal
