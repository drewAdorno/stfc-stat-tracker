@echo off
setlocal enabledelayedexpansion

set PROJECT_DIR=C:\Users\drewa\Desktop\stfc_stat_tracker
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

echo Committing data... >> "%LOG_FILE%"
git add data\*.json >> "%LOG_FILE%" 2>&1
git commit -m "Alliance data update %date% %time:~0,5%" >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! neq 0 (
    echo Nothing new to commit or commit failed. >> "%LOG_FILE%"
)

echo Pushing to GitHub... >> "%LOG_FILE%"
git push origin master >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! neq 0 (
    echo Push failed, trying main branch... >> "%LOG_FILE%"
    git push origin main >> "%LOG_FILE%" 2>&1
)

echo Sending hourly alerts... >> "%LOG_FILE%"
"%PYTHON_EXE%" send_hourly_alerts.py >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! neq 0 (
    echo WARNING: Hourly alerts failed >> "%LOG_FILE%"
)

echo Sending Discord notification... >> "%LOG_FILE%"
"%PYTHON_EXE%" send_discord_notification.py >> "%LOG_FILE%" 2>&1
if !ERRORLEVEL! neq 0 (
    echo WARNING: Discord notification failed >> "%LOG_FILE%"
)

:end
echo Run finished: %date% %time% >> "%LOG_FILE%"
echo. >> "%LOG_FILE%"
endlocal
