@echo off
REM Start the auth.json watcher in the background (no console window with pythonw)
REM Schedule this via Task Scheduler: trigger=At logon, action=this bat file
start "" /B pythonw "C:\Users\drewa\Desktop\stfc\stfc_stat_tracker\deploy\watch_auth.py"
