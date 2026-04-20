@echo off
setlocal
if "%~1"=="" (
  echo Usage: install_sync_jobs.cmd "C:\Full\Path\To\OptimaFlow"
  echo Copies %%~dp0sync_jobs into OptimaFlow\sync_jobs ^(mirrors destination^).
  exit /b 1
)
set "SRC=%~dp0sync_jobs"
set "DST=%~1"
if not exist "%SRC%\spec_types.py" (
  echo ERROR: sync_jobs not found next to this script: %SRC%
  exit /b 2
)
robocopy "%SRC%" "%DST%\sync_jobs" /MIR /NFL /NDL /NJH /NJS
if errorlevel 8 exit /b %ERRORLEVEL%
echo Updated: %DST%\sync_jobs
exit /b 0
