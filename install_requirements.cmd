@echo off
setlocal
cd /d "%~dp0"
echo Installing from %cd%\requirements.txt using:
where python 2>nul
python -c "import struct; print('Python', struct.calcsize('P')*8, 'bit')"
echo.
python -m pip install -r requirements.txt
if errorlevel 1 exit /b 1
echo.
echo Done. Re-run: python access_sync\customer_services_sync.py  (from OptimaFlow root, or adjust path)
exit /b 0
