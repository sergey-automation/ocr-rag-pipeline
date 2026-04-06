@echo off
chcp 65001 >nul
setlocal

set "SCRIPTS_DIR=%~dp0"
set "SCRIPTS_DIR=%SCRIPTS_DIR:~0,-1%"
for %%I in ("%SCRIPTS_DIR%\..") do set "LIB_ROOT=%%~fI"
set "SCAN_ROOT=%LIB_ROOT%\OCR_TXT"
set "PY=python"

echo ==========================================
echo LIB ROOT : %LIB_ROOT%
echo SCAN ROOT: %SCAN_ROOT%
echo SCRIPTS  : %SCRIPTS_DIR%
echo ==========================================
echo.

echo [RUN] make_manifest.py
%PY% "%SCRIPTS_DIR%\make_manifest.py" ^
  --lib-root "%LIB_ROOT%" ^
  --out "%LIB_ROOT%\manifest.jsonl" ^
  --output-dir "%LIB_ROOT%\metadata"

if errorlevel 1 (
  echo.
  echo [ERROR] make_manifest.py failed
  pause
  exit /b 1
)

echo.
echo === DONE ===
pause
