@echo off
chcp 65001 >nul
setlocal

set "SCRIPT_DIR=%~dp0"
set "PY_SCRIPT=%SCRIPT_DIR%MAKE_CHUNKS_FULL.py"

if not exist "%PY_SCRIPT%" (
    echo ERROR: Python script not found:
    echo %PY_SCRIPT%
    exit /b 1
)

set "LIB=%~1"
if "%LIB%"=="" set "LIB=C:\LIB1"

set "MANIFEST=%~2"
if "%MANIFEST%"=="" set "MANIFEST=manifest.jsonl"

set "OUT=%~3"
if "%OUT%"=="" set "OUT=_chunks\chunks_full.jsonl"

set "MIN_CHARS=%~4"
if "%MIN_CHARS%"=="" set "MIN_CHARS=20"

set "REPORT_EVERY=%~5"
if "%REPORT_EVERY%"=="" set "REPORT_EVERY=100"

echo LIB=%LIB%
echo MANIFEST=%MANIFEST%
echo OUT=%OUT%
echo MIN_CHARS=%MIN_CHARS%
echo REPORT_EVERY=%REPORT_EVERY%
echo.

python "%PY_SCRIPT%" ^
  --lib "%LIB%" ^
  --manifest "%MANIFEST%" ^
  --out "%OUT%" ^
  --min-chars %MIN_CHARS% ^
  --report-every %REPORT_EVERY%

set "ERR=%ERRORLEVEL%"
echo.
echo Exit code: %ERR%
exit /b %ERR%
