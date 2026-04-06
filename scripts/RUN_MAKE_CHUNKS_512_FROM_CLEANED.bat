@echo off
chcp 65001 >nul
setlocal

set "PY=python"
set "LIB=C:\LIB1"
set "IN=%LIB%\_chunks\chunks_cleaned_full.jsonl"
set "OUT=%LIB%\_chunks\chunks_512.jsonl"
set "MAX_WORDS=222"
set "OVERLAP_WORDS=10"
set "REPORT_EVERY=1000"

%PY% "%~dp0make_chunks_512_from_cleaned.py" ^
  --chunks-in "%IN%" ^
  --chunks-out "%OUT%" ^
  --max-words %MAX_WORDS% ^
  --overlap-words %OVERLAP_WORDS% ^
  --report-every %REPORT_EVERY%

if errorlevel 1 (
  echo.
  echo [ERROR] make_chunks_512_from_cleaned.py failed
  pause
  exit /b 1
)

echo.
echo DONE
pause
