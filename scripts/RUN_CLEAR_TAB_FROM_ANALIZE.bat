@echo off
chcp 65001 >nul
setlocal

set "LIB=C:\LIB1"
set "SCRIPTS=C:\LIB1\_scripts"
set "PY=%SCRIPTS%\clear_tab_from_analize.py"

set "IN=%LIB%\_chunks\chunks_analiz.jsonl"
set "OUT=%LIB%\_chunks\chunks_cleaned_full.jsonl"
set "SUMMARY_JSON=%LIB%\_chunks\clean_chunks_full_summary.json"
set "SUMMARY_TXT=%LIB%\_chunks\clean_chunks_full_report.txt"
set "MIN_WORD_LEN=3"

python "%PY%" ^
  --chunks "%IN%" ^
  --out "%OUT%" ^
  --summary-json "%SUMMARY_JSON%" ^
  --summary-txt "%SUMMARY_TXT%" ^
  --min-word-len %MIN_WORD_LEN% ^
  --report-every 2000

if errorlevel 1 (
  echo.
  echo [ERROR] clear_tab_from_analiz.py failed
  pause
  exit /b 1
)

echo.
echo Done.
echo Output      : %OUT%
echo Summary JSON: %SUMMARY_JSON%
echo Summary TXT : %SUMMARY_TXT%
pause
