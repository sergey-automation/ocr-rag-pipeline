@echo off
chcp 65001 >nul
setlocal

REM === CONFIG ===
set "SCRIPT=chunks_analyze_from_full.py"
set "IN=C:\LIB1\_chunks\chunks_full.jsonl"
set "OUT=C:\LIB1\_chunks\chunks_analiz.jsonl"

REM === RUN ===
echo ==========================================
echo RUN CHUNKS ANALYZE (COMPACT MODE)
echo ==========================================
echo Input : %IN%
echo Output: %OUT%
echo ==========================================

python "%SCRIPT%" ^
  --chunks-in "%IN%" ^
  --chunks-out "%OUT%" ^
  --lines-format compact ^
  --report-every 1000

if errorlevel 1 (
    echo.
    echo ERROR: script failed
    pause
    exit /b 1
)

echo.
echo DONE
pause
