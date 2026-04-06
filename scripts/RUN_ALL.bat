@echo off
chcp 65001 >nul

REM ===== PATHS =====
set "LIB=C:\LIB1"

REM ===== STEP 1: MAKE MANIFEST =====
python scripts\make_manifest.py --lib "%LIB%"

REM ===== STEP 2: MAKE CHUNKS FULL =====
python scripts\make_chunks_full.py --lib "%LIB%"

REM ===== STEP 3: ANALYZE =====
python scripts\chunks_analyze_from_full.py ^
  --chunks-in "%LIB%\_chunks\chunks_full.jsonl" ^
  --chunks-out "%LIB%\_chunks\chunks_analiz.jsonl"

REM ===== STEP 4: CLEAR TABLES =====
python scripts\clear_tab_from_analyze.py ^
  --chunks-in "%LIB%\_chunks\chunks_analiz.jsonl" ^
  --chunks-out "%LIB%\_chunks\chunks_cleaned_full.jsonl"

REM ===== STEP 5: SPLIT TO 512 =====
python scripts\make_chunks_512_from_cleaned.py ^
  --chunks-in "%LIB%\_chunks\chunks_cleaned_full.jsonl" ^
  --chunks-out "%LIB%\_chunks\chunks_512.jsonl"

echo.
echo DONE
pause
