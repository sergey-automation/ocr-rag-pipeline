# OCR RAG Pipeline

Industrial-grade tools for processing OCR text and preparing datasets for RAG systems.

## Overview

This repository contains a set of Python utilities designed for:

- Cleaning OCR text (noise, broken symbols, encoding issues)
- Chunking large documents into structured segments
- Detecting low-quality or corrupted chunks
- Preparing datasets for embedding and retrieval systems
- Designed for large-scale corpora (100K–10M+ chunks).
- Tested on real industrial OCR datasets.
The tools are optimized for large technical corpora (hundreds of thousands to millions of chunks).

## Requirements

- Python 3.10+

## Features

- High-speed chunk processing (CPU optimized)
- OCR noise filtering and normalization
- Chunk quality analysis (junk / suspicious detection)
- Statistics and dataset validation
- Designed for integration with vector search systems (FAISS, HNSW, etc.)

## Use Cases

- Technical book indexing
- Industrial documentation processing
- RAG pipelines for engineering knowledge bases
- Large-scale OCR dataset cleanup

## Structure

```text
scripts/
  RUN_ALL.bat
  RUN_MAKE_MANIFEST.bat
  RUN_MAKE_CHUNKS_FULL.bat
  RUN_CHUNKS_ANALYZE_FULL.bat
  RUN_CLEAR_TAB_FROM_ANALIZE.bat
  RUN_MAKE_CHUNKS_512_FROM_CLEANED.bat
  make_manifest.py
  make_chunks_full.py
  chunks_analyze_from_full.py
  clear_tab_from_analyze.py
  make_chunks_512_from_cleaned.py
```
## Step-by-step execution

1. Build manifest:`scripts/RUN_MAKE_MANIFEST.bat`
2. Create chunks:`scripts/RUN_MAKE_CHUNKS_FULL.bat`
3. Analyze chunks:`scripts/RUN_CHUNKS_ANALYZE_FULL.bat`
4. Clean noisy/table chunks:`scripts/RUN_CLEAR_TAB_FROM_ANALIZE.bat`
5. Split into 512 chunks:`scripts/RUN_MAKE_CHUNKS_512_FROM_CLEANED.bat`

## Quick Start
Run full pipeline:

`scripts/RUN_ALL.bat`

## Input / Output

Input:
- OCR text files (.txt)

Output:
- manifest.jsonl
- chunks_full.jsonl
- chunks_cleaned.jsonl
- chunks_512.jsonl

## Pipeline

1. `make_manifest.py` — builds `manifest.jsonl`
2. `make_chunks_full.py` — creates page-based chunks
3. `chunks_analyze_from_full.py` — analyzes chunk quality
4. `clear_tab_from_analyze.py` — cleans noisy/table-heavy chunks
5. `make_chunks_512_from_cleaned.py` — splits cleaned chunks into smaller parts

## Example

Input:
C:\LIB1\OCR_TXT

Run:
C:\LIB1\scripts\RUN_ALL.bat

Output:

C:\LIB1\
 - manifest.jsonl
   
C:\LIB1\metadata\
- manifest reports and diagnostics

C:\LIB1\_chunks\
 - chunks_full.jsonl
 - chunks_cleaned_full.jsonl
 - chunks_512.jsonl
  
## Performance

 (CPU i5-4570)
 
- Chunk generation: ~4200 chunks/sec
- Chunk analysis: ~440 chunks/sec (main bottleneck)
- Chunk splitting (<512): ~6600 chunks/sec
- Dataset size: ~2.3M pages (~2.3M chunks input)
