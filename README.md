# OCR RAG Pipeline

Industrial-grade tools for processing OCR text and preparing datasets for RAG systems.

## Overview

This repository contains a set of Python utilities designed for:

- Cleaning OCR text (noise, broken symbols, encoding issues)
- Chunking large documents into structured segments
- Detecting low-quality or corrupted chunks
- Preparing datasets for embedding and retrieval systems

The tools are optimized for large technical corpora (hundreds of thousands to millions of chunks).

---

## Features

- High-speed chunk processing (CPU optimized)
- OCR noise filtering and normalization
- Chunk quality analysis (junk / suspicious detection)
- Statistics and dataset validation
- Designed for integration with vector search systems (FAISS, HNSW, etc.)

---

## Use Cases

- Technical book indexing
- Industrial documentation processing
- RAG pipelines for engineering knowledge bases
- Large-scale OCR dataset cleanup

---

## Structure

```text
scripts/
  make_manifest.py
  make_chunks_full.py
  chunks_analyze_from_full.py
  clear_tab_from_analyze.py
  make_chunks_512_from_cleaned.py
  RUN_ALL.bat
README.md
```
## Quick Start

RUN_ALL.bat

## Pipeline

1. `make_manifest.py` — builds `manifest.jsonl`
2. `make_chunks_full.py` — creates page-based chunks
3. `chunks_analyze_from_full.py` — analyzes chunk quality
4. `clear_tab_from_analyze.py` — cleans noisy/table-heavy chunks
5. `make_chunks_512_from_cleaned.py` — splits cleaned chunks into smaller parts
6. `RUN_ALL.bat` — runs the full pipeline
