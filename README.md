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

## Structure (planned)
