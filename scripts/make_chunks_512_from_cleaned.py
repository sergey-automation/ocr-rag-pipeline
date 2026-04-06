# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

PROGRAM = "MAKE_CHUNKS_512_FROM_CLEANED"
VERSION = "2026-04-05 v1.2"


def whitespace_words(text: str) -> List[str]:
    # Быстрая разбивка текста на слова без использования токенизатора.
    return text.split()


def choose_parts(word_count: int, max_words: int, overlap_words: int) -> int:
    # Выбираем минимальное количество частей,
    # чтобы каждая часть с учётом overlap влезала в max_words.
    if word_count <= max_words:
        return 1

    k = 2
    while True:
        max_core = math.ceil(word_count / k)
        if max_core + overlap_words <= max_words:
            return k
        k += 1


def balanced_ranges(word_count: int, parts: int) -> List[Tuple[int, int]]:
    # Делим страницу на примерно равные непрерывные диапазоны слов.
    base = word_count // parts
    rem = word_count % parts
    ranges: List[Tuple[int, int]] = []
    start = 0
    for i in range(parts):
        size = base + (1 if i < rem else 0)
        end = start + size
        ranges.append((start, end))
        start = end
    return ranges


def build_subchunks(words: List[str], max_words: int, overlap_words: int) -> List[str]:
    # Формируем итоговые чанки с перекрытием между соседними частями.
    n = len(words)
    if n == 0:
        return []

    parts = choose_parts(n, max_words, overlap_words)
    ranges = balanced_ranges(n, parts)
    out: List[str] = []

    for i, (start, end) in enumerate(ranges):
        if i == 0:
            chunk_words = words[start:end]
        else:
            prev_start, prev_end = ranges[i - 1]
            tail_start = max(prev_start, prev_end - overlap_words)
            chunk_words = words[tail_start:prev_end] + words[start:end]

        if len(chunk_words) > max_words:
            raise RuntimeError(
                f"Internal error: chunk size {len(chunk_words)} > max_words {max_words}"
            )
        out.append(" ".join(chunk_words))

    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunks-in", required=True, help="Input chunks_cleaned_full.jsonl")
    ap.add_argument("--chunks-out", required=True, help="Output chunks_512.jsonl")
    ap.add_argument("--max-words", type=int, default=222, help="Max words per chunk (whitespace split)")
    ap.add_argument("--overlap-words", type=int, default=10, help="Tail overlap from previous part")
    ap.add_argument("--report-every", type=int, default=1000, help="Print progress every N input pages")
    args = ap.parse_args()

    inp = Path(args.chunks_in)
    out = Path(args.chunks_out)
    out.parent.mkdir(parents=True, exist_ok=True)

    # TXT-отчёт кладём рядом с итоговым chunks_512.jsonl.
    report_txt = out.with_name("chunks_512_report.txt")

    total_in = 0
    total_out = 0
    bad_json = 0
    empty_input_text = 0
    total_input_words = 0
    total_output_words = 0
    pages_single = 0
    pages_split = 0
    max_parts_on_page = 0

    t0 = time.perf_counter()

    with inp.open("r", encoding="utf-8", errors="ignore") as fin, \
         out.open("w", encoding="utf-8", newline="\n") as fout:

        write_line = fout.write
        dumps = json.dumps

        # Основной цикл обработки страниц из chunks_cleaned_full.jsonl.
        for line_no, line in enumerate(fin, start=1):
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except Exception:
                bad_json += 1
                continue

            total_in += 1
            text = rec.get("text", "") or ""
            words = whitespace_words(text)
            word_count = len(words)
            total_input_words += word_count

            if word_count == 0:
                empty_input_text += 1
                continue

            subchunks = build_subchunks(words, args.max_words, args.overlap_words)
            parts = len(subchunks)

            if parts == 1:
                pages_single += 1
            else:
                pages_split += 1

            if parts > max_parts_on_page:
                max_parts_on_page = parts

            doc_id = rec.get("doc_id", "")
            page_start = rec.get("page_start", 0)
            year = rec.get("year")
            txt_rel = rec.get("txt_rel")

            for idx, subtext in enumerate(subchunks, start=1):
                out_rec: Dict[str, Any] = {
                    "chunk_id": f"{doc_id}::p{int(page_start):05d}::w{idx:02d}of{parts:02d}",
                    "doc_id": doc_id,
                    "text": subtext,
                    "page_start": page_start,
                    "year": year,
                    "txt_rel": txt_rel,
                }

                # Запись минимального чанка в итоговый JSONL.
                write_line(dumps(out_rec, ensure_ascii=False) + "\n")
                total_out += 1
                total_output_words += len(subtext.split())

            if args.report_every > 0 and total_in % args.report_every == 0:
                elapsed = time.perf_counter() - t0
                speed = total_in / elapsed if elapsed > 0 else 0.0
                print(
                    f"progress pages={total_in} out_chunks={total_out} split_pages={pages_split} "
                    f"bad_json={bad_json} speed={speed:.1f} pages/s",
                    flush=True,
                )

    elapsed = time.perf_counter() - t0
    speed_in = total_in / elapsed if elapsed > 0 else 0.0
    speed_out = total_out / elapsed if elapsed > 0 else 0.0
    avg_in_words = (total_input_words / total_in) if total_in else 0.0
    avg_out_words = (total_output_words / total_out) if total_out else 0.0

    # Запись краткого текстового отчёта рядом с итоговым файлом.
    with report_txt.open("w", encoding="utf-8", newline="\n") as f:
        f.write("MAKE_CHUNKS_512_FROM_CLEANED REPORT\n")
        f.write(f"Program: {PROGRAM}\n")
        f.write(f"Version: {VERSION}\n")
        f.write("\n")
        f.write(f"Input file : {inp}\n")
        f.write(f"Output file: {out}\n")
        f.write("\n")
        f.write("PARAMETERS\n")
        f.write(f"max_words     : {args.max_words}\n")
        f.write(f"overlap_words : {args.overlap_words}\n")
        f.write(f"report_every  : {args.report_every}\n")
        f.write("\n")
        f.write("RESULT\n")
        f.write(f"total_input_pages         : {total_in}\n")
        f.write(f"total_output_chunks       : {total_out}\n")
        f.write(f"pages_single              : {pages_single}\n")
        f.write(f"pages_split               : {pages_split}\n")
        f.write(f"max_parts_on_page         : {max_parts_on_page}\n")
        f.write(f"bad_json                  : {bad_json}\n")
        f.write(f"empty_input_text          : {empty_input_text}\n")
        f.write("\n")
        f.write("AVERAGES\n")
        f.write(f"avg_input_words_per_page   : {round(avg_in_words, 6)}\n")
        f.write(f"avg_output_words_per_chunk : {round(avg_out_words, 6)}\n")
        f.write("\n")
        f.write("PERFORMANCE\n")
        f.write(f"time_sec   : {elapsed:.6f}\n")
        f.write(f"speed_in   : {speed_in:.2f} pages/s\n")
        f.write(f"speed_out  : {speed_out:.2f} chunks/s\n")

    print("OK", flush=True)
    print(
        {
            "total_input_pages": total_in,
            "total_output_chunks": total_out,
            "pages_single": pages_single,
            "pages_split": pages_split,
            "max_parts_on_page": max_parts_on_page,
            "bad_json": bad_json,
            "empty_input_text": empty_input_text,
            "avg_input_words_per_page": round(avg_in_words, 6),
            "avg_output_words_per_chunk": round(avg_out_words, 6),
        },
        flush=True,
    )
    print(f"time: {elapsed:.6f}", flush=True)
    print(f"speed_in: {speed_in:.2f} pages/s", flush=True)
    print(f"speed_out: {speed_out:.2f} chunks/s", flush=True)
    print(f"Written: {out}", flush=True)
    print(f"Report TXT: {report_txt}", flush=True)


if __name__ == "__main__":
    main()
