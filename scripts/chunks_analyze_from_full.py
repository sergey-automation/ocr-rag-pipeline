# -*- coding: utf-8 -*-

import json
import argparse
import time
import re
from pathlib import Path
from typing import Dict, Any, List, Tuple

PROGRAM = "CHUNKS_ANALYZE_FROM_FULL"
VERSION = "2026-04-04 v1.3"

THRESHOLDS = {
    "letter_ratio_bad": 0.35,
    "digit_ratio_bad": 0.30,
    "punct_ratio_bad": 0.25,
    "single_char_token_ratio_bad": 0.30,
    "non_letter_line_ratio_bad": 0.40,
    "avg_token_len_bad": 3.0,
    "short_line_ratio_bad": 0.30,
    "duplicate_line_ratio_bad": 0.20,
    "suspicious_score_min": 3,
    "junk_score_min": 5,
    "short_line_max_len": 3,
}

TOKEN_RE = re.compile(r"\w+", flags=re.UNICODE)

LINE_SCHEMA = [
    "line_no",
    "text",
    "text_len",
    "text_len_stripped",
    "token_count_simple",
    "letter_ratio",
    "digit_ratio",
    "punct_ratio",
    "space_ratio",
    "single_char_token_ratio",
    "avg_token_len",
    "has_letter",
    "has_digit",
    "is_duplicate_in_chunk",
    "table_like_score_line",
    "line_class",
]

LINE_CLASS_TO_CODE = {
    "text_line": 0,
    "short_text": 1,
    "table_like": 2,
    "non_letter": 3,
}

QUALITY_FIELDS = (
    "text_len",
    "line_count",
    "token_count",
    "letter_ratio",
    "digit_ratio",
    "punct_ratio",
    "single_char_token_ratio",
    "non_letter_line_ratio",
    "avg_token_len",
    "short_line_ratio",
    "duplicate_line_ratio",
    "junk_score",
    "quality_class",
)

COMPAT_MISSING_FIELDS = (
    "doc_id16",
    "rel_path_stg1",
    "authors",
    "title",
    "year",
    "udc",
    "bbk",
    "size_bytes_txt",
    "manifest_generator_program",
    "manifest_generator_version",
    "manifest_schema_version",
    "chunk_generator_program",
    "chunk_generator_version",
)


def safe_ratio(num, den):
    return num / den if den > 0 else 0.0


def round_metric(x: float) -> float:
    return round(float(x), 6)


def build_line_items(text: str, lines_format: str = "dict") -> Tuple[List[Any], List[str], int]:
    split_lines = text.splitlines()
    non_empty_lines = [l for l in split_lines if l.strip()]

    counts = {}
    for l in non_empty_lines:
        k = l.strip()
        counts[k] = counts.get(k, 0) + 1

    line_items: List[Any] = []
    for idx, line in enumerate(split_lines, start=1):
        if not line.strip():
            continue

        line_text = line
        line_text_len = len(line_text)
        line_text_stripped = line_text.strip()
        line_text_len_stripped = len(line_text_stripped)

        line_tokens = TOKEN_RE.findall(line_text)
        token_count_simple = len(line_tokens)
        single_char_token_ratio = round_metric(
            safe_ratio(sum(1 for t in line_tokens if len(t) == 1), token_count_simple)
        )
        avg_token_len_line = round_metric(
            safe_ratio(sum(len(t) for t in line_tokens), token_count_simple)
        )

        line_letters = sum(c.isalpha() for c in line_text)
        line_digits = sum(c.isdigit() for c in line_text)
        line_punct = sum((not c.isalnum()) and (not c.isspace()) for c in line_text)
        line_spaces = sum(c.isspace() for c in line_text)

        letter_ratio_line = round_metric(safe_ratio(line_letters, line_text_len))
        digit_ratio_line = round_metric(safe_ratio(line_digits, line_text_len))
        punct_ratio_line = round_metric(safe_ratio(line_punct, line_text_len))
        space_ratio_line = round_metric(safe_ratio(line_spaces, line_text_len))

        has_letter = line_letters > 0
        has_digit = line_digits > 0
        is_duplicate_in_chunk = counts.get(line_text_stripped, 0) > 1

        table_like_score_line = 0
        if digit_ratio_line >= 0.30:
            table_like_score_line += 1
        if letter_ratio_line <= 0.50:
            table_like_score_line += 1
        if single_char_token_ratio >= 0.40 and token_count_simple >= 4:
            table_like_score_line += 1
        if not has_letter:
            table_like_score_line += 1
        if avg_token_len_line > 0 and avg_token_len_line < 3.0:
            table_like_score_line += 1

        if not has_letter:
            line_class = "non_letter"
        elif table_like_score_line >= 2:
            line_class = "table_like"
        elif line_text_len_stripped <= 3:
            line_class = "short_text"
        else:
            line_class = "text_line"

        if lines_format == "compact":
            line_items.append([
                idx,
                line_text,
                line_text_len,
                line_text_len_stripped,
                token_count_simple,
                letter_ratio_line,
                digit_ratio_line,
                punct_ratio_line,
                space_ratio_line,
                single_char_token_ratio,
                avg_token_len_line,
                1 if has_letter else 0,
                1 if has_digit else 0,
                1 if is_duplicate_in_chunk else 0,
                table_like_score_line,
                LINE_CLASS_TO_CODE.get(line_class, 0),
            ])
        else:
            line_items.append({
                "line_no": idx,
                "text": line_text,
                "text_len": line_text_len,
                "text_len_stripped": line_text_len_stripped,
                "token_count_simple": token_count_simple,
                "letter_ratio": letter_ratio_line,
                "digit_ratio": digit_ratio_line,
                "punct_ratio": punct_ratio_line,
                "space_ratio": space_ratio_line,
                "single_char_token_ratio": single_char_token_ratio,
                "avg_token_len": avg_token_len_line,
                "has_letter": has_letter,
                "has_digit": has_digit,
                "is_duplicate_in_chunk": is_duplicate_in_chunk,
                "table_like_score_line": table_like_score_line,
                "line_class": line_class,
            })

    return line_items, non_empty_lines, counts


def analyze(text: str, lines_format: str = "dict") -> Dict[str, Any]:
    text = text or ""
    text_len = len(text)

    letters = sum(c.isalpha() for c in text)
    digits = sum(c.isdigit() for c in text)
    punct = sum((not c.isalnum()) and (not c.isspace()) for c in text)

    line_items, non_empty_lines, counts = build_line_items(text, lines_format=lines_format)
    line_count = len(non_empty_lines)

    tokens = TOKEN_RE.findall(text)
    token_count = len(tokens)

    single_char_tokens = sum(1 for t in tokens if len(t) == 1)
    avg_token_len = safe_ratio(sum(len(t) for t in tokens), token_count)

    non_letter_lines = sum(
        1 for l in non_empty_lines if not any(c.isalpha() for c in l)
    )
    short_lines = sum(
        1 for l in non_empty_lines
        if 0 < len(l.strip()) <= THRESHOLDS["short_line_max_len"]
    )

    duplicate_ratio = 0.0
    if line_count > 0:
        dup = sum(v for v in counts.values() if v > 1)
        duplicate_ratio = safe_ratio(dup, line_count)

    metrics = {
        "text_len": text_len,
        "line_count": line_count,
        "token_count": token_count,
        "letter_ratio": round_metric(safe_ratio(letters, text_len)),
        "digit_ratio": round_metric(safe_ratio(digits, text_len)),
        "punct_ratio": round_metric(safe_ratio(punct, text_len)),
        "single_char_token_ratio": round_metric(safe_ratio(single_char_tokens, token_count)),
        "non_letter_line_ratio": round_metric(safe_ratio(non_letter_lines, line_count)),
        "avg_token_len": round_metric(avg_token_len),
        "short_line_ratio": round_metric(safe_ratio(short_lines, line_count)),
        "duplicate_line_ratio": round_metric(duplicate_ratio),
    }

    junk_score = 0
    if metrics["letter_ratio"] < THRESHOLDS["letter_ratio_bad"]:
        junk_score += 1
    if metrics["digit_ratio"] > THRESHOLDS["digit_ratio_bad"]:
        junk_score += 1
    if metrics["punct_ratio"] > THRESHOLDS["punct_ratio_bad"]:
        junk_score += 1
    if metrics["single_char_token_ratio"] > THRESHOLDS["single_char_token_ratio_bad"]:
        junk_score += 1
    if metrics["non_letter_line_ratio"] > THRESHOLDS["non_letter_line_ratio_bad"]:
        junk_score += 1
    if metrics["avg_token_len"] < THRESHOLDS["avg_token_len_bad"]:
        junk_score += 1
    if metrics["short_line_ratio"] > THRESHOLDS["short_line_ratio_bad"]:
        junk_score += 1
    if metrics["duplicate_line_ratio"] > THRESHOLDS["duplicate_line_ratio_bad"]:
        junk_score += 1

    if junk_score >= THRESHOLDS["junk_score_min"]:
        qclass = "junk_candidate"
    elif junk_score >= THRESHOLDS["suspicious_score_min"]:
        qclass = "suspicious"
    else:
        qclass = "ok"

    metrics["junk_score"] = junk_score
    metrics["quality_class"] = qclass
    metrics["lines"] = line_items
    if lines_format == "compact":
        metrics["lines_schema"] = LINE_SCHEMA
    return metrics


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunks-in", required=True, help="Входной chunks_full.jsonl")
    ap.add_argument("--chunks-out", required=True, help="Выходной chunks_analiz.jsonl")
    ap.add_argument("--report-every", type=int, default=1000, help="Печатать прогресс каждые N чанков")
    ap.add_argument(
        "--lines-format",
        choices=("dict", "compact"),
        default="dict",
        help="Формат поля lines: dict (старый) или compact (через lines_schema + массивы)",
    )
    args = ap.parse_args()

    inp = Path(args.chunks_in)
    out = Path(args.chunks_out)
    out.parent.mkdir(parents=True, exist_ok=True)

    stats = {
        "total": 0,
        "ok": 0,
        "suspicious": 0,
        "junk_candidate": 0,
        "bad_json": 0,
        "empty_lines": 0,
        "total_non_empty_lines": 0,
    }

    t0 = time.perf_counter()

    with inp.open("r", encoding="utf-8", errors="ignore") as fin, \
         out.open("w", encoding="utf-8", newline="\n") as fout:

        write_line = fout.write
        dumps = json.dumps

        for line_no, line in enumerate(fin, start=1):
            line = line.lstrip("﻿")
            if not line.strip():
                stats["empty_lines"] += 1
                continue

            try:
                rec = json.loads(line)
            except Exception:
                stats["bad_json"] += 1
                continue

            text = rec.get("text", "")
            if isinstance(text, str):
                text = text.lstrip("\ufeff")
                rec["text"] = text

            for k in COMPAT_MISSING_FIELDS:
                if k not in rec:
                    rec[k] = None

            q = analyze(text, lines_format=args.lines_format)

            for k in QUALITY_FIELDS:
                rec[k] = q[k]
            rec["lines"] = q["lines"]
            if args.lines_format == "compact":
                rec["lines_schema"] = q["lines_schema"]

            write_line(dumps(rec, ensure_ascii=False) + "\n")

            stats["total"] += 1
            stats[q["quality_class"]] += 1
            stats["total_non_empty_lines"] += q["line_count"]

            if args.report_every > 0 and stats["total"] % args.report_every == 0:
                elapsed = time.perf_counter() - t0
                speed = stats["total"] / elapsed if elapsed > 0 else 0.0
                print(
                    f"progress chunks={stats['total']} "
                    f"ok={stats['ok']} "
                    f"suspicious={stats['suspicious']} "
                    f"junk_candidate={stats['junk_candidate']} "
                    f"bad_json={stats['bad_json']} "
                    f"speed={speed:.1f} chunks/s",
                    flush=True
                )

    elapsed = time.perf_counter() - t0
    speed = stats["total"] / elapsed if elapsed > 0 else 0.0

    txt = out.with_name(out.stem + "_stats.txt")
    with txt.open("w", encoding="utf-8", newline="\n") as f:
        f.write(f"program: {PROGRAM}\n")
        f.write(f"version: {VERSION}\n")
        f.write(f"input: {inp}\n")
        f.write(f"output: {out}\n")
        f.write(f"total: {stats['total']}\n")
        f.write(f"ok: {stats['ok']}\n")
        f.write(f"suspicious: {stats['suspicious']}\n")
        f.write(f"junk_candidate: {stats['junk_candidate']}\n")
        f.write(f"bad_json: {stats['bad_json']}\n")
        f.write(f"empty_lines: {stats['empty_lines']}\n")
        f.write(f"time_sec: {elapsed:.3f}\n")
        f.write(f"speed_chunks_per_sec: {speed:.3f}\n")

    js = out.with_name(out.stem + "_stats.json")
    with js.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(
            {
                "program": PROGRAM,
                "version": VERSION,
                "input": str(inp),
                "output": str(out),
                **stats,
                "time_sec": round(elapsed, 6),
                "speed_chunks_per_sec": round(speed, 6),
                "thresholds": THRESHOLDS,
                "line_stats_added": True,
                "lines_format": args.lines_format,
                "line_class_codes": LINE_CLASS_TO_CODE,
                "total_non_empty_lines": stats["total_non_empty_lines"],
                "avg_non_empty_lines_per_chunk": round(
                    safe_ratio(stats["total_non_empty_lines"], stats["total"]), 6
                ),
            },
            f,
            ensure_ascii=False,
            indent=2
        )

    print("OK", flush=True)
    print(stats, flush=True)
    print(f"time: {elapsed:.6f}", flush=True)
    print(f"speed: {speed:.2f} chunks/s", flush=True)
    print(f"Written: {out}", flush=True)
    print(f"Stats TXT: {txt}", flush=True)
    print(f"Stats JSON: {js}", flush=True)


if __name__ == "__main__":
    main()
