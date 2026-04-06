# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PROGRAM = "CLEAR_TAB_FROM_ANALIZ"
VERSION = "2026-04-04 v1.2"

BOX_DRAWING_RE = re.compile(r"[│┃─━┌┐└┘├┤┬┴┼╔╗╚╝╠╣╦╩╬═║]")
REPEATED_SEPARATORS_RE = re.compile(r"[-_=~`^*•·.…]{2,}")
COLUMN_SEPARATORS_RE = re.compile(r"[|¦]+")
MULTISPACE_RE = re.compile(r"\s+")
CYR_KEEP_TOKEN_TEMPLATE = r"^[А-Яа-яЁё]{{{min_len_plus},}}$"

SHORT_TECH_TOKENS = {
    "ph", "fe", "cu", "zn", "pb", "au", "ag", "ni", "co", "al", "mg", "ca", "na", "k",
    "мм", "см", "м", "км", "г", "кг", "т", "с", "мс", "а", "в", "кв", "квт", "мпа",
    "па", "бар", "%", "№", "гост", "ост", "ту", "рис", "рис.", "табл", "табл.",
}


def path_is_tab(rec: Dict[str, Any]) -> bool:
    for key in ("txt_abs", "txt_rel"):
        value = rec.get(key)
        if isinstance(value, str) and value:
            v = value.replace("\\", "/").lower()
            if "/tab/" in v:
                return True
    return False


def normalize_line(line: str) -> str:
    line = line.replace("\t", " ")
    line = BOX_DRAWING_RE.sub(" ", line)
    line = COLUMN_SEPARATORS_RE.sub(" ", line)
    line = REPEATED_SEPARATORS_RE.sub(" ", line)
    cleaned_chars: List[str] = []
    for ch in line:
        if ch.isalpha() or ch.isdigit() or ch.isspace():
            cleaned_chars.append(ch)
        elif ch in ",.;:/+-()[]{}№%":
            cleaned_chars.append(" ")
        else:
            cleaned_chars.append(" ")
    line = "".join(cleaned_chars)
    return MULTISPACE_RE.sub(" ", line).strip()


def line_metrics(line: str) -> Dict[str, Any]:
    total_chars = len(line)
    if total_chars <= 0:
        return {
            "digit_ratio": 0.0,
            "letter_ratio": 0.0,
            "single_char_token_ratio": 0.0,
            "token_count": 0,
            "avg_token_len": 0.0,
            "has_letter": False,
            "text_len_stripped": 0,
        }
    digits = sum(ch.isdigit() for ch in line)
    letters = sum(ch.isalpha() for ch in line)
    tokens = line.split()
    token_count = len(tokens)
    single_char_token_ratio = (sum(1 for t in tokens if len(t) == 1) / token_count) if token_count else 0.0
    avg_token_len = (sum(len(t) for t in tokens) / token_count) if token_count else 0.0
    return {
        "digit_ratio": digits / total_chars,
        "letter_ratio": letters / total_chars,
        "single_char_token_ratio": single_char_token_ratio,
        "token_count": token_count,
        "avg_token_len": avg_token_len,
        "has_letter": letters > 0,
        "text_len_stripped": len(line.strip()),
    }


def is_table_line(line: str) -> bool:
    m = line_metrics(line)
    return (
        (m["digit_ratio"] >= 0.30)
        or (m["letter_ratio"] <= 0.50)
        or (m["single_char_token_ratio"] >= 0.40 and m["token_count"] >= 4)
    )


def clean_table_line(line: str, keep_re: re.Pattern[str]) -> Tuple[str, Dict[str, int]]:
    tokens = line.split()
    kept: List[str] = []
    stats = {
        "tokens_seen": len(tokens),
        "tokens_kept": 0,
        "numeric_removed": 0,
        "mixed_removed": 0,
        "latin_removed": 0,
        "other_removed": 0,
    }
    for tok in tokens:
        if keep_re.match(tok):
            kept.append(tok)
            stats["tokens_kept"] += 1
            continue
        has_digit = any(ch.isdigit() for ch in tok)
        has_alpha = any(ch.isalpha() for ch in tok)
        has_latin = any(("A" <= ch <= "Z") or ("a" <= ch <= "z") for ch in tok)
        if (not has_alpha) and has_digit:
            stats["numeric_removed"] += 1
        elif has_digit and has_alpha:
            stats["mixed_removed"] += 1
        elif has_latin:
            stats["latin_removed"] += 1
        else:
            stats["other_removed"] += 1
    return " ".join(kept), stats


def clean_tab_text(text: str, min_word_len: int) -> Tuple[str, Dict[str, int]]:
    keep_re = re.compile(CYR_KEEP_TOKEN_TEMPLATE.format(min_len_plus=min_word_len + 1))
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    out_lines: List[str] = []
    stats = {
        "lines_in": 0,
        "lines_out": 0,
        "table_lines": 0,
        "text_lines": 0,
        "tokens_seen": 0,
        "tokens_kept": 0,
        "numeric_removed": 0,
        "mixed_removed": 0,
        "latin_removed": 0,
        "other_removed": 0,
        "lines_removed": 0,
    }
    for raw_line in lines:
        stats["lines_in"] += 1
        line = normalize_line(raw_line)
        if not line:
            stats["lines_removed"] += 1
            continue
        if is_table_line(line):
            stats["table_lines"] += 1
            cleaned_line, st = clean_table_line(line, keep_re)
            for k in ("tokens_seen", "tokens_kept", "numeric_removed", "mixed_removed", "latin_removed", "other_removed"):
                stats[k] += st[k]
            if cleaned_line:
                out_lines.append(cleaned_line)
                stats["lines_out"] += 1
            else:
                stats["lines_removed"] += 1
        else:
            stats["text_lines"] += 1
            out_lines.append(line)
            stats["lines_out"] += 1
            token_count = len(line.split())
            stats["tokens_seen"] += token_count
            stats["tokens_kept"] += token_count
    return "\n".join(out_lines), stats


def is_separator_like(line: str) -> bool:
    s = line.strip()
    if not s:
        return True
    stripped = re.sub(r"[\-_=~`^*•·.…|¦\s]+", "", s)
    return stripped == ""


def line_has_short_tech_whitelist(line: str) -> bool:
    for tok in line.split():
        t = tok.strip(" ,.;:/+-()[]{}")
        if not t:
            continue
        if t.lower() in SHORT_TECH_TOKENS:
            return True
    return False


LINE_CLASS_CODE_TO_NAME = {
    0: "text_line",
    1: "short_text",
    2: "table_like",
    3: "non_letter",
}


def compact_row_to_dict(schema_map: Dict[str, int], row: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(row, list):
        return None
    out: Dict[str, Any] = {}
    for key, idx in schema_map.items():
        if 0 <= idx < len(row):
            out[key] = row[idx]

    line_class = out.get("line_class")
    if isinstance(line_class, int):
        out["line_class"] = LINE_CLASS_CODE_TO_NAME.get(line_class, "unknown")

    for bool_key in ("has_letter", "has_digit", "is_duplicate_in_chunk"):
        if bool_key in out and isinstance(out[bool_key], int):
            out[bool_key] = bool(out[bool_key])

    return out


def maybe_get_line_entries(rec: Dict[str, Any], raw_lines: List[str]) -> List[Optional[Dict[str, Any]]]:
    # Новый compact-формат: lines_schema + lines (list[list]).
    schema = rec.get("lines_schema")
    lines_value = rec.get("lines")
    if isinstance(schema, list) and schema and isinstance(lines_value, list):
        schema_map = {str(name): i for i, name in enumerate(schema)}
        compact_dicts = [compact_row_to_dict(schema_map, row) for row in lines_value]
        compact_dicts = [x for x in compact_dicts if isinstance(x, dict)]
        if compact_dicts:
            arrays = compact_dicts
        else:
            arrays = []
    else:
        arrays = []

    # Старый dict-формат.
    if not arrays:
        candidates = (
            "line_stats", "lines_stats", "text_lines_stats", "per_line_stats",
            "line_metrics_list", "lines", "analyzed_lines",
        )

        for key in candidates:
            value = rec.get(key)
            if isinstance(value, list) and value:
                dict_items = [x for x in value if isinstance(x, dict)]
                if dict_items:
                    arrays = dict_items
                    break

    if not arrays:
        return [None] * len(raw_lines)

    if len(arrays) == len(raw_lines):
        return arrays

    # Попытка выровнять по line_no (чаще всего нумерация 1-based).
    by_no: Dict[int, Dict[str, Any]] = {}
    for d in arrays:
        line_no = d.get("line_no") if isinstance(d, dict) else None
        if isinstance(line_no, int) and line_no >= 1:
            by_no[line_no] = d
    if by_no:
        return [by_no.get(i + 1) for i in range(len(raw_lines))]

    # Попытка выровнять только по непустым строкам.
    non_empty_positions = [i for i, s in enumerate(raw_lines) if s.strip()]
    if len(arrays) == len(non_empty_positions):
        out: List[Optional[Dict[str, Any]]] = [None] * len(raw_lines)
        for pos, d in zip(non_empty_positions, arrays):
            out[pos] = d
        return out

    return [None] * len(raw_lines)


def build_line_state(raw_line: str, ext: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    norm_line = normalize_line(raw_line)
    base = line_metrics(norm_line)
    if ext is None:
        return {
            "line": norm_line,
            "text_len_stripped": int(base["text_len_stripped"]),
            "token_count_simple": int(base["token_count"]),
            "letter_ratio": float(base["letter_ratio"]),
            "digit_ratio": float(base["digit_ratio"]),
            "single_char_token_ratio": float(base["single_char_token_ratio"]),
            "avg_token_len": float(base["avg_token_len"]),
            "has_letter": bool(base["has_letter"]),
            "is_duplicate_in_chunk": False,
            "table_like_score_line": 0,
            "line_class": "unknown",
        }

    text_from_ext = ext.get("text")
    if isinstance(text_from_ext, str) and text_from_ext.strip():
        norm_from_ext = normalize_line(text_from_ext)
        if norm_from_ext:
            norm_line = norm_from_ext
            base = line_metrics(norm_line)

    def pick_int(*keys: str, default: int) -> int:
        for k in keys:
            v = ext.get(k)
            if isinstance(v, (int, float)):
                return int(v)
        return default

    def pick_float(*keys: str, default: float) -> float:
        for k in keys:
            v = ext.get(k)
            if isinstance(v, (int, float)):
                return float(v)
        return default

    def pick_bool(*keys: str, default: bool) -> bool:
        for k in keys:
            v = ext.get(k)
            if isinstance(v, bool):
                return v
        return default

    return {
        "line": norm_line,
        "text_len_stripped": pick_int("text_len_stripped", "text_len", default=int(base["text_len_stripped"])),
        "token_count_simple": pick_int("token_count_simple", "token_count", default=int(base["token_count"])),
        "letter_ratio": pick_float("letter_ratio", default=float(base["letter_ratio"])),
        "digit_ratio": pick_float("digit_ratio", default=float(base["digit_ratio"])),
        "single_char_token_ratio": pick_float("single_char_token_ratio", default=float(base["single_char_token_ratio"])),
        "avg_token_len": pick_float("avg_token_len", default=float(base["avg_token_len"])),
        "has_letter": pick_bool("has_letter", default=bool(base["has_letter"])),
        "is_duplicate_in_chunk": pick_bool("is_duplicate_in_chunk", default=False),
        "table_like_score_line": pick_int("table_like_score_line", default=0),
        "line_class": str(ext.get("line_class", "unknown") or "unknown"),
    }


def decide_non_tab_line_action(st: Dict[str, Any], qclass: str, seen_short_norm: set[str]) -> Tuple[str, str]:
    line = st["line"]
    norm_low = line.strip().lower()

    if not line:
        return "drop", "empty_after_normalize"
    if is_separator_like(line):
        return "drop", "separator_like"

    if line_has_short_tech_whitelist(line):
        if len(norm_low) <= 80:
            seen_short_norm.add(norm_low)
        return "keep", "tech_whitelist"

    text_len = st["text_len_stripped"]
    token_count = st["token_count_simple"]
    letter_ratio = st["letter_ratio"]
    digit_ratio = st["digit_ratio"]
    single_ratio = st["single_char_token_ratio"]
    avg_token_len = st["avg_token_len"]
    has_letter = st["has_letter"]
    is_dup = st["is_duplicate_in_chunk"]
    table_like = st["table_like_score_line"]
    line_class = st["line_class"]

    if text_len <= 2:
        return "drop", "too_short"
    if (not has_letter) and text_len < 25:
        return "drop", "short_no_letters"
    if (text_len <= 80) and (is_dup or norm_low in seen_short_norm):
        return "drop", "duplicate_short_line"

    # OCR-мусор. Для ok ослабляем порог.
    if qclass == "ok":
        if single_ratio > 0.60 and token_count >= 4:
            return "drop", "single_char_ocr_noise"
    else:
        if single_ratio > 0.50 and token_count >= 4:
            return "drop", "single_char_ocr_noise"

    # Реальное использование line_class / table_like_score_line.
    if qclass == "ok":
        if line_class == "non_letter" and letter_ratio < 0.20 and text_len < 40:
            return "drop", "non_letter_line"
        if table_like >= 3 and digit_ratio > 0.45 and letter_ratio < 0.30:
            return "drop", "table_like_line"
        if digit_ratio > 0.50 and letter_ratio < 0.30:
            return "drop", "heavy_numeric"
    elif qclass == "suspicious":
        if line_class == "non_letter" and letter_ratio < 0.35:
            return "drop", "non_letter_line"
        if table_like >= 2 and letter_ratio < 0.45:
            return "drop", "table_like_line"
        if digit_ratio > 0.45 and letter_ratio < 0.35:
            return "drop", "heavy_numeric"
    else:  # junk_candidate / unknown
        if line_class == "non_letter" and letter_ratio < 0.40:
            return "drop", "non_letter_line"
        if table_like >= 2 and letter_ratio < 0.50:
            return "drop", "table_like_line"
        if digit_ratio > 0.40 and letter_ratio < 0.40:
            return "drop", "heavy_numeric"

    # Только нормализовать, не выкидывать.
    weak = False
    if qclass == "ok":
        if letter_ratio < 0.40 or avg_token_len < 2.8 or single_ratio > 0.35:
            weak = True
    else:
        if letter_ratio < 0.45 or avg_token_len < 3.0 or single_ratio > 0.35:
            weak = True

    if len(norm_low) <= 80:
        seen_short_norm.add(norm_low)

    if weak:
        return "normalize", "weak_text_normalize_only"
    return "keep", "good_text"


def clean_non_tab_text_with_line_stats(rec: Dict[str, Any], text: str, qclass: str) -> Tuple[str, Dict[str, int]]:
    raw_lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    ext_lines = maybe_get_line_entries(rec, raw_lines)
    out_lines: List[str] = []
    seen_short_norm: set[str] = set()

    stats = {
        "lines_in": 0,
        "lines_out": 0,
        "lines_removed": 0,
        "separator_removed": 0,
        "too_short_removed": 0,
        "no_letters_short_removed": 0,
        "ocr_noise_removed": 0,
        "heavy_numeric_removed": 0,
        "duplicate_removed": 0,
        "non_letter_removed": 0,
        "table_like_removed": 0,
        "normalized_only": 0,
        "whitelist_kept": 0,
    }

    for raw_line, ext in zip(raw_lines, ext_lines):
        stats["lines_in"] += 1
        st = build_line_state(raw_line, ext)
        action, reason = decide_non_tab_line_action(st, qclass, seen_short_norm)
        line = st["line"]

        if action == "drop":
            stats["lines_removed"] += 1
            if reason in ("empty_after_normalize", "separator_like"):
                stats["separator_removed"] += 1
            elif reason == "too_short":
                stats["too_short_removed"] += 1
            elif reason == "short_no_letters":
                stats["no_letters_short_removed"] += 1
            elif reason == "single_char_ocr_noise":
                stats["ocr_noise_removed"] += 1
            elif reason == "heavy_numeric":
                stats["heavy_numeric_removed"] += 1
            elif reason == "duplicate_short_line":
                stats["duplicate_removed"] += 1
            elif reason == "non_letter_line":
                stats["non_letter_removed"] += 1
            elif reason == "table_like_line":
                stats["table_like_removed"] += 1
            continue

        if action == "normalize":
            stats["normalized_only"] += 1
        elif reason == "tech_whitelist":
            stats["whitelist_kept"] += 1

        if line:
            out_lines.append(line)
            stats["lines_out"] += 1
        else:
            stats["lines_removed"] += 1
            stats["separator_removed"] += 1

    return "\n".join(out_lines), stats


def to_txt_summary(summary: Dict[str, Any]) -> str:
    keys = [
        ("program", "Program"),
        ("version", "Version"),
        ("chunks_in", "Input / вход"),
        ("chunks_out", "Output / выход"),
        ("total_chunks", "Total chunks / всего чанков"),
        ("bad_json", "Bad JSON / битых JSON"),
        ("before_total_chars", "Before chars / символов до"),
        ("after_total_chars", "After chars / символов после"),
        ("char_reduction", "Reduction / сокращение"),
        ("char_reduction_ratio", "Reduction ratio / доля сокращения"),
        ("written_chunks", "Written chunks / записано чанков"),
        ("removed_empty_chunks", "Removed empty chunks / удалено пустых чанков"),
        ("tab_chunks", "TAB chunks / чанков tab"),
        ("other_chunks", "Other chunks / прочих чанков"),
        ("non_tab_ok_cleaned", "Non-tab ok cleaned / non-tab ok очищено"),
        ("non_tab_suspicious_cleaned", "Non-tab suspicious cleaned / non-tab suspicious очищено"),
        ("non_tab_junk_cleaned", "Non-tab junk cleaned / non-tab junk очищено"),
        ("tab_lines_in", "TAB lines in / строк tab до"),
        ("tab_lines_out", "TAB lines out / строк tab после"),
        ("tab_table_lines", "TAB table lines / табличных строк"),
        ("tab_text_lines", "TAB text lines / текстовых строк"),
        ("tab_lines_removed", "TAB lines removed / строк tab удалено"),
        ("tab_tokens_seen", "TAB tokens seen / токенов tab до"),
        ("tab_tokens_kept", "TAB tokens kept / токенов tab после"),
        ("tab_token_keep_ratio", "TAB token keep ratio / доля сохраненных токенов tab"),
        ("tab_numeric_removed", "TAB numeric removed / удалено чисел"),
        ("tab_mixed_removed", "TAB mixed removed / удалено смешанных токенов"),
        ("tab_latin_removed", "TAB latin removed / удалено латиницы"),
        ("tab_other_removed", "TAB other removed / удалено прочего"),
        ("non_tab_ok_lines_in", "Non-tab ok lines in / non-tab ok строк до"),
        ("non_tab_ok_lines_out", "Non-tab ok lines out / non-tab ok строк после"),
        ("non_tab_ok_lines_removed", "Non-tab ok lines removed / non-tab ok строк удалено"),
        ("non_tab_suspicious_lines_in", "Non-tab suspicious lines in / non-tab suspicious строк до"),
        ("non_tab_suspicious_lines_out", "Non-tab suspicious lines out / non-tab suspicious строк после"),
        ("non_tab_suspicious_lines_removed", "Non-tab suspicious lines removed / non-tab suspicious строк удалено"),
        ("non_tab_junk_lines_in", "Non-tab junk lines in / non-tab junk строк до"),
        ("non_tab_junk_lines_out", "Non-tab junk lines out / non-tab junk строк после"),
        ("non_tab_junk_lines_removed", "Non-tab junk lines removed / non-tab junk строк удалено"),
        ("non_tab_separator_removed", "Non-tab separator removed / удалено разделителей"),
        ("non_tab_too_short_removed", "Non-tab too short removed / удалено слишком коротких"),
        ("non_tab_no_letters_short_removed", "Non-tab no letters short removed / удалено коротких без букв"),
        ("non_tab_ocr_noise_removed", "Non-tab ocr noise removed / удалено OCR-шума"),
        ("non_tab_heavy_numeric_removed", "Non-tab heavy numeric removed / удалено тяжёлых цифровых строк"),
        ("non_tab_duplicate_removed", "Non-tab duplicate removed / удалено дублей строк"),
        ("non_tab_non_letter_removed", "Non-tab non-letter removed / удалено non-letter строк"),
        ("non_tab_table_like_removed", "Non-tab table-like removed / удалено table-like строк"),
        ("non_tab_normalized_only", "Non-tab normalized only / только нормализовано"),
        ("non_tab_whitelist_kept", "Non-tab whitelist kept / сохранено whitelist"),
        ("min_word_len_rule", "Rule / правило"),
    ]
    return "\n".join(f"{label}: {summary[key]}" for key, label in keys) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunks", default="chunks_analiz.jsonl")
    ap.add_argument("--out", default="chunks_cleaned_full.jsonl")
    ap.add_argument("--summary-json", default="clean_chunks_full_summary.json")
    ap.add_argument("--summary-txt", default="clean_chunks_full_report.txt")
    ap.add_argument("--min-word-len", type=int, default=3)
    ap.add_argument("--report-every", type=int, default=50000)
    args = ap.parse_args()

    total = bad_json = written_chunks = removed_empty_chunks = 0
    total_before_chars = total_after_chars = 0
    tab_chunks = other_chunks = tab_changed = 0
    non_tab_ok_cleaned = non_tab_suspicious_cleaned = non_tab_junk_cleaned = 0

    tab_lines_in = tab_lines_out = tab_table_lines = tab_text_lines = tab_tokens_seen = tab_tokens_kept = 0
    tab_numeric_removed = tab_mixed_removed = tab_latin_removed = tab_other_removed = tab_lines_removed = 0

    non_tab_ok_lines_in = non_tab_ok_lines_out = non_tab_ok_lines_removed = 0
    non_tab_suspicious_lines_in = non_tab_suspicious_lines_out = non_tab_suspicious_lines_removed = 0
    non_tab_junk_lines_in = non_tab_junk_lines_out = non_tab_junk_lines_removed = 0

    non_tab_separator_removed = non_tab_too_short_removed = non_tab_no_letters_short_removed = 0
    non_tab_ocr_noise_removed = non_tab_heavy_numeric_removed = non_tab_duplicate_removed = 0
    non_tab_non_letter_removed = non_tab_table_like_removed = 0
    non_tab_normalized_only = non_tab_whitelist_kept = 0

    chunks_path = Path(args.chunks)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with chunks_path.open("r", encoding="utf-8", errors="ignore") as f_in, \
         out_path.open("w", encoding="utf-8", newline="\n") as f_out:
        for line in f_in:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                bad_json += 1
                continue

            total += 1
            orig_text = rec.get("text", "")
            total_before_chars += len(orig_text)
            new_text = orig_text

            if path_is_tab(rec):
                tab_chunks += 1
                new_text, st = clean_tab_text(orig_text, args.min_word_len)
                rec["clean_mode"] = "tab_mixed"
                tab_lines_in += st["lines_in"]
                tab_lines_out += st["lines_out"]
                tab_table_lines += st["table_lines"]
                tab_text_lines += st["text_lines"]
                tab_tokens_seen += st["tokens_seen"]
                tab_tokens_kept += st["tokens_kept"]
                tab_numeric_removed += st["numeric_removed"]
                tab_mixed_removed += st["mixed_removed"]
                tab_latin_removed += st["latin_removed"]
                tab_other_removed += st["other_removed"]
                tab_lines_removed += st["lines_removed"]
                if new_text != orig_text:
                    tab_changed += 1
            else:
                other_chunks += 1
                qclass = rec.get("quality_class", "ok")
                if qclass == "ok":
                    rec["clean_mode"] = "non_tab_line_clean_ok"
                    non_tab_ok_cleaned += 1
                    new_text, st = clean_non_tab_text_with_line_stats(rec, orig_text, qclass)
                    non_tab_ok_lines_in += st["lines_in"]
                    non_tab_ok_lines_out += st["lines_out"]
                    non_tab_ok_lines_removed += st["lines_removed"]
                elif qclass == "suspicious":
                    rec["clean_mode"] = "non_tab_line_clean_suspicious"
                    non_tab_suspicious_cleaned += 1
                    new_text, st = clean_non_tab_text_with_line_stats(rec, orig_text, qclass)
                    non_tab_suspicious_lines_in += st["lines_in"]
                    non_tab_suspicious_lines_out += st["lines_out"]
                    non_tab_suspicious_lines_removed += st["lines_removed"]
                else:
                    rec["clean_mode"] = "non_tab_line_clean_junk"
                    non_tab_junk_cleaned += 1
                    new_text, st = clean_non_tab_text_with_line_stats(rec, orig_text, "junk_candidate")
                    non_tab_junk_lines_in += st["lines_in"]
                    non_tab_junk_lines_out += st["lines_out"]
                    non_tab_junk_lines_removed += st["lines_removed"]

                non_tab_separator_removed += st["separator_removed"]
                non_tab_too_short_removed += st["too_short_removed"]
                non_tab_no_letters_short_removed += st["no_letters_short_removed"]
                non_tab_ocr_noise_removed += st["ocr_noise_removed"]
                non_tab_heavy_numeric_removed += st["heavy_numeric_removed"]
                non_tab_duplicate_removed += st["duplicate_removed"]
                non_tab_non_letter_removed += st["non_letter_removed"]
                non_tab_table_like_removed += st["table_like_removed"]
                non_tab_normalized_only += st["normalized_only"]
                non_tab_whitelist_kept += st["whitelist_kept"]

            rec["text_before_clean_len"] = len(orig_text)
            rec["text_after_clean_len"] = len(new_text)
            rec["chunk_changed"] = (new_text != orig_text)
            rec["became_empty_after_clean"] = (not new_text.strip())

            if rec["became_empty_after_clean"]:
                removed_empty_chunks += 1
                continue

            rec["text"] = new_text
            total_after_chars += len(new_text)
            f_out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            written_chunks += 1

            if args.report_every and total % args.report_every == 0:
                print(
                    f"progress: {total:,} chunks | written: {written_chunks:,} | removed: {removed_empty_chunks:,}",
                    flush=True,
                )

    summary = {
        "program": PROGRAM,
        "version": VERSION,
        "chunks_in": str(chunks_path.resolve()),
        "chunks_out": str(out_path.resolve()),
        "total_chunks": total,
        "bad_json": bad_json,
        "written_chunks": written_chunks,
        "removed_empty_chunks": removed_empty_chunks,
        "min_word_len_rule": f"keep cyrillic words with length > {args.min_word_len} in table lines",
        "before_total_chars": total_before_chars,
        "after_total_chars": total_after_chars,
        "char_reduction": total_before_chars - total_after_chars,
        "char_reduction_ratio": round((total_before_chars - total_after_chars) / total_before_chars, 6) if total_before_chars else 0.0,
        "tab_chunks": tab_chunks,
        "other_chunks": other_chunks,
        "tab_changed": tab_changed,
        "non_tab_ok_cleaned": non_tab_ok_cleaned,
        "non_tab_suspicious_cleaned": non_tab_suspicious_cleaned,
        "non_tab_junk_cleaned": non_tab_junk_cleaned,
        "tab_lines_in": tab_lines_in,
        "tab_lines_out": tab_lines_out,
        "tab_table_lines": tab_table_lines,
        "tab_text_lines": tab_text_lines,
        "tab_lines_removed": tab_lines_removed,
        "tab_tokens_seen": tab_tokens_seen,
        "tab_tokens_kept": tab_tokens_kept,
        "tab_token_keep_ratio": round(tab_tokens_kept / tab_tokens_seen, 6) if tab_tokens_seen else 0.0,
        "tab_numeric_removed": tab_numeric_removed,
        "tab_mixed_removed": tab_mixed_removed,
        "tab_latin_removed": tab_latin_removed,
        "tab_other_removed": tab_other_removed,
        "non_tab_ok_lines_in": non_tab_ok_lines_in,
        "non_tab_ok_lines_out": non_tab_ok_lines_out,
        "non_tab_ok_lines_removed": non_tab_ok_lines_removed,
        "non_tab_suspicious_lines_in": non_tab_suspicious_lines_in,
        "non_tab_suspicious_lines_out": non_tab_suspicious_lines_out,
        "non_tab_suspicious_lines_removed": non_tab_suspicious_lines_removed,
        "non_tab_junk_lines_in": non_tab_junk_lines_in,
        "non_tab_junk_lines_out": non_tab_junk_lines_out,
        "non_tab_junk_lines_removed": non_tab_junk_lines_removed,
        "non_tab_separator_removed": non_tab_separator_removed,
        "non_tab_too_short_removed": non_tab_too_short_removed,
        "non_tab_no_letters_short_removed": non_tab_no_letters_short_removed,
        "non_tab_ocr_noise_removed": non_tab_ocr_noise_removed,
        "non_tab_heavy_numeric_removed": non_tab_heavy_numeric_removed,
        "non_tab_duplicate_removed": non_tab_duplicate_removed,
        "non_tab_non_letter_removed": non_tab_non_letter_removed,
        "non_tab_table_like_removed": non_tab_table_like_removed,
        "non_tab_normalized_only": non_tab_normalized_only,
        "non_tab_whitelist_kept": non_tab_whitelist_kept,
    }

    Path(args.summary_json).write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    Path(args.summary_txt).write_text(to_txt_summary(summary), encoding="utf-8")

    print("\nOK")
    print(f"Written chunks : {written_chunks}")
    print(f"Removed empty  : {removed_empty_chunks}")
    print(f"Reduction      : {summary['char_reduction_ratio']:.4%}")
    print(f"\nOutput      : {out_path}")
    print(f"Summary JSON: {Path(args.summary_json)}")
    print(f"Summary TXT : {Path(args.summary_txt)}")


if __name__ == "__main__":
    main()
