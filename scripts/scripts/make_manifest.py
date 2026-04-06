#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import argparse
import hashlib
import json
import re
from pathlib import Path
from collections import Counter
from difflib import SequenceMatcher

PROGRAM = "make_manifest"
VERSION = "1.0"
MANIFEST_SCHEMA_VERSION = "2026-04-04 v4.0"

DEFAULT_LIB_ROOT = r"C:\LIB1"
DEFAULT_OUTPUT_MANIFEST = r"C:\LIB1\manifest.jsonl"
DEFAULT_OUTPUT_DIR = r"C:\LIB1\metadata"
DEFAULT_REPORT_TXT = r"C:\LIB1\metadata\make_manifest_report.txt"
DEFAULT_REPORT_JSON = r"C:\LIB1\metadata\make_manifest_report.json"
DEFAULT_DEBUG_TXT = r"C:\LIB1\metadata\make_manifest_debug.txt"
DEFAULT_MERGE_LOG_JSONL = r"C:\LIB1\metadata\make_manifest_merge_log.jsonl"
DEFAULT_SAME_SIZE_SAME_CONTENT_MANIFEST_JSONL = r"C:\LIB1\metadata\make_manifest_same_size_same_content.jsonl"
DEFAULT_SAME_SIZE_SAME_CONTENT_REPORT_TXT = r"C:\LIB1\metadata\make_manifest_same_size_same_content_report.txt"
DEFAULT_SIZE_DISTRIBUTION_REPORT_TXT = r"C:\LIB1\metadata\make_manifest_size_distribution_report.txt"
DEFAULT_E5_SMALL_FILES_REPORT_TXT = r"C:\LIB1\metadata\make_manifest_e5_small_files_report.txt"
DEFAULT_E5_SMALL_SCAN_MAX_BYTES = 512   # верхняя граница для проверки очень коротких TXT
DEFAULT_E5_MIN_SEARCH_TOKENS = 20       # если токенов меньше 20, документ не попадет в поиск
DEFAULT_E5_TOKENIZER_NAME = "intfloat/multilingual-e5-base"
DEFAULT_E5_REPORT_MAX_TEXT_CHARS = 4000

DEFAULT_CONTEXT_CHARS = 80
DEFAULT_MAX_UDC = 4
DEFAULT_EARLY_LIMIT = 500
DEFAULT_YEAR_HEAD_CHARS = 5000
PROGRESS_EVERY = 500

# ============================================================
# PROGRAM: make_manifest
# VERSION: 2026-04-04 v4.0
#
# ЧТО ДЕЛАЕТ ПРОГРАММА:
# - сканирует TXT-файлы в дереве C:\LIB1\OCR_TXT
# - формирует основной manifest.jsonl
# - основной идентификатор: doc_id = короткий SHA1 от относительного пути
# - сохраняет размер файла TXT в поле size_bytes
# - извлекает из имени файла: год, авторов, заглавие
# - пытается извлечь из текста: УДК, ББК, год (если год не найден в имени)
# - строит отчет по группам файлов одинакового размера
# - для файлов из групп одинакового размера считает SHA1 содержимого
# - сохраняет только подгруппы: одинаковый размер + одинаковое содержимое
# - считает распределение размеров TXT-файлов по диапазонам
# - отдельно проверяет только очень короткие TXT-файлы токенизатором E5
# - считает только token_count_e5, без расчета чанков
# - делит документы на:
#     1) token_count_e5 < 20  -> не попадут в поиск
#     2) token_count_e5 >= 20 -> попадут в поиск
# - в TXT-отчет выводит не только источник, но и текст документа
#
# КАК ОПРЕДЕЛЯЕТСЯ src_kind:
# - если в относительном пути TXT есть папка pdf_OCR_TXT -> src_kind = "pdf"
# - если в относительном пути TXT есть папка djvu_OCR_TXT -> src_kind = "djvu"
# - иначе src_kind = None
# ============================================================


# ============================================================
# STAGE1 (ровно по логике extract_authors_manifest_v9.0.py)
# ============================================================

YEAR_RE_STAGE1 = re.compile(r"(1[5-9]\d{2}|20[0-2]\d|2030)")
UNKNOWN_YEAR_TEXT_RE = re.compile(r"\(?\s*год\s+неизв\.?\s*\)?", re.I)
EMPTY_PARENS_RE = re.compile(r"\(\s*\)")
NOAUTHOR_PREFIX_RE = re.compile(
    r"^\s*(Без\s+авт\.?|Сборник\.?|Много\s+авт\.?|Коллектив\s+авторов\.?)\s*(.*)$",
    re.I
)
INITIALS_RE = re.compile(r"(?:[A-ZА-ЯЁ]\.){1,3}")
DOUBLE_INITIALS_RE = re.compile(r"(?:[A-ZА-ЯЁ]\.){2}")
TITLE_LEADING_AUTHOR_MARKER_RE = re.compile(
    r"^\s*("
    r"\(\s*ред\.?\s*\)|"
    r"\(\s*под\s+ред\.?\s*\)|"
    r"\(\s*отв\.\s*ред\.?\s*\)|"
    r"\(\s*ркд\s*\)|"
    r"\(\s*сост\.?\s*\)|"
    r"и\s+др\.?"
    r")\s*[-–—]?\s*",
    re.I
)
EMPTY_AUTHOR_PREFIX_RE = re.compile(
    r"^\s*("
    r"Альманах\.|"
    r"Журнал\.|"
    r"Интернет-журнал\.|"
    r"Конспекты\s+лекций\.|"
    r"Серия\.\s*Наука\.|"
    r"Landau_"
    r")\s*(.*)$",
    re.I
)
UNDERSCORE_AUTHOR_FROM_TITLE_RE = re.compile(
    r"^\s*([A-Za-zА-Яа-яЁё]{1,12}_)\s*(.*)$"
)
HYPHEN_AUTHOR_FROM_TITLE_RE = re.compile(
    r"^\s*([A-Za-zА-Яа-яЁё]{1,12}(?:\s+[A-Za-zА-Яа-яЁё]{1,12}){0,2})\s-\s+(.*)$"
)
DJ_INITIAL_AUTHOR_FROM_TITLE_RE = re.compile(
    r"^\s*([A-Za-zА-Яа-яЁё]{1,12}\s+Дж\.)\s+(.*)$"
)
SURNAME_HYPHEN_FROM_TITLE_RE = re.compile(
    r"^\s*([A-Za-zА-Яа-яЁё]{1,12})\s*[-–—]\s*(.*)$"
)
AUTHOR_TAIL_TO_TITLE_RE = re.compile(
    r"^(.*?(?:и\s+др\.?|\(\s*ред\s*\)))(?:\s+|[-–—]\s*)(.+)$",
    re.I
)

# ============================================================
# STAGE2 (ровно по логике manifest_stage2_from_text.py)
# ============================================================

UDK_LABEL_RE = re.compile(r"УДК")
UDK_GROUP1_LABEL_RE = re.compile(r"УДК[:.\-]")
UDK_BLOCK_RE = re.compile(r"^\s*([0-9][0-9\s./:;,+()=\[\]\-]{1,120})")

BBK_LABEL_RE = re.compile(r"ББК")
BBK_STOP_RE = re.compile(
    r"(?:\bУДК\b|\bISBN\b|\bАвторы?\b|\bРецензент(?:ы)?\b|\bРедактор\b|\n\s*[А-ЯЁA-Z][^\n]{5,}|[—–]\s*[А-ЯЁA-Z][а-яёa-z])",
    re.IGNORECASE,
)
BBK_VALUE_RE = re.compile(
    r"(?ix)"
    r"(?:"
    r"\d{1,3}(?:\.\d{1,3}){0,3}(?:[А-ЯЁа-яёA-Za-z]{1,3}\d{0,3})?(?:-\d{1,3})?"
    r"|"
    r"\d{1,3}[А-ЯЁа-яёA-Za-z]{1,3}\d{0,3}(?:\.\d{1,3}){0,2}(?:-\d{1,3})?"
    r")"
)

YEAR_RE_STAGE2 = re.compile(r"(?<!\d)(18[6-9]\d|19\d\d|20[0-1]\d|202[0-5])(?!\d)")
YEAR_AFTER_MARKER_RE = re.compile(r"^\s*(?:г\.?|Г\.?|год|ГОД|т\.)\b")
YEAR_BEFORE_MARKER_RE = re.compile(r"\b(?:г\.?|Г\.?|год|ГОД|т\.)\s*$")


def cleanup_rest(rest: str) -> str:
    rest = re.sub(
        r"\(\s*\d+(?:[.,]\d+)?\s*(?:kb|mb|gb)\s*\)",
        "",
        rest,
        flags=re.I
    )
    rest = re.sub(
        r"\(\s*(?:djvu|pdf|ocr|txt|scan)\s*\)",
        "",
        rest,
        flags=re.I
    )
    rest = re.sub(
        r"[\s._\-()]*\b(?:djvu|pdf|ocr|txt|scan)\b[\s._\-()]*$",
        "",
        rest,
        flags=re.I
    )
    prev = None
    while prev != rest:
        prev = rest
        rest = EMPTY_PARENS_RE.sub("", rest)

    rest = re.sub(r"\s{2,}", " ", rest)
    rest = re.sub(r"\s+([.,;:])", r"\1", rest)
    rest = rest.strip(" -_,;:()[]")
    return rest


def extract_year(name: str):
    matches = list(YEAR_RE_STAGE1.finditer(name))
    if matches:
        m = max(matches, key=lambda x: x.start())
        year = int(m.group(1))
        before = name[:m.start(1)]
        after = name[m.end(1):]
        rest = cleanup_rest(before + after)
        return year, rest

    has_unknown_year_text = bool(UNKNOWN_YEAR_TEXT_RE.search(name))
    has_empty_parens = bool(EMPTY_PARENS_RE.search(name))

    if has_unknown_year_text or has_empty_parens:
        rest = name
        rest = UNKNOWN_YEAR_TEXT_RE.sub("", rest)
        prev = None
        while prev != rest:
            prev = rest
            rest = EMPTY_PARENS_RE.sub("", rest)
        rest = cleanup_rest(rest)
        return 1111, rest

    return None, cleanup_rest(name)


def move_leading_title_marker_to_authors(authors: str, title: str):
    m = TITLE_LEADING_AUTHOR_MARKER_RE.match(title)
    if not m:
        return authors, title

    marker = m.group(1).strip()
    title = title[m.end():].strip()

    if authors:
        authors = f"{authors} {marker}".strip()
    else:
        authors = marker

    authors = re.sub(r"\s{2,}", " ", authors).strip()
    title = re.sub(r"\s{2,}", " ", title).strip(" ,;:-")
    return authors, title


def move_empty_author_prefix_from_name(name_after_year: str, authors: str, title: str):
    if authors:
        return authors, title

    m = EMPTY_AUTHOR_PREFIX_RE.match(name_after_year)
    if not m:
        return authors, title

    authors = m.group(1).strip()
    title = m.group(2).strip()
    authors = re.sub(r"\s{2,}", " ", authors).strip()
    title = re.sub(r"\s{2,}", " ", title).strip(" ,;:-")
    return authors, title


def move_underscore_leading_word_from_title(authors: str, title: str):
    if authors:
        return authors, title, False

    m = UNDERSCORE_AUTHOR_FROM_TITLE_RE.match(title)
    if not m:
        return authors, title, False

    authors = m.group(1).strip()
    title = m.group(2).strip()
    authors = re.sub(r"\s{2,}", " ", authors).strip()
    title = re.sub(r"\s{2,}", " ", title).strip(" ,;:-")
    return authors, title, True


def move_hyphen_leading_author_from_title(authors: str, title: str):
    if authors:
        return authors, title, False

    m = HYPHEN_AUTHOR_FROM_TITLE_RE.match(title)
    if not m:
        return authors, title, False

    authors = m.group(1).strip()
    title = m.group(2).strip()
    authors = re.sub(r"\s{2,}", " ", authors).strip()
    title = re.sub(r"\s{2,}", " ", title).strip(" ,;:-")
    return authors, title, True


def move_dj_initial_author_from_title(authors: str, title: str):
    if authors:
        return authors, title, False

    m = DJ_INITIAL_AUTHOR_FROM_TITLE_RE.match(title)
    if not m:
        return authors, title, False

    authors = m.group(1).strip()
    title = m.group(2).strip()
    authors = re.sub(r"\s{2,}", " ", authors).strip()
    title = re.sub(r"\s{2,}", " ", title).strip(" ,;:-")
    return authors, title, True


def is_only_two_initials(authors: str) -> bool:
    s = authors.strip()
    if len(s) >= 5:
        return False
    return bool(re.fullmatch(r"[A-ZА-ЯЁ]\.[A-ZА-ЯЁ]\.", s))


def move_surname_from_title_to_initials(authors: str, title: str):
    if not authors:
        return authors, title, False
    if not is_only_two_initials(authors):
        return authors, title, False

    m = SURNAME_HYPHEN_FROM_TITLE_RE.match(title)
    if not m:
        return authors, title, False

    surname = m.group(1).strip()
    title = m.group(2).strip()
    authors = f"{surname} {authors}".strip()

    authors = re.sub(r"\s{2,}", " ", authors).strip()
    title = re.sub(r"\s{2,}", " ", title).strip(" ,;:-")
    return authors, title, True


def move_author_tail_after_marker_to_title(authors: str, title: str):
    if len(authors.strip()) <= 10:
        return authors, title, False

    m = AUTHOR_TAIL_TO_TITLE_RE.match(authors.strip())
    if not m:
        return authors, title, False

    authors_head = m.group(1).strip()
    tail = m.group(2).strip()
    if not tail:
        return authors, title, False

    if title:
        title = f"{tail} {title}".strip()
    else:
        title = tail

    authors = authors_head
    authors = re.sub(r"\s{2,}", " ", authors).strip()
    title = re.sub(r"\s{2,}", " ", title).strip(" ,;:-")
    return authors, title, True


def fix_title_tail_after_double_initials(authors: str, title: str):
    authors = authors.strip()
    title = title.strip()

    if not authors or not title:
        return authors, title, False
    if len(authors) <= len(title):
        return authors, title, False
    if not re.search(r"[A-ZА-ЯЁ]\.\s*$", authors):
        return authors, title, False

    matches = list(DOUBLE_INITIALS_RE.finditer(authors))
    if not matches:
        return authors, title, False

    last_double = matches[-1]
    new_authors = authors[:last_double.end()].strip(" ,;:-")
    moved_tail = authors[last_double.end():].strip(" ,;:-")
    if not moved_tail:
        return authors, title, False

    new_title = f"{moved_tail} {title}".strip()
    new_title = re.sub(r"\s{2,}", " ", new_title).strip(" ,;:-")
    new_authors = re.sub(r"\s{2,}", " ", new_authors).strip(" ,;:-")
    return new_authors, new_title, True


def move_right_sentence_from_authors_to_title_if_title_empty(authors: str, title: str):
    authors = authors.strip()
    title = title.strip()

    if title:
        return authors, title, False
    if not authors:
        return authors, title, False

    matches = list(DOUBLE_INITIALS_RE.finditer(authors))
    if not matches:
        return authors, title, False

    last_double = matches[-1]
    new_authors = authors[:last_double.end()].strip(" ,;:-")
    new_title = authors[last_double.end():].strip(" ,;:-")
    if not new_title:
        return authors, title, False

    new_authors = re.sub(r"\s{2,}", " ", new_authors).strip()
    new_title = re.sub(r"\s{2,}", " ", new_title).strip()
    return new_authors, new_title, True


def split_authors_title(name_after_year: str):
    s = name_after_year.strip()

    m = NOAUTHOR_PREFIX_RE.match(s)
    if m:
        authors = m.group(1).strip()
        title = m.group(2).strip()

        authors, title = move_leading_title_marker_to_authors(authors, title)

        authors, title, moved = move_author_tail_after_marker_to_title(authors, title)
        if moved:
            return authors, title, "author_tail_after_marker_to_title"

        authors, title, moved = fix_title_tail_after_double_initials(authors, title)
        if moved:
            return authors, title, "fix_title_tail_after_double_initials"

        authors, title, moved = move_right_sentence_from_authors_to_title_if_title_empty(authors, title)
        if moved:
            return authors, title, "move_right_sentence_from_authors_to_title_if_title_empty"

        return authors, title, "noauthor_prefix"

    matches = list(INITIALS_RE.finditer(s))
    if not matches:
        authors = ""
        title = s

        authors, title = move_leading_title_marker_to_authors(authors, title)
        authors, title = move_empty_author_prefix_from_name(s, authors, title)
        if authors:
            authors, title, moved = move_author_tail_after_marker_to_title(authors, title)
            if moved:
                return authors, title, "author_tail_after_marker_to_title"

            authors, title, moved = fix_title_tail_after_double_initials(authors, title)
            if moved:
                return authors, title, "fix_title_tail_after_double_initials"

            authors, title, moved = move_right_sentence_from_authors_to_title_if_title_empty(authors, title)
            if moved:
                return authors, title, "move_right_sentence_from_authors_to_title_if_title_empty"

            return authors, title, "empty_author_prefix"

        authors, title, moved = move_underscore_leading_word_from_title(authors, title)
        if moved:
            authors, title, moved2 = move_author_tail_after_marker_to_title(authors, title)
            if moved2:
                return authors, title, "author_tail_after_marker_to_title"

            authors, title, moved2 = fix_title_tail_after_double_initials(authors, title)
            if moved2:
                return authors, title, "fix_title_tail_after_double_initials"

            authors, title, moved2 = move_right_sentence_from_authors_to_title_if_title_empty(authors, title)
            if moved2:
                return authors, title, "move_right_sentence_from_authors_to_title_if_title_empty"

            return authors, title, "underscore_leading_word"

        authors, title, moved = move_hyphen_leading_author_from_title(authors, title)
        if moved:
            authors, title, moved2 = move_author_tail_after_marker_to_title(authors, title)
            if moved2:
                return authors, title, "author_tail_after_marker_to_title"

            authors, title, moved2 = fix_title_tail_after_double_initials(authors, title)
            if moved2:
                return authors, title, "fix_title_tail_after_double_initials"

            authors, title, moved2 = move_right_sentence_from_authors_to_title_if_title_empty(authors, title)
            if moved2:
                return authors, title, "move_right_sentence_from_authors_to_title_if_title_empty"

            return authors, title, "hyphen_leading_author"

        authors, title, moved = move_dj_initial_author_from_title(authors, title)
        if moved:
            authors, title, moved2 = move_author_tail_after_marker_to_title(authors, title)
            if moved2:
                return authors, title, "author_tail_after_marker_to_title"

            authors, title, moved2 = fix_title_tail_after_double_initials(authors, title)
            if moved2:
                return authors, title, "fix_title_tail_after_double_initials"

            authors, title, moved2 = move_right_sentence_from_authors_to_title_if_title_empty(authors, title)
            if moved2:
                return authors, title, "move_right_sentence_from_authors_to_title_if_title_empty"

            return authors, title, "dj_initial_author"

        return authors, title, "no_initials"

    m = matches[-1]
    authors = s[:m.end()].strip(" ,;:-")
    title = s[m.end():].strip(" ,;:-")

    authors, title = move_leading_title_marker_to_authors(authors, title)
    authors, title = move_empty_author_prefix_from_name(s, authors, title)

    authors, title, moved = move_surname_from_title_to_initials(authors, title)
    if moved:
        authors, title, moved2 = move_author_tail_after_marker_to_title(authors, title)
        if moved2:
            return authors, title, "author_tail_after_marker_to_title"

        authors, title, moved2 = fix_title_tail_after_double_initials(authors, title)
        if moved2:
            return authors, title, "fix_title_tail_after_double_initials"

        authors, title, moved2 = move_right_sentence_from_authors_to_title_if_title_empty(authors, title)
        if moved2:
            return authors, title, "move_right_sentence_from_authors_to_title_if_title_empty"

        return authors, title, "initials_plus_hyphen_surname"

    authors, title, moved = move_author_tail_after_marker_to_title(authors, title)
    if moved:
        return authors, title, "author_tail_after_marker_to_title"

    authors, title, moved = fix_title_tail_after_double_initials(authors, title)
    if moved:
        return authors, title, "fix_title_tail_after_double_initials"

    authors, title, moved = move_right_sentence_from_authors_to_title_if_title_empty(authors, title)
    if moved:
        return authors, title, "move_right_sentence_from_authors_to_title_if_title_empty"

    return authors, title, "initials_from_right"


def read_text_file(path: Path) -> str:
    encodings = ["utf-8-sig", "utf-8", "cp1251", "cp866", "latin-1"]
    last_err = None
    for enc in encodings:
        try:
            return path.read_text(encoding=enc, errors="strict")
        except Exception as e:
            last_err = e
    raise last_err


def normalize_block(raw: str) -> str:
    s = raw.strip()
    s = re.sub(r"\s+", " ", s)
    s = s.strip(" ,;:-")
    return s


def clean_udk_for_compare(udk: str) -> str:
    return re.sub(r"[^0-9./:+()\[\]-]", "", udk)


def is_plausible_udk(udk: str) -> bool:
    cleaned = clean_udk_for_compare(udk)
    digits = re.sub(r"\D", "", cleaned)
    if len(digits) < 3:
        return False
    if len(cleaned) < 3:
        return False
    return True


def clean_bbk_for_compare(bbk: str) -> str:
    return re.sub(r"[^0-9A-Za-zА-Яа-яЁё./:+()\[\]-]", "", bbk)


def is_plausible_bbk(bbk: str) -> bool:
    cleaned = clean_bbk_for_compare(bbk)
    digits = re.sub(r"\D", "", cleaned)
    if len(digits) < 2:
        return False
    if len(cleaned) < 3:
        return False
    if len(cleaned) > 25:
        return False
    return True


def extract_hits_with_label_re(text: str, context_chars: int, label_re, block_re, source_tag: str, value_key: str, validator):
    hits = []
    text_len = len(text)
    for m in label_re.finditer(text):
        after = text[m.end(): m.end() + 140]
        block_m = block_re.match(after)
        if not block_m:
            continue

        raw_block = block_m.group(1)
        value = normalize_block(raw_block)
        if not value or not validator(value):
            continue

        pos_from_start = m.start()
        pos_from_end = text_len - pos_from_start

        left = max(0, pos_from_start - context_chars)
        right = min(text_len, m.end() + len(raw_block) + context_chars)
        context = text[left:right].replace("\n", " ").replace("\r", " ")
        context = re.sub(r"\s+", " ", context).strip()

        hits.append({
            value_key: value,
            "context": context,
            "pos_from_start": pos_from_start,
            "pos_from_end": pos_from_end,
            "source_tag": source_tag,
        })
    return hits


def extract_udk_hits_full_text(text: str, context_chars: int):
    return extract_hits_with_label_re(text, context_chars, UDK_LABEL_RE, UDK_BLOCK_RE, "exact", "udk", is_plausible_udk)


def extract_udk_hits_group1(text: str, context_chars: int):
    return extract_hits_with_label_re(text, context_chars, UDK_GROUP1_LABEL_RE, UDK_BLOCK_RE, "group1", "udk", is_plausible_udk)


def extract_bbk_hits_full_text(text: str, context_chars: int):
    hits = []
    text_len = len(text)

    for m in BBK_LABEL_RE.finditer(text):
        pos_from_start = m.start()
        pos_from_end = text_len - pos_from_start

        after = text[m.end(): m.end() + 160]
        stop_m = BBK_STOP_RE.search(after)
        if stop_m:
            after = after[:stop_m.start()]

        ordinary_text_m = re.search(r"\.\s+[А-ЯЁA-Z][а-яёa-z]{2,}", after)
        if ordinary_text_m:
            after = after[:ordinary_text_m.start()]

        candidate_m = BBK_VALUE_RE.search(after)
        if not candidate_m:
            continue

        raw_value = candidate_m.group(0)
        value = normalize_block(raw_value)
        if not value or not is_plausible_bbk(value):
            continue

        left = max(0, pos_from_start - context_chars)
        right = min(text_len, m.end() + len(after) + context_chars)
        context = text[left:right].replace("\n", " ").replace("\r", " ")
        context = re.sub(r"\s+", " ", context).strip()

        hits.append({
            "bbk": value,
            "context": context,
            "pos_from_start": pos_from_start,
            "pos_from_end": pos_from_end,
            "source_tag": "exact",
        })

    return hits


def choose_year_from_text_head(text: str, head_chars: int = DEFAULT_YEAR_HEAD_CHARS):
    head = text[:head_chars]
    candidates = []
    for m in YEAR_RE_STAGE2.finditer(head):
        year = int(m.group(1))
        pos = m.start()
        before = head[max(0, pos - 12):pos]
        after = head[m.end(): min(len(head), m.end() + 12)]

        score = 0
        score += max(0, 2000 - pos) / 2000.0

        if YEAR_AFTER_MARKER_RE.match(after):
            score += 3.0
        if YEAR_BEFORE_MARKER_RE.search(before):
            score += 1.5

        window = head[max(0, pos - 40): min(len(head), m.end() + 40)]
        if re.search(r"изд\.?|издание|выпуск|тираж|печать|подписано", window, re.I):
            score += 0.7

        candidates.append((score, -pos, year))

    if not candidates:
        return ""

    candidates.sort(reverse=True)
    return candidates[0][2]


def similarity_ratio(a: str, b: str, cleaner) -> float:
    a2 = cleaner(a)
    b2 = cleaner(b)
    if not a2 or not b2:
        return 0.0
    return SequenceMatcher(None, a2, b2).ratio()


def score_group_similarity(values, cleaner):
    if not values:
        return 0.0
    if len(values) == 1:
        return 1.0

    sims = []
    for i in range(len(values)):
        for j in range(i + 1, len(values)):
            sims.append(similarity_ratio(values[i], values[j], cleaner))
    return sum(sims) / len(sims) if sims else 1.0


def choose_accepted_code(hits, early_limit: int, max_hits: int, value_field: str, cleaner, early_base: float = 0.85, late_base: float = 0.45):
    if not hits:
        return "", 0.0, "not_found", []

    early_hits = [h for h in hits if h["pos_from_start"] <= early_limit][:max_hits]
    if early_hits:
        selected = early_hits
        zone = "early"
        base_weight = early_base
    else:
        late_hits = [h for h in hits if h["pos_from_start"] > early_limit][:max_hits]
        if not late_hits:
            return "", 0.0, "not_found", []
        selected = late_hits
        zone = "late"
        base_weight = late_base

    values = [h[value_field] for h in selected]
    similarity = score_group_similarity(values, cleaner)

    cnt = Counter(values)
    best_value = None
    best_key = None
    first_pos_by_value = {}
    for h in selected:
        first_pos_by_value.setdefault(h[value_field], h["pos_from_start"])

    for value, count in cnt.items():
        key = (count, -first_pos_by_value[value])
        if best_key is None or key > best_key:
            best_key = key
            best_value = value

    repeat_ratio = cnt[best_value] / len(selected)
    weight = base_weight + 0.10 * similarity + 0.05 * repeat_ratio
    weight = round(min(1.0, weight), 3)

    return best_value, weight, zone, selected


def is_valid_year_for_merge(y):
    if y is None:
        return False
    y = str(y).strip()
    if y == "" or y == "1111":
        return False
    return len(y) == 4 and y.isdigit()


def norm(s):
    return str(s).replace("\\", "/").strip() if s else ""


def detect_src_kind_from_txt_rel(txt_rel):
    if not txt_rel:
        return None
    s = str(txt_rel).replace("\\", "/").lower()
    if "pdf_ocr_txt" in s:
        return "pdf"
    if "djvu_ocr_txt" in s:
        return "djvu"
    return None


def sha1_file_hex(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def human_size(num_bytes):
    if num_bytes is None:
        return ""
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(num_bytes)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.2f} {unit}"
        value /= 1024.0


def build_same_size_rows(out_rows):
    groups = {}
    for row in out_rows:
        size_bytes = row.get("size_bytes")
        groups.setdefault(size_bytes, []).append(row)

    same_content_rows = []
    same_content_group_summaries = []
    total_files_in_size_groups = 0
    total_unique_sizes_with_duplicates = 0
    size_group_no = 0
    same_content_group_no = 0

    for size_bytes in sorted(groups.keys()):
        rows = groups[size_bytes]
        if size_bytes is None or len(rows) <= 1:
            continue

        total_unique_sizes_with_duplicates += 1
        total_files_in_size_groups += len(rows)
        size_group_no += 1

        rows_sorted = sorted(rows, key=lambda r: (str(r.get("txt_rel", "")).lower(), str(r.get("txt_abs", "")).lower()))

        for row in rows_sorted:
            txt_abs = str(row.get("txt_abs", "")).strip()
            try:
                row["content_sha1"] = sha1_file_hex(Path(txt_abs)) if txt_abs else ""
            except Exception as e:
                row["content_sha1"] = f"ERROR: {e}"

        hash_groups = {}
        for row in rows_sorted:
            hash_key = row.get("content_sha1", "")
            hash_groups.setdefault(hash_key, []).append(row)

        for hash_key in sorted(hash_groups.keys()):
            hash_rows = hash_groups[hash_key]
            if len(hash_rows) < 2:
                continue
            if str(hash_key).startswith("ERROR:"):
                continue

            same_content_group_no += 1
            same_content_group_summaries.append({
                "report_group_no": same_content_group_no,
                "size_group_no": size_group_no,
                "size_bytes": size_bytes,
                "content_sha1": hash_key,
                "group_size": len(hash_rows),
            })
            for idx, row in enumerate(hash_rows, 1):
                same_content_rows.append({
                    "report_group_no": same_content_group_no,
                    "size_group_no": size_group_no,
                    "group_size": len(hash_rows),
                    "group_index": idx,
                    "size_bytes": size_bytes,
                    "content_sha1": row.get("content_sha1", ""),
                    "doc_id": row.get("doc_id", ""),
                    "sid": row.get("sid", ""),
                    "txt_rel": row.get("txt_rel", ""),
                    "txt_abs": row.get("txt_abs", ""),
                    "uid": row.get("uid", ""),
                    "file_name": Path(str(row.get("txt_abs", ""))).name if row.get("txt_abs") else "",
                })

    control = {
        "total_files_in_size_groups": total_files_in_size_groups,
        "total_unique_sizes_with_duplicates": total_unique_sizes_with_duplicates,
    }
    return same_content_rows, same_content_group_summaries, control


def write_same_size_report_txt(path: Path, title: str, group_summaries, manifest_rows, control):
    by_group = {}
    for row in manifest_rows:
        by_group.setdefault(row["report_group_no"], []).append(row)

    with path.open("w", encoding="utf-8", newline="\n") as f:
        f.write(f"{title}\n")
        f.write("=" * len(title) + "\n\n")
        f.write(f"group_count: {len(group_summaries)}\n")
        f.write(f"file_count: {len(manifest_rows)}\n\n")

        for summary in group_summaries:
            group_no = summary["report_group_no"]
            header = (
                f"[GROUP {group_no}] size_group_no={summary['size_group_no']} "
                f"size_bytes={summary['size_bytes']} files={summary['group_size']}"
            )
            if "content_sha1" in summary:
                header += f" | content_sha1={summary['content_sha1']}"
            f.write(header + "\n")

            for row in by_group.get(group_no, []):
                f.write(
                    f"  - #{row['group_index']} | size_group_no={row['size_group_no']} | "
                    f"size_bytes={row['size_bytes']} | content_sha1={row.get('content_sha1', '')} | "
                    f"doc_id={row['doc_id']} | sid={row['sid']} | txt_abs={row['txt_abs']}\n"
                )
            f.write("\n")


def build_size_distribution_rows(rows):
    ranges = [
        (0, 0, "0 B"),
        (1, 255, "1-255 B"),
        (256, 1023, "256-1023 B"),
        (1024, 2047, "1-2 KB"),
        (2048, 4095, "2-4 KB"),
        (4096, 8191, "4-8 KB"),
        (8192, 16383, "8-16 KB"),
        (16384, 32767, "16-32 KB"),
        (32768, 65535, "32-64 KB"),
        (65536, 131071, "64-128 KB"),
        (131072, 196607, "128-192 KB"),
        (196608, 262143, "192-256 KB"),
        (262144, 524287, "256-512 KB"),
        (524288, 1048575, "512 KB-1 MB"),
        (1048576, 2097151, "1-2 MB"),
        (2097152, 4194303, "2-4 MB"),
        (4194304, 8388607, "4-8 MB"),
        (8388608, None, "> 8 MB"),
    ]
    bucket_counts = []
    for start, end, label in ranges:
        cnt = 0
        for row in rows:
            size = row.get("size_bytes")
            if size is None:
                continue
            if size < start:
                continue
            if end is not None and size > end:
                continue
            cnt += 1
        bucket_counts.append({
            "range_label": label,
            "start_bytes": start,
            "end_bytes": end,
            "file_count": cnt,
        })
    return bucket_counts


def write_size_distribution_report_txt(path: Path, bucket_counts, rows_total, control=None):
    with path.open("w", encoding="utf-8", newline="\n") as f:
        f.write("make_manifest size distribution report\n")
        f.write("===================================\n\n")
        f.write(f"rows_total: {rows_total}\n")
        if control:
            f.write(f"small_file_max_bytes: {control.get('small_file_max_bytes')}\n")
            f.write(f"small_file_count: {control.get('small_file_count')}\n")
            ratio = (control.get('small_file_count', 0) / rows_total) if rows_total else 0.0
            f.write(f"small_file_ratio: {ratio:.6f}\n")
        f.write("\n")
        f.write("size_distribution:\n")
        for bucket in bucket_counts:
            ratio = (bucket["file_count"] / rows_total) if rows_total else 0.0
            f.write(f"{bucket['range_label']}: {bucket['file_count']} ({ratio:.6f})\n")


def load_e5_tokenizer(tokenizer_name):
    from transformers import AutoTokenizer
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_name, use_fast=True)
    tokenizer.model_max_length = 10**9
    return tokenizer


def estimate_e5_token_count(text, tokenizer):
    token_ids = tokenizer.encode(text, add_special_tokens=False, truncation=False)
    return len(token_ids)


def normalize_text_for_report(text: str, max_chars: int = DEFAULT_E5_REPORT_MAX_TEXT_CHARS) -> str:
    if text is None:
        return ""
    s = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if len(s) > max_chars:
        s = s[:max_chars] + "\n...[TRUNCATED]..."
    return s


def build_e5_small_files_report_rows(
    rows,
    scan_max_bytes=DEFAULT_E5_SMALL_SCAN_MAX_BYTES,
    min_search_tokens=DEFAULT_E5_MIN_SEARCH_TOKENS,
    tokenizer_name=DEFAULT_E5_TOKENIZER_NAME,
):
    candidate_rows = []
    for row in rows:
        size = row.get("size_bytes")
        if size is None:
            continue
        if size <= scan_max_bytes:
            candidate_rows.append(row)

    analyzed_rows = []
    tokenizer = None
    tokenizer_error = ""

    if candidate_rows:
        try:
            tokenizer = load_e5_tokenizer(tokenizer_name)
        except Exception as e:
            tokenizer_error = str(e)

    for row in sorted(candidate_rows, key=lambda r: (r.get("size_bytes", 0), str(r.get("txt_rel", "")).lower())):
        txt_abs = str(row.get("txt_abs", "")).strip()
        rec = {
            "doc_id": row.get("doc_id"),
            "sid": row.get("sid"),
            "size_bytes": row.get("size_bytes"),
            "size_human": human_size(row.get("size_bytes")),
            "txt_rel": row.get("txt_rel"),
            "txt_abs": row.get("txt_abs"),
            "uid": row.get("uid"),
            "src_kind": row.get("src_kind"),
            "src_rel": row.get("src_rel"),
            "file_name": Path(txt_abs).name if txt_abs else "",
            "token_count_e5": None,
            "is_searchable_e5": False,
            "text_for_report": "",
            "error": "",
        }

        if tokenizer is None:
            rec["error"] = tokenizer_error or "transformers tokenizer not loaded"
            analyzed_rows.append(rec)
            continue

        try:
            text = read_text_file(Path(txt_abs))
            token_count = estimate_e5_token_count(text, tokenizer)
            rec["token_count_e5"] = token_count
            rec["is_searchable_e5"] = token_count >= min_search_tokens
            rec["text_for_report"] = normalize_text_for_report(text)
        except Exception as e:
            rec["error"] = str(e)

        analyzed_rows.append(rec)

    not_searchable_rows = [r for r in analyzed_rows if r.get("error", "") == "" and not r.get("is_searchable_e5", False)]
    searchable_rows = [r for r in analyzed_rows if r.get("error", "") == "" and r.get("is_searchable_e5", False)]
    not_searchable_rows.sort(key=lambda r: (r.get("size_bytes", 0), r.get("token_count_e5", 0), str(r.get("txt_rel", "")).lower()))
    searchable_rows.sort(key=lambda r: (r.get("size_bytes", 0), r.get("token_count_e5", 0), str(r.get("txt_rel", "")).lower()))

    max_size_bytes_not_searchable = max((r["size_bytes"] for r in not_searchable_rows), default=None)
    min_size_bytes_searchable = min((r["size_bytes"] for r in searchable_rows), default=None)

    control = {
        "e5_small_scan_max_bytes": scan_max_bytes,
        "e5_small_scan_max_kb": round(scan_max_bytes / 1024.0, 3),
        "e5_tokenizer_name": tokenizer_name,
        "e5_min_search_tokens": min_search_tokens,
        "candidate_file_count": len(candidate_rows),
        "analyzed_file_count": len(analyzed_rows),
        "not_searchable_file_count": len(not_searchable_rows),
        "searchable_file_count": len(searchable_rows),
        "max_size_bytes_not_searchable": max_size_bytes_not_searchable,
        "max_size_human_not_searchable": human_size(max_size_bytes_not_searchable) if max_size_bytes_not_searchable is not None else "",
        "min_size_bytes_searchable": min_size_bytes_searchable,
        "min_size_human_searchable": human_size(min_size_bytes_searchable) if min_size_bytes_searchable is not None else "",
        "tokenizer_error": tokenizer_error,
        "threshold_may_need_review": bool(candidate_rows and (
            (max_size_bytes_not_searchable is not None and max_size_bytes_not_searchable >= scan_max_bytes) or
            (min_size_bytes_searchable is not None and min_size_bytes_searchable >= scan_max_bytes)
        )),
    }
    return analyzed_rows, not_searchable_rows, searchable_rows, control


def write_e5_small_files_report_txt(path: Path, analyzed_rows, not_searchable_rows, searchable_rows, control, rows_total):
    with path.open("w", encoding="utf-8", newline="\n") as f:
        f.write("make_manifest E5 token-count report\n")
        f.write("=================================\n\n")
        f.write(f"rows_total: {rows_total}\n")
        f.write(f"e5_tokenizer_name: {control['e5_tokenizer_name']}\n")
        f.write(f"e5_min_search_tokens: {control['e5_min_search_tokens']}\n")
        f.write(f"e5_small_scan_max_bytes: {control['e5_small_scan_max_bytes']}\n")
        f.write(f"e5_small_scan_max_kb: {control['e5_small_scan_max_kb']}\n")
        f.write(f"candidate_file_count: {control['candidate_file_count']}\n")
        f.write(f"analyzed_file_count: {control['analyzed_file_count']}\n")
        f.write(f"not_searchable_file_count: {control['not_searchable_file_count']}\n")
        f.write(f"searchable_file_count: {control['searchable_file_count']}\n")
        ratio_not = (control['not_searchable_file_count'] / rows_total) if rows_total else 0.0
        ratio_yes = (control['searchable_file_count'] / rows_total) if rows_total else 0.0
        f.write(f"not_searchable_file_ratio: {ratio_not:.6f}\n")
        f.write(f"searchable_file_ratio: {ratio_yes:.6f}\n")
        f.write(f"max_size_bytes_not_searchable: {control['max_size_bytes_not_searchable']}\n")
        f.write(f"max_size_human_not_searchable: {control['max_size_human_not_searchable']}\n")
        f.write(f"min_size_bytes_searchable: {control['min_size_bytes_searchable']}\n")
        f.write(f"min_size_human_searchable: {control['min_size_human_searchable']}\n")
        f.write(f"threshold_may_need_review: {control['threshold_may_need_review']}\n")
        if control.get('tokenizer_error'):
            f.write(f"tokenizer_error: {control['tokenizer_error']}\n")

        error_rows = [r for r in analyzed_rows if r.get("error")]
        if error_rows:
            f.write("\nanalysis_errors:\n")
            for row in error_rows:
                f.write(
                    f"size_bytes={row['size_bytes']} | src_kind={row.get('src_kind')} | "
                    f"txt_rel={row['txt_rel']} | txt_abs={row['txt_abs']} | error={row['error']}\n"
                )

        f.write("\nDOCUMENTS WITH token_count_e5 < 20 (НЕ ПОПАДУТ В ПОИСК):\n")
        for row in not_searchable_rows:
            f.write(
                f"size_bytes={row['size_bytes']} | size_human={row['size_human']} | "
                f"token_count_e5={row['token_count_e5']} | src_kind={row.get('src_kind')} | "
                f"txt_rel={row['txt_rel']} | txt_abs={row['txt_abs']}\n"
            )
            f.write("TEXT_BEGIN\n")
            f.write((row.get("text_for_report") or "") + "\n")
            f.write("TEXT_END\n\n")

        f.write("\nDOCUMENTS WITH token_count_e5 >= 20 (ПОПАДУТ В ПОИСК):\n")
        for row in searchable_rows:
            f.write(
                f"size_bytes={row['size_bytes']} | size_human={row['size_human']} | "
                f"token_count_e5={row['token_count_e5']} | src_kind={row.get('src_kind')} | "
                f"txt_rel={row['txt_rel']} | txt_abs={row['txt_abs']}\n"
            )
            f.write("TEXT_BEGIN\n")
            f.write((row.get("text_for_report") or "") + "\n")
            f.write("TEXT_END\n\n")


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def make_stage1_doc_id_and_paths(root: Path, p: Path):
    rel_path = p.relative_to(root).as_posix()
    full_path = str(p.resolve())
    doc_id = hashlib.sha1(rel_path.encode("utf-8")).hexdigest()[:16]
    return doc_id, rel_path, full_path


def build_txt_index(scan_root: Path):
    index = {}
    collisions = Counter()
    for p in scan_root.rglob("*.txt"):
        stem = p.stem
        if stem not in index:
            index[stem] = p
        else:
            collisions[stem] += 1
    return index, collisions


def write_jsonl(path: Path, rows):
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def make_base_manifest_row(lib_root: Path, txt_path: Path):
    txt_rel = txt_path.relative_to(lib_root).as_posix()
    sid = sha1_hex(txt_rel)[:16]
    doc_id = sid
    txt_abs = str(txt_path.resolve())
    uid = sha1_hex(txt_rel)
    size_bytes = txt_path.stat().st_size
    src_kind = detect_src_kind_from_txt_rel(txt_rel)

    row = {}
    row["sid"] = sid
    row["doc_id"] = doc_id
    row["txt_rel"] = txt_rel
    row["txt_abs"] = txt_abs
    row["src_kind"] = src_kind
    row["src_rel"] = ""
    row["src_exists"] = False
    row["uid"] = uid
    row["size_bytes"] = size_bytes
    return row


def make_stage1_row(lib_root: Path, txt_path: Path):
    stem = txt_path.stem
    year, rest = extract_year(stem)
    authors, title, author_parse_mode = split_authors_title(rest)
    doc_id16, rel_path, full_path = make_stage1_doc_id_and_paths(lib_root, txt_path)

    return {
        "doc_id": doc_id16,
        "rel_path": rel_path,
        "full_path": full_path,
        "file": txt_path.name,
        "stem": stem,
        "year": year,
        "name_after_year": rest,
        "authors": authors,
        "title": title,
        "author_parse_mode": author_parse_mode,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lib-root", default=DEFAULT_LIB_ROOT)
    parser.add_argument("--out", default=DEFAULT_OUTPUT_MANIFEST)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--context-chars", type=int, default=DEFAULT_CONTEXT_CHARS)
    parser.add_argument("--max-udc", type=int, default=DEFAULT_MAX_UDC)
    parser.add_argument("--early-limit", type=int, default=DEFAULT_EARLY_LIMIT)
    parser.add_argument("--year-head-chars", type=int, default=DEFAULT_YEAR_HEAD_CHARS)
    parser.add_argument("--e5-small-scan-max-bytes", type=int, default=DEFAULT_E5_SMALL_SCAN_MAX_BYTES)
    parser.add_argument("--e5-min-search-tokens", type=int, default=DEFAULT_E5_MIN_SEARCH_TOKENS)
    parser.add_argument("--e5-tokenizer-name", default=DEFAULT_E5_TOKENIZER_NAME)
    args = parser.parse_args()

    lib_root = Path(args.lib_root)
    scan_root = lib_root / "OCR_TXT"
    out_manifest = Path(args.out)
    output_dir = Path(args.output_dir)
    report_txt = output_dir / Path(DEFAULT_REPORT_TXT).name
    report_json = output_dir / Path(DEFAULT_REPORT_JSON).name
    debug_txt = output_dir / Path(DEFAULT_DEBUG_TXT).name
    merge_log_jsonl = output_dir / Path(DEFAULT_MERGE_LOG_JSONL).name
    same_size_same_content_manifest_jsonl = output_dir / Path(DEFAULT_SAME_SIZE_SAME_CONTENT_MANIFEST_JSONL).name
    same_size_same_content_report_txt = output_dir / Path(DEFAULT_SAME_SIZE_SAME_CONTENT_REPORT_TXT).name
    size_distribution_report_txt = output_dir / Path(DEFAULT_SIZE_DISTRIBUTION_REPORT_TXT).name
    e5_small_files_report_txt = output_dir / Path(DEFAULT_E5_SMALL_FILES_REPORT_TXT).name

    output_dir.mkdir(parents=True, exist_ok=True)
    out_manifest.parent.mkdir(parents=True, exist_ok=True)

    txt_files = sorted(
        [p for p in scan_root.rglob("*.txt")],
        key=lambda p: p.relative_to(lib_root).as_posix().lower()
    )
    txt_index, collisions = build_txt_index(scan_root)

    stage1_year_found = 0
    stage1_year_missing = 0
    stage1_year_unknown_marker = 0
    stage1_authors_found = 0
    stage1_authors_missing = 0
    parse_mode_counter = Counter()
    src_kind_counter = Counter()

    total_rows = 0
    text_read_ok = 0
    text_not_found = 0
    text_read_error = 0

    udc_found_rows = 0
    udc_not_found_rows = 0
    accepted_udk_zone_counter = Counter()
    accepted_udk_counter = Counter()
    udk_status_counter = Counter()

    bbk_found_rows = 0
    bbk_not_found_rows = 0
    accepted_bbk_zone_counter = Counter()
    accepted_bbk_counter = Counter()
    bbk_status_counter = Counter()

    found_udk_count_counter = Counter()
    found_bbk_count_counter = Counter()
    year_stg2_found_rows = 0

    ok_id = 0
    ok_abs = 0
    ok_rel = 0
    year_from_stg1 = 0
    year_from_stg2 = 0
    year_fallback = 0

    out_rows = []
    debug_lines = []
    merge_log_rows = []

    total = len(txt_files)

    for idx, txt_path in enumerate(txt_files, 1):
        total_rows += 1

        base_row = make_base_manifest_row(lib_root, txt_path)
        stage1_row = make_stage1_row(lib_root, txt_path)
        src_kind_counter[base_row["src_kind"] or "not_determined"] += 1

        # Основной doc_id теперь равен короткому стабильному id от относительного пути.
        # Старый doc_id=stem больше не используется как идентификатор.
        base_row["doc_id"] = stage1_row["doc_id"]

        year1 = stage1_row["year"]
        if year1 is None:
            stage1_year_missing += 1
        else:
            stage1_year_found += 1
            if year1 == 1111:
                stage1_year_unknown_marker += 1

        if stage1_row["authors"]:
            stage1_authors_found += 1
        else:
            stage1_authors_missing += 1

        parse_mode_counter[stage1_row["author_parse_mode"]] += 1

        stem = stage1_row["stem"]
        src_path = txt_index.get(stem)

        if not src_path:
            text_not_found += 1
            final_udk_status = "not_found"
            final_bbk_status = "not_found"
            accepted_udk = ""
            accepted_bbk = ""
            year2 = ""

            udk_status_counter[final_udk_status] += 1
            bbk_status_counter[final_bbk_status] += 1
            found_udk_count_counter[0] += 1
            found_bbk_count_counter[0] += 1
        else:
            try:
                text = read_text_file(src_path)
                text_read_ok += 1
            except Exception:
                text_read_error += 1
                text = None

            if text is None:
                final_udk_status = "read_error"
                final_bbk_status = "read_error"
                accepted_udk = ""
                accepted_bbk = ""
                year2 = ""

                udk_status_counter[final_udk_status] += 1
                bbk_status_counter[final_bbk_status] += 1
                found_udk_count_counter[0] += 1
                found_bbk_count_counter[0] += 1
            else:
                all_udk_hits = extract_udk_hits_full_text(text, args.context_chars)
                udk_search_status = "ok"
                if not all_udk_hits:
                    all_udk_hits = extract_udk_hits_group1(text, args.context_chars)
                    if all_udk_hits:
                        udk_search_status = "found_group1"
                    else:
                        udk_search_status = "not_found"

                accepted_udk, udk_trust_weight, accepted_udk_zone, _ = choose_accepted_code(
                    all_udk_hits, args.early_limit, args.max_udc, "udk", clean_udk_for_compare
                )
                total_udk_found = len(all_udk_hits)
                if accepted_udk:
                    udc_found_rows += 1
                    accepted_udk_zone_counter[accepted_udk_zone] += 1
                    accepted_udk_counter[accepted_udk] += 1
                else:
                    udc_not_found_rows += 1
                found_udk_count_counter[min(total_udk_found, args.max_udc)] += 1

                all_bbk_hits = extract_bbk_hits_full_text(text, args.context_chars)
                bbk_search_status = "ok" if all_bbk_hits else "not_found"

                accepted_bbk, bbk_trust_weight, accepted_bbk_zone, _ = choose_accepted_code(
                    all_bbk_hits, args.early_limit, args.max_udc, "bbk", clean_bbk_for_compare
                )
                total_bbk_found = len(all_bbk_hits)
                if accepted_bbk:
                    bbk_found_rows += 1
                    accepted_bbk_zone_counter[accepted_bbk_zone] += 1
                    accepted_bbk_counter[accepted_bbk] += 1
                else:
                    bbk_not_found_rows += 1
                found_bbk_count_counter[min(total_bbk_found, args.max_udc)] += 1

                final_udk_status = udk_search_status if not accepted_udk else ("found_group1" if udk_search_status == "found_group1" else "ok")
                final_bbk_status = bbk_search_status if not accepted_bbk else "ok"

                need_year_stg2 = year1 in (None, "", 1111, "1111")
                year2 = choose_year_from_text_head(text, args.year_head_chars) if need_year_stg2 else ""
                if year2 != "":
                    year_stg2_found_rows += 1

                udk_status_counter[final_udk_status] += 1
                bbk_status_counter[final_bbk_status] += 1

                for hit in all_udk_hits:
                    debug_lines.append(f"UDK | {stage1_row['file']} | {hit['udk']} | {hit['pos_from_start']}")
                for hit in all_bbk_hits:
                    debug_lines.append(f"BBK | {stage1_row['file']} | {hit['bbk']} | {hit['pos_from_start']}")

        expected_rel = str(stage1_row["rel_path"]).lstrip("/")
        id_ok = base_row["doc_id"] == stage1_row["doc_id"]
        abs_ok = norm(base_row["txt_abs"]) == norm(stage1_row["full_path"])
        rel_ok = norm(base_row["txt_rel"]) == norm(expected_rel)

        if id_ok:
            ok_id += 1
        if abs_ok:
            ok_abs += 1
        if rel_ok:
            ok_rel += 1

        if is_valid_year_for_merge(year1):
            year = str(year1).strip()
            year_from_stg1 += 1
        elif is_valid_year_for_merge(year2):
            year = str(year2).strip()
            year_from_stg2 += 1
        else:
            year = None if year1 is None else str(year1).strip()
            year_fallback += 1

        row = {}
        row["sid"] = base_row["sid"]
        row["doc_id"] = base_row["doc_id"]
        row["txt_rel"] = base_row["txt_rel"]
        row["txt_abs"] = base_row["txt_abs"]
        row["src_kind"] = base_row["src_kind"]
        row["src_rel"] = base_row["src_rel"]
        row["src_exists"] = base_row["src_exists"]
        row["uid"] = base_row["uid"]
        row["size_bytes"] = base_row["size_bytes"]
        row["rel_path_stg1"] = stage1_row["rel_path"]
        row["authors"] = stage1_row["authors"]
        row["title"] = stage1_row["title"]
        row["year"] = year
        row["udc"] = accepted_udk
        row["bbk"] = accepted_bbk
        out_rows.append(row)

        if not (id_ok and abs_ok and rel_ok):
            merge_log_rows.append({
                "line": idx,
                "doc_id_ok": id_ok,
                "txt_abs_ok": abs_ok,
                "txt_rel_ok": rel_ok,
                "manifest_doc_id": base_row.get("doc_id"),
                "stage2_stem_stg1": stage1_row.get("stem"),
                "manifest_txt_abs": base_row.get("txt_abs"),
                "stage2_full_path_stg1": stage1_row.get("full_path"),
                "manifest_txt_rel": base_row.get("txt_rel"),
                "expected_txt_rel": expected_rel
            })

        if idx % PROGRESS_EVERY == 0 or idx == total:
            print(f"processed {idx}/{total}")

    write_jsonl(out_manifest, out_rows)
    if merge_log_rows:
        write_jsonl(merge_log_jsonl, merge_log_rows)

    same_size_same_content_rows, same_size_same_content_group_summaries, same_size_control = build_same_size_rows(out_rows)
    write_jsonl(same_size_same_content_manifest_jsonl, same_size_same_content_rows)
    write_same_size_report_txt(
        same_size_same_content_report_txt,
        "make_manifest same-size same-content report",
        same_size_same_content_group_summaries,
        same_size_same_content_rows,
        same_size_control,
    )

    size_distribution_buckets = build_size_distribution_rows(out_rows)
    size_distribution_control = {
        "small_file_max_bytes": 65536,
        "small_file_count": sum(1 for r in out_rows if (r.get("size_bytes") is not None and r.get("size_bytes") <= 65536)),
    }
    write_size_distribution_report_txt(size_distribution_report_txt, size_distribution_buckets, total_rows, size_distribution_control)

    e5_analyzed_rows, e5_not_searchable_rows, e5_searchable_rows, e5_control = build_e5_small_files_report_rows(
        out_rows,
        scan_max_bytes=args.e5_small_scan_max_bytes,
        min_search_tokens=args.e5_min_search_tokens,
        tokenizer_name=args.e5_tokenizer_name,
    )
    write_e5_small_files_report_txt(
        e5_small_files_report_txt,
        e5_analyzed_rows,
        e5_not_searchable_rows,
        e5_searchable_rows,
        e5_control,
        total_rows,
    )

    summary = {
        "program": PROGRAM,
        "version": VERSION,
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "lib_root": str(lib_root),
        "scan_root": str(scan_root),
        "out_manifest": str(out_manifest),
        "rows_total": total_rows,
        "doc_id_eq_stem_stg1_ok": ok_id,
        "txt_abs_eq_full_path_stg1_ok": ok_abs,
        "txt_rel_eq_rel_path_stg1_ok": ok_rel,
        "mismatch_rows_logged": len(merge_log_rows),
        "year_from_stg1": year_from_stg1,
        "year_from_stg2": year_from_stg2,
        "year_fallback_to_raw_stg1": year_fallback,
        "stage1_year_found": stage1_year_found,
        "stage1_year_missing": stage1_year_missing,
        "stage1_year_unknown_marker_1111": stage1_year_unknown_marker,
        "stage1_authors_found": stage1_authors_found,
        "stage1_authors_missing": stage1_authors_missing,
        "text_read_ok": text_read_ok,
        "text_not_found": text_not_found,
        "text_read_error": text_read_error,
        "year_stg2_found_rows": year_stg2_found_rows,
        "udc_found_rows": udc_found_rows,
        "udc_not_found_rows": udc_not_found_rows,
        "bbk_found_rows": bbk_found_rows,
        "bbk_not_found_rows": bbk_not_found_rows,
        "unique_collision_stems": len(collisions),
        "src_kind_pdf_rows": src_kind_counter.get("pdf", 0),
        "src_kind_djvu_rows": src_kind_counter.get("djvu", 0),
        "src_kind_not_determined_rows": src_kind_counter.get("not_determined", 0),
        "same_size_same_content_group_count": len(same_size_same_content_group_summaries),
        "same_size_same_content_file_count": len(same_size_same_content_rows),
        "total_files_in_size_groups": same_size_control["total_files_in_size_groups"],
        "total_unique_sizes_with_duplicates": same_size_control["total_unique_sizes_with_duplicates"],
        "size_distribution_bucket_count": len(size_distribution_buckets),
        "small_file_max_bytes": size_distribution_control["small_file_max_bytes"],
        "small_file_count": size_distribution_control["small_file_count"],
        "e5_small_scan_max_bytes": e5_control["e5_small_scan_max_bytes"],
        "e5_min_search_tokens": e5_control["e5_min_search_tokens"],
        "e5_candidate_file_count": e5_control["candidate_file_count"],
        "e5_analyzed_file_count": e5_control["analyzed_file_count"],
        "e5_not_searchable_file_count": e5_control["not_searchable_file_count"],
        "e5_searchable_file_count": e5_control["searchable_file_count"],
    }

    with report_txt.open("w", encoding="utf-8", newline="\n") as f:
        f.write("make_manifest report\n")
        f.write("====================\n\n")
        for k, v in summary.items():
            f.write(f"{k}: {v}\n")

        f.write("\nparse_modes:\n")
        for mode in sorted(parse_mode_counter):
            f.write(f"{mode}: {parse_mode_counter[mode]}\n")

        f.write("\nudc_stage2_status_counts:\n")
        for k in sorted(udk_status_counter):
            f.write(f"{k}: {udk_status_counter[k]}\n")

        f.write("\nudc_accepted_zone_counts:\n")
        for k in sorted(accepted_udk_zone_counter):
            f.write(f"{k}: {accepted_udk_zone_counter[k]}\n")

        f.write("\nrows_by_total_udc_found_capped_to_4:\n")
        for k in sorted(found_udk_count_counter):
            f.write(f"{k}: {found_udk_count_counter[k]}\n")

        f.write("\ntop_accepted_udk_values:\n")
        for value, cnt in accepted_udk_counter.most_common(100):
            f.write(f"{value}: {cnt}\n")

        f.write("\n--------------------------------\n")

        f.write("\nbbk_stage2_status_counts:\n")
        for k in sorted(bbk_status_counter):
            f.write(f"{k}: {bbk_status_counter[k]}\n")

        f.write("\nbbk_accepted_zone_counts:\n")
        for k in sorted(accepted_bbk_zone_counter):
            f.write(f"{k}: {accepted_bbk_zone_counter[k]}\n")

        f.write("\nrows_by_total_bbk_found_capped_to_4:\n")
        for k in sorted(found_bbk_count_counter):
            f.write(f"{k}: {found_bbk_count_counter[k]}\n")

        f.write("\ntop_accepted_bbk_values:\n")
        for value, cnt in accepted_bbk_counter.most_common(100):
            f.write(f"{value}: {cnt}\n")

    with report_json.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    with debug_txt.open("w", encoding="utf-8", newline="\n") as f:
        for line in debug_lines:
            f.write(line + "\n")

    print()
    print("====================================")
    print("MAKE_MANIFEST STATISTICS")
    print("====================================")
    for k, v in summary.items():
        print(f"{k}: {v}")
    print()
    print("Manifest saved:")
    print(out_manifest)
    print()
    print("Report saved:")
    print(report_txt)
    print(report_json)
    print()
    print("Debug saved:")
    print(debug_txt)
    if merge_log_rows:
        print()
        print("Merge log saved:")
        print(merge_log_jsonl)


    print()
    print("Same-size SAME-CONTENT manifest saved:")
    print(same_size_same_content_manifest_jsonl)
    print()
    print("Same-size SAME-CONTENT report saved:")
    print(same_size_same_content_report_txt)
    print()
    print("Size distribution report saved:")
    print(size_distribution_report_txt)
    print()
    print("E5 token-count report saved:")
    print(e5_small_files_report_txt)


if __name__ == "__main__":
    main()
