# -*- coding: utf-8 -*-
#
# MAKE_CHUNKS_FULL.py
#
# Назначение:
#   Преобразование manifest.jsonl в набор чанков JSONL,
#   где каждый чанк соответствует одной странице OCR-текста.
#
# Версия программы нарезания чанков:
#   PROGRAM = "MAKE_CHUNKS_FULL"
#   VERSION = "2026-04-04 v3.2"
#
# Версия генератора манифеста, под которую адаптирована эта программа:
#   MANIFEST_GENERATOR_PROGRAM = "make_manifest"
#   MANIFEST_GENERATOR_VERSION = "1.0"
#   MANIFEST_SCHEMA_VERSION = "2026-04-04 v4.0"
#
# Источник схемы manifest:
#   make_manifest.py
#   PROGRAM = "make_manifest"
#   VERSION = "1.0"
#   MANIFEST_SCHEMA_VERSION = "2026-04-04 v4.0"
#
# Особенности этой версии:
#   1) сохраняет совместимость со старой структурой чанков;
#   2) адаптирована под новый manifest.jsonl версии 2026-04-04 v4.0;
#   3) убрано ложное поле doc_id16, которого больше нет в manifest;
#   4) поле size_bytes из manifest протягивается в чанк как size_bytes_txt;
#   5) не считает метрики качества чанков;
#   6) пишет краткую итоговую статистику в TXT-файл;
#   7) добавляет в каждый чанк служебные поля с названием и версией;
#   8) не меняет формат chunk_id и логику page-based chunking;
#   9) минимальная длина страницы для записи по умолчанию снижена до 20 символов;
#   10) в TXT-отчет добавлены быстрые агрегаты без тяжелой постобработки.
#
# Контракт:
#   manifest.jsonl -> _chunks/chunks_full.jsonl
#
# Эта версия сделана как быстрый этап подготовки чанков.
# Отдельная программа далее может считать статистику, сравнивать версии очистки,
# проверять качество и строить подробные отчеты.
#
# Что считается быстро уже на этом уровне:
#   - количество обработанных документов и чанков;
#   - количество пропущенных коротких страниц;
#   - среднее число страниц и чанков на документ;
#   - доля записанных и пропущенных страниц;
#   - средняя длина записанного чанка в символах;
#   - минимальная и максимальная длина записанного чанка;
#   - число документов, у которых после фильтрации не осталось ни одного чанка;
#   - грубое распределение длин записанных чанков по диапазонам.
#
# Что немного ускорено по сравнению с ранними версиями:
#   - regex для normalize скомпилированы заранее;
#   - убран лишний resolve() при сборке fallback-пути;
#   - локально привязаны write/json.dumps внутри горячего цикла.
#

import re
import json
import time
import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional


PROGRAM = "MAKE_CHUNKS_FULL"
VERSION = "2026-04-04 v3.2"

# Версия генератора манифеста, под которую адаптирована программа.
# Эти значения взяты из make_manifest.py, предоставленного пользователем.
MANIFEST_GENERATOR_PROGRAM = "make_manifest"
MANIFEST_GENERATOR_VERSION = "1.0"
MANIFEST_SCHEMA_VERSION = "2026-04-04 v4.0"


# Поля, которые старая версия уже использовала и писала в чанк.
# Их стараемся сохранить без изменений, чтобы не ломать downstream-пайплайн.
LEGACY_CHUNK_FIELDS = (
    "chunk_id",
    "sid",
    "doc_id",
    "uid",
    "topic",
    "txt_sha1",
    "txt_rel",
    "txt_abs",
    "src_kind",
    "src_rel",
    "src_exists",
    "page_start",
    "page_end",
    "text",
)

# Поля из нового manifest, которые протаскиваем в чанк.
# ВАЖНО:
# - doc_id16 убран, потому что в новом manifest его больше нет;
# - size_bytes из manifest переименовываем в size_bytes_txt,
#   чтобы было ясно: это размер исходного TXT-файла, а не размер чанка.
EXTRA_MANIFEST_FIELDS = (
    "rel_path_stg1",
    "authors",
    "title",
    "year",
    "udc",
    "bbk",
    "size_bytes_txt",
)

# Служебные поля версии/происхождения схемы.
COMMON_META_FIELDS = (
    "manifest_generator_program",
    "manifest_generator_version",
    "manifest_schema_version",
    "chunk_generator_program",
    "chunk_generator_version",
)

# Грубые диапазоны длины записанных чанков в символах.
CHUNK_LEN_BUCKETS = (
    (20, 49, "20-49"),
    (50, 99, "50-99"),
    (100, 499, "100-499"),
    (500, 999, "500-999"),
    (1000, None, "1000+"),
)

# Регулярки compile один раз, чтобы не создавать их заново на каждом файле.
HYPHEN_LINEBREAK_RE = re.compile(r"(\w)-\n(\w)", flags=re.UNICODE)
TRAILING_SPACE_BEFORE_NL_RE = re.compile(r"[ \t]+\n")
MANY_BLANK_LINES_RE = re.compile(r"\n{3,}")


def read_text(path: Path) -> str:
    """Чтение OCR-текста с попыткой нескольких кодировок."""
    for enc in ("utf-8", "utf-8-sig", "cp1251"):
        try:
            return path.read_text(encoding=enc, errors="strict")
        except Exception:
            pass
    return path.read_text(encoding="utf-8", errors="ignore")


def normalize(text: str) -> str:
    """Мягкая нормализация OCR-текста без агрессивного разрушения структуры."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = HYPHEN_LINEBREAK_RE.sub(r"\1\2", text)
    text = TRAILING_SPACE_BEFORE_NL_RE.sub("\n", text)
    text = MANY_BLANK_LINES_RE.sub("\n\n", text)
    return text.strip()


def split_pages(text: str) -> List[str]:
    """Деление OCR-текста на страницы по form feed (\f)."""
    if "\f" in text:
        return [p.strip() for p in text.split("\f")]
    text = text.strip()
    return [text] if text else []


def norm_slashes(value: Optional[str]) -> Optional[str]:
    """Приведение путей к формату со слешами '/' для JSONL."""
    if isinstance(value, str):
        return value.replace("\\", "/")
    return value


def clean_scalar(value: Any) -> Any:
    """Легкая очистка скалярных значений из manifest без изменения типа."""
    if isinstance(value, str):
        value = value.strip()
        return value if value != "" else None
    return value


def build_chunk_record(
    rec: Dict[str, Any],
    sid: str,
    doc_id: str,
    txt_rel: str,
    txt_abs: Path,
    page_no: int,
    page_text: str,
) -> Dict[str, Any]:
    """Формирование записи чанка с сохранением старой схемы и добавлением новых полей."""

    src_rel = rec.get("src_rel")

    out_rec: Dict[str, Any] = {
        # legacy-структура — не менять
        "chunk_id": f"{sid}::p{page_no:05d}",
        "sid": sid,
        "doc_id": doc_id,
        "uid": rec.get("uid"),
        # Эти два ключа сохраняем для совместимости со старым downstream,
        # даже если в новом manifest их уже может не быть.
        "topic": rec.get("topic"),
        "txt_sha1": rec.get("txt_sha1"),
        "txt_rel": norm_slashes(txt_rel),
        "txt_abs": str(txt_abs),
        "src_kind": rec.get("src_kind"),
        "src_rel": norm_slashes(src_rel) if isinstance(src_rel, str) else None,
        "src_exists": bool(rec.get("src_exists", False)),
        "page_start": page_no,
        "page_end": page_no,
        "text": page_text,
    }

    # Поля manifest.
    out_rec["rel_path_stg1"] = norm_slashes(clean_scalar(rec.get("rel_path_stg1")))
    out_rec["authors"] = clean_scalar(rec.get("authors"))
    out_rec["title"] = clean_scalar(rec.get("title"))
    out_rec["year"] = clean_scalar(rec.get("year"))
    out_rec["udc"] = clean_scalar(rec.get("udc"))
    out_rec["bbk"] = clean_scalar(rec.get("bbk"))
    out_rec["size_bytes_txt"] = rec.get("size_bytes")

    # Служебные поля версии/происхождения.
    out_rec["manifest_generator_program"] = MANIFEST_GENERATOR_PROGRAM
    out_rec["manifest_generator_version"] = MANIFEST_GENERATOR_VERSION
    out_rec["manifest_schema_version"] = MANIFEST_SCHEMA_VERSION
    out_rec["chunk_generator_program"] = PROGRAM
    out_rec["chunk_generator_version"] = VERSION

    return out_rec


def format_seconds(seconds: float) -> str:
    """Красивое представление времени для TXT-отчета."""
    total = int(round(seconds))
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def bucket_label_for_chunk_len(text_len: int) -> str:
    """Возвращает название диапазона длины чанка."""
    for start, end, label in CHUNK_LEN_BUCKETS:
        if text_len < start:
            continue
        if end is not None and text_len > end:
            continue
        return label
    return "OUT_OF_RANGE"


def write_stats_txt(
    stats_path: Path,
    manifest_path: Path,
    out_path: Path,
    min_chars: int,
    total_docs: int,
    total_pages: int,
    written: int,
    missing_txt: int,
    bad_json: int,
    skipped_no_txt_rel: int,
    skipped_short_pages: int,
    docs_with_zero_written_chunks: int,
    sum_chunk_chars: int,
    min_written_chunk_chars: int,
    max_chunk_chars: int,
    chunk_len_buckets: Dict[str, int],
    elapsed_sec: float,
) -> None:
    """Запись краткой итоговой статистики в TXT."""
    docs_per_sec = total_docs / elapsed_sec if elapsed_sec > 0 else 0.0
    chunks_per_sec = written / elapsed_sec if elapsed_sec > 0 else 0.0

    avg_pages_per_doc = (total_pages / total_docs) if total_docs > 0 else 0.0
    avg_chunks_per_doc = (written / total_docs) if total_docs > 0 else 0.0
    chunk_write_ratio = (written / total_pages) if total_pages > 0 else 0.0
    skipped_short_pages_ratio = (skipped_short_pages / total_pages) if total_pages > 0 else 0.0
    avg_chars_per_written_chunk = (sum_chunk_chars / written) if written > 0 else 0.0

    min_chunk_for_report = min_written_chunk_chars if written > 0 else 0
    max_chunk_for_report = max_chunk_chars if written > 0 else 0

    with stats_path.open("w", encoding="utf-8", newline="\n") as f:
        f.write("MAKE_CHUNKS_FULL statistics\n")
        f.write("===========================\n\n")
        f.write(f"program: {PROGRAM}\n")
        f.write(f"version: {VERSION}\n")
        f.write(f"manifest_generator_program: {MANIFEST_GENERATOR_PROGRAM}\n")
        f.write(f"manifest_generator_version: {MANIFEST_GENERATOR_VERSION}\n")
        f.write(f"manifest_schema_version: {MANIFEST_SCHEMA_VERSION}\n\n")
        f.write(f"manifest_path: {manifest_path}\n")
        f.write(f"chunks_out_path: {out_path}\n")
        f.write(f"stats_txt_path: {stats_path}\n")
        f.write(f"min_chars_threshold: {min_chars}\n\n")

        f.write("COUNTS\n")
        f.write("------\n")
        f.write(f"docs_processed: {total_docs}\n")
        f.write(f"pages_total: {total_pages}\n")
        f.write(f"written_chunks: {written}\n")
        f.write(f"missing_txt: {missing_txt}\n")
        f.write(f"bad_json: {bad_json}\n")
        f.write(f"skipped_no_txt_rel: {skipped_no_txt_rel}\n")
        f.write(f"skipped_short_pages: {skipped_short_pages}\n")
        f.write(f"docs_with_zero_written_chunks: {docs_with_zero_written_chunks}\n\n")

        f.write("AVERAGES AND RATIOS\n")
        f.write("-------------------\n")
        f.write(f"avg_pages_per_doc: {avg_pages_per_doc:.3f}\n")
        f.write(f"avg_chunks_per_doc: {avg_chunks_per_doc:.3f}\n")
        f.write(f"chunk_write_ratio: {chunk_write_ratio:.6f}\n")
        f.write(f"skipped_short_pages_ratio: {skipped_short_pages_ratio:.6f}\n")
        f.write(f"avg_chars_per_written_chunk: {avg_chars_per_written_chunk:.3f}\n")
        f.write(f"min_written_chunk_chars: {min_chunk_for_report}\n")
        f.write(f"max_chunk_chars: {max_chunk_for_report}\n\n")

        f.write("CHUNK LENGTH BUCKETS (written chunks only)\n")
        f.write("-----------------------------------------\n")
        for _, _, label in CHUNK_LEN_BUCKETS:
            count = chunk_len_buckets.get(label, 0)
            ratio = (count / written) if written > 0 else 0.0
            f.write(f"{label}: {count} ({ratio:.6f})\n")
        f.write("\n")

        f.write("PERFORMANCE\n")
        f.write("-----------\n")
        f.write(f"elapsed_seconds: {elapsed_sec:.3f}\n")
        f.write(f"elapsed_hms: {format_seconds(elapsed_sec)}\n")
        f.write(f"docs_per_second: {docs_per_sec:.3f}\n")
        f.write(f"chunks_per_second: {chunks_per_sec:.3f}\n")


def main() -> None:
    ap = argparse.ArgumentParser()

    ap.add_argument(
        "--lib",
        default=r"C:\LIB4",
        help="Корень библиотеки, где manifest и OCR_TXT",
    )
    ap.add_argument(
        "--manifest",
        default="manifest.jsonl",
        help="Файл manifest (относительно --lib или абсолютный путь)",
    )
    ap.add_argument(
        "--out",
        default=r"_chunks\chunks_full.jsonl",
        help="Куда писать chunks_full.jsonl",
    )
    ap.add_argument(
        "--min-chars",
        type=int,
        default=20,
        help="Минимальная длина страницы в символах; более короткие страницы пропускаются",
    )
    ap.add_argument(
        "--report-every",
        type=int,
        default=100,
        help="Печатать прогресс каждые N документов; 0 = не печатать",
    )
    args = ap.parse_args()

    t0 = time.perf_counter()

    lib = Path(args.lib)

    manifest = Path(args.manifest)
    if not manifest.is_absolute():
        manifest = lib / manifest

    out = Path(args.out)
    if not out.is_absolute():
        out = lib / out
    out.parent.mkdir(parents=True, exist_ok=True)

    # TXT со статистикой кладем рядом с итоговым JSONL.
    # Имя делаем предсказуемым и отдельным.
    stats_txt = out.with_name(out.stem + "_stats.txt")

    total_docs = 0
    total_pages = 0
    written = 0
    missing_txt = 0
    bad_json = 0
    skipped_no_txt_rel = 0
    skipped_short_pages = 0
    docs_with_zero_written_chunks = 0

    # Быстрые агрегаты по длине записанных чанков.
    sum_chunk_chars = 0
    min_written_chunk_chars = 0
    max_chunk_chars = 0
    chunk_len_buckets: Dict[str, int] = {label: 0 for _, _, label in CHUNK_LEN_BUCKETS}

    with manifest.open("r", encoding="utf-8", errors="ignore") as f_in, \
         out.open("w", encoding="utf-8", newline="\n") as f_out:

        write_line = f_out.write
        json_dumps = json.dumps

        for line_no, line in enumerate(f_in, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                rec = json.loads(line)
            except Exception:
                bad_json += 1
                continue

            txt_rel = rec.get("txt_rel")
            if not txt_rel:
                skipped_no_txt_rel += 1
                continue

            # Предпочитаем путь txt_abs из manifest.
            # Если его нет, собираем путь из --lib и txt_rel.
            txt_abs_str = rec.get("txt_abs")
            if txt_abs_str:
                txt_abs = Path(txt_abs_str)
            else:
                txt_abs = Path(txt_rel)
                if not txt_abs.is_absolute():
                    txt_abs = lib / txt_rel

            if not txt_abs.exists():
                missing_txt += 1
                continue

            sid = rec.get("sid") or txt_abs.stem
            doc_id = rec.get("doc_id") or txt_abs.stem

            txt = normalize(read_text(txt_abs))
            pages = split_pages(txt)

            total_docs += 1
            written_before_doc = written

            for page_no, page_text in enumerate(pages, start=1):
                total_pages += 1
                page_text = page_text.strip()
                page_len = len(page_text)
                if page_len < args.min_chars:
                    skipped_short_pages += 1
                    continue

                out_rec = build_chunk_record(
                    rec=rec,
                    sid=sid,
                    doc_id=doc_id,
                    txt_rel=txt_rel,
                    txt_abs=txt_abs,
                    page_no=page_no,
                    page_text=page_text,
                )

                write_line(json_dumps(out_rec, ensure_ascii=False) + "\n")
                written += 1

                sum_chunk_chars += page_len
                if written == 1:
                    min_written_chunk_chars = page_len
                    max_chunk_chars = page_len
                else:
                    if page_len < min_written_chunk_chars:
                        min_written_chunk_chars = page_len
                    if page_len > max_chunk_chars:
                        max_chunk_chars = page_len

                bucket_label = bucket_label_for_chunk_len(page_len)
                chunk_len_buckets[bucket_label] = chunk_len_buckets.get(bucket_label, 0) + 1

            if written == written_before_doc:
                docs_with_zero_written_chunks += 1

            if args.report_every and total_docs % args.report_every == 0:
                print(
                    f"progress docs={total_docs} pages={total_pages} "
                    f"written_chunks={written} missing_txt={missing_txt} "
                    f"bad_json={bad_json} skipped_no_txt_rel={skipped_no_txt_rel} "
                    f"skipped_short_pages={skipped_short_pages} "
                    f"docs_with_zero_written_chunks={docs_with_zero_written_chunks}",
                    flush=True,
                )

    elapsed_sec = time.perf_counter() - t0
    chunks_per_sec = written / elapsed_sec if elapsed_sec > 0 else 0.0

    write_stats_txt(
        stats_path=stats_txt,
        manifest_path=manifest,
        out_path=out,
        min_chars=args.min_chars,
        total_docs=total_docs,
        total_pages=total_pages,
        written=written,
        missing_txt=missing_txt,
        bad_json=bad_json,
        skipped_no_txt_rel=skipped_no_txt_rel,
        skipped_short_pages=skipped_short_pages,
        docs_with_zero_written_chunks=docs_with_zero_written_chunks,
        sum_chunk_chars=sum_chunk_chars,
        min_written_chunk_chars=min_written_chunk_chars,
        max_chunk_chars=max_chunk_chars,
        chunk_len_buckets=chunk_len_buckets,
        elapsed_sec=elapsed_sec,
    )

    print(
        "OK "
        f"docs={total_docs} "
        f"pages={total_pages} "
        f"written_chunks={written} "
        f"missing_txt={missing_txt} "
        f"bad_json={bad_json} "
        f"skipped_no_txt_rel={skipped_no_txt_rel} "
        f"skipped_short_pages={skipped_short_pages} "
        f"docs_with_zero_written_chunks={docs_with_zero_written_chunks} "
        f"elapsed_sec={elapsed_sec:.3f} "
        f"chunks_per_sec={chunks_per_sec:.3f}",
        flush=True,
    )
    print(f"Written: {out}", flush=True)
    print(f"Stats TXT: {stats_txt}", flush=True)
    print("Legacy fields preserved:", ", ".join(LEGACY_CHUNK_FIELDS), flush=True)
    print("Extra manifest fields added:", ", ".join(EXTRA_MANIFEST_FIELDS), flush=True)
    print("Common meta fields added:", ", ".join(COMMON_META_FIELDS), flush=True)


if __name__ == "__main__":
    main()
