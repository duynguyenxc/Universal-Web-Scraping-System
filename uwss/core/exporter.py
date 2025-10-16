from __future__ import annotations

import csv
import json
import os
from datetime import datetime
from typing import Dict, Any, List

from ..logger import get_logger


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _now_stamp() -> str:
    # UTC-based timestamp để tên file ổn định, không chứa ký tự đặc biệt
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def _choose_fields(rows: List[Dict[str, Any]]) -> List[str]:
    """
    Chọn tập cột hợp lý để xuất. Nếu DB có schema ổn định thì trả về bộ cột chuẩn.
    Nếu không, hợp nhất keys từ các dòng (tránh lỗi missing).
    """
    if not rows:
        # fallback bộ cột tối thiểu
        return [
            "id",
            "title",
            "year",
            "doi",
            "venue",
            "source_url",
            "pdf_path",
            "html_path",
            "text_path",
            "score",
            "kept",
        ]

    # Ưu tiên bộ cột chuẩn nếu có đủ
    preferred = [
        "id",
        "title",
        "year",
        "doi",
        "venue",
        "source_url",
        "pdf_path",
        "html_path",
        "text_path",
        "score",
        "kept",
    ]
    row_keys = set()
    for r in rows:
        row_keys.update(list(r.keys()))

    if all(k in row_keys for k in preferred):
        return preferred

    # Nếu thiếu, hợp nhất keys sẵn có theo thứ tự ổn định
    merged = sorted(row_keys)
    return merged


def export_rows(
    db,
    out_dir: str,
    fmt: str = "csv",
    only_kept: bool = False,
    log_level: str = "INFO",
    only_with_files: bool = False,
) -> str:
    """
    Xuất dữ liệu ra CSV/JSONL.
    - only_kept: chỉ xuất các dòng kept=1
    - only_with_files: chỉ xuất dòng có pdf_path hoặc html_path
    Trả về: đường dẫn file đã xuất.
    """
    log = get_logger("uwss.export", log_level)

    rows: List[Dict[str, Any]] = []
    for row in db.iter_items():
        if only_kept and int(row.get("kept") or 0) != 1:
            continue
        if only_with_files:
            has_pdf = bool((row.get("pdf_path") or "").strip())
            has_html = bool((row.get("html_path") or "").strip())
            if not (has_pdf or has_html):
                continue
        rows.append(row)

    _ensure_dir(out_dir)
    stamp = _now_stamp()

    if fmt.lower() == "jsonl":
        out_path = os.path.join(out_dir, f"uwss_export_{stamp}.jsonl")
        with open(out_path, "w", encoding="utf-8") as fo:
            for r in rows:
                fo.write(json.dumps(r, ensure_ascii=False) + "\n")
        log.info("exported %d rows -> %s", len(rows), out_path)
        return out_path

    # Mặc định CSV
    out_path = os.path.join(out_dir, f"uwss_export_{stamp}.csv")
    fields = _choose_fields(rows)

    # Ghi CSV với UTF-8 BOM để mở bằng Excel thân thiện
    with open(out_path, "w", encoding="utf-8-sig", newline="") as fo:
        writer = csv.DictWriter(fo, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            # đảm bảo các field thiếu không gây lỗi
            safe = {k: r.get(k, "") for k in fields}
            writer.writerow(safe)

    log.info("exported %d rows -> %s", len(rows), out_path)
    return out_path
