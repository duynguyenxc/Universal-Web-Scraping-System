from __future__ import annotations
import csv
import json
import os
from datetime import datetime

from .storage import DB
from ..logger import get_logger


_EXPORT_COLS = [
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


def _now_tag() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def export_rows(
    db: DB,
    out_dir: str,
    fmt: str = "csv",
    only_kept: bool = False,
    log_level: str = "INFO",
) -> str:
    log = get_logger("uwss.export", log_level)
    os.makedirs(out_dir, exist_ok=True)
    tag = _now_tag()

    rows = []
    for row in db.iter_items():
        if only_kept and int(row.get("kept") or 0) == 0:
            continue
        rows.append({c: row.get(c) for c in _EXPORT_COLS})

    if fmt == "jsonl":
        path = os.path.join(out_dir, f"uwss_export_{tag}.jsonl")
        with open(path, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
    else:
        path = os.path.join(out_dir, f"uwss_export_{tag}.csv")
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=_EXPORT_COLS)
            w.writeheader()
            w.writerows(rows)

    log.info("exported %d rows -> %s", len(rows), path)
    return path
