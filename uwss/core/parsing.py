# uwss/core/parsing.py
from __future__ import annotations
import os
import json
import time
import re
from typing import Optional

import fitz  # PyMuPDF
import trafilatura

from .storage import DB

SAFE_CHARS = re.compile(r"[^a-zA-Z0-9._-]+")


def _safe_name(s: str) -> str:
    s = (s or "").strip().replace("https://openalex.org/", "")
    return SAFE_CHARS.sub("_", s)[:128] or f"item_{int(time.time())}"


def _extract_text_from_pdf(pdf_path: str, max_pages: Optional[int] = None) -> str:
    """Trích text từ PDF bằng PyMuPDF, ổn trên Windows. max_pages=None => toàn bộ."""
    if not os.path.exists(pdf_path):
        return ""
    text_parts = []
    with fitz.open(pdf_path) as doc:
        n = len(doc)
        end = n if max_pages is None else min(max_pages, n)
        for i in range(end):
            try:
                page = doc.load_page(i)
                txt = page.get_text("text")
                if txt:
                    text_parts.append(txt)
            except Exception:
                # tiếp tục trang sau nếu trang này lỗi
                continue
    return "\n".join(text_parts).strip()


def _extract_text_from_html(html_path: str) -> str:
    """Trích text sạch từ HTML với trafilatura (khử boilerplate)."""
    if not os.path.exists(html_path):
        return ""
    try:
        with open(html_path, "rb") as f:
            raw = f.read()
        txt = trafilatura.extract(raw, include_comments=False, include_tables=False)
        return (txt or "").strip()
    except Exception:
        return ""


def extract_one(
    db: DB, item: dict, text_dir: str, max_pdf_pages: Optional[int] = None
) -> dict:
    """
    Với 1 record trong DB:
      - nếu có pdf_path -> extract pdf
      - else nếu có html_path -> extract html
      - lưu vào data/text/<safe_id>.txt
      - cập nhật text_path trong DB nếu có text
    """
    pdf_path = item.get("pdf_path") or ""
    html_path = item.get("html_path") or ""

    if not pdf_path and not html_path:
        return item  # không có gì để extract

    os.makedirs(text_dir, exist_ok=True)
    safe_id = _safe_name(item["id"])
    out_path = os.path.join(text_dir, f"{safe_id}.txt")

    text = ""
    if pdf_path:
        text = _extract_text_from_pdf(pdf_path, max_pages=max_pdf_pages)
    if not text and html_path:
        text = _extract_text_from_html(html_path)

    updated = dict(item)
    if text:
        try:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(text)
            updated["text_path"] = out_path
            # có thể thêm vài thống kê cơ bản nếu muốn
            meta = {}
            try:
                meta = json.loads(updated.get("meta_json") or "{}")
            except Exception:
                meta = {}
            meta["_extract"] = {"chars": len(text)}
            updated["meta_json"] = json.dumps(meta, ensure_ascii=False)
            db.upsert_item(updated)
        except Exception:
            # nếu lưu file lỗi thì bỏ qua
            pass
    return updated
