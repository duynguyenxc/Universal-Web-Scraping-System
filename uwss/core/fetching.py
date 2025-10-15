from __future__ import annotations
import os
import json
import time
import re
from typing import Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import certifi

from .storage import DB

SAFE_CHARS = re.compile(r"[^a-zA-Z0-9._-]+")


def _safe_name(s: str) -> str:
    s = s.strip().replace("https://openalex.org/", "")
    return SAFE_CHARS.sub("_", s)[:128] or f"item_{int(time.time())}"


def _extract_urls(meta_json: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Trả về (pdf_url, landing_url) nếu có từ raw meta_json của OpenAlex.
    """
    try:
        meta = json.loads(meta_json or "{}")
    except Exception:
        return None, None
    primary = meta.get("primary_location") or {}
    pdf_url = primary.get("pdf_url") or None
    landing = primary.get("landing_page_url") or None
    return pdf_url, landing


def _make_session(ua: str, retries: int = 3, backoff: float = 0.5) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": ua})
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _download(
    url: str, out_path: str, ua: str, timeout: int = 30, verify_ssl: bool = True
) -> bool:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    sess = _make_session(ua)
    verify_param = certifi.where() if verify_ssl else False
    try:
        with sess.get(
            url, timeout=timeout, stream=True, verify=verify_param, allow_redirects=True
        ) as r:
            if r.status_code != 200:
                return False
            total = 0
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if not chunk:
                        continue
                    f.write(chunk)
                    total += len(chunk)
            return total > 0
    except requests.exceptions.SSLError:
        # lỗi chứng chỉ → bỏ qua item này
        return False
    except requests.exceptions.RequestException:
        return False


def fetch_one(
    db: DB, item: dict, raw_dir: str, ua: str, verify_ssl: bool = True
) -> dict:
    """
    Cố gắng tải PDF; nếu không có/không tải được, thử HTML landing (nhẹ).
    Trả về row đã cập nhật đường dẫn (nếu tải được).
    """
    pdf_url, landing = _extract_urls(item.get("meta_json", ""))
    safe_id = _safe_name(item["id"])
    updated = dict(item)

    # Ưu tiên PDF
    if pdf_url:
        pdf_path = os.path.join(raw_dir, f"{safe_id}.pdf")
        ok = _download(pdf_url, pdf_path, ua=ua, verify_ssl=verify_ssl)
        if ok:
            updated["pdf_path"] = pdf_path
            db.upsert_item(updated)
            return updated

    # Fallback HTML (để tối thiểu; không parse nội dung ở bước này)
    if landing:
        html_path = os.path.join(raw_dir, f"{safe_id}.html")
        ok = _download(landing, html_path, ua=ua, verify_ssl=verify_ssl)
        if ok:
            updated["html_path"] = html_path
            db.upsert_item(updated)
            return updated

    # Không tải được gì
    return updated
