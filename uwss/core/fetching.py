# uwss/core/fetching.py
from __future__ import annotations
import os
import json
import time
import re
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import certifi

from .storage import DB
from uwss.schemas.location import Location
from uwss.registry import locations_from_meta

SAFE_CHARS = re.compile(r"[^a-zA-Z0-9._-]+")


def _safe_name(s: str) -> str:
    s = s.strip().replace("https://openalex.org/", "")
    return SAFE_CHARS.sub("_", s)[:128] or f"item_{int(time.time())}"


def _make_session(ua: str, retries: int = 3, backoff: float = 0.5) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": ua, "Accept": "*/*"})
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


def _is_real_pdf(path: str) -> bool:
    """Xác minh file thực sự là PDF (magic header %PDF)."""
    try:
        with open(path, "rb") as f:
            head = f.read(4)
        return head == b"%PDF"
    except OSError:
        return False


def _head_content_type(
    sess: requests.Session, url: str, timeout: int, verify_ssl: bool
) -> str:
    try:
        resp = sess.head(
            url,
            timeout=timeout,
            allow_redirects=True,
            verify=(certifi.where() if verify_ssl else False),
        )
        return (resp.headers.get("Content-Type") or "").lower()
    except requests.RequestException:
        return ""


def _download(
    sess: requests.Session,
    url: str,
    out_path: str,
    timeout: int = 30,
    verify_ssl: bool = True,
) -> bool:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
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
    except requests.exceptions.RequestException:
        return False


def _try_pdf(
    sess, loc: Location, base_path: str, timeout: int, verify_ssl: bool
) -> Optional[str]:
    if not loc.pdf_url:
        return None
    pdf_path = f"{base_path}.pdf"
    ctype = _head_content_type(
        sess, loc.pdf_url, timeout=timeout, verify_ssl=verify_ssl
    )
    is_pdf_like = (
        ("application/pdf" in ctype) if ctype else True
    )  # nếu HEAD không trả type, vẫn thử
    if is_pdf_like and _download(
        sess, loc.pdf_url, pdf_path, timeout=timeout, verify_ssl=verify_ssl
    ):
        if _is_real_pdf(pdf_path):
            return pdf_path
        try:
            if os.path.exists(pdf_path):
                os.remove(pdf_path)
        except OSError:
            pass
    return None


def _try_html(
    sess, loc: Location, base_path: str, timeout: int, verify_ssl: bool
) -> Optional[str]:
    if not loc.html_url:
        return None
    html_path = f"{base_path}.html"
    if _download(sess, loc.html_url, html_path, timeout=timeout, verify_ssl=verify_ssl):
        return html_path
    return None


def fetch_one(
    db: DB,
    item: dict,
    raw_dir: str,
    ua: str,
    verify_ssl: bool = True,
    timeout: int = 30,
) -> dict:
    """
    Universal fetch:
    - Adapter (registry) chuyển meta → List[Location] (đa nguồn)
    - Ưu tiên PDF (HEAD + %PDF), fallback HTML
    - Giữ retry/certifi; cập nhật DB nếu có file
    """
    try:
        meta = json.loads(item.get("meta_json") or "{}")
    except Exception:
        meta = {}

    locs = locations_from_meta(meta)  # <— adapter theo nguồn
    safe_id = _safe_name(item["id"])
    base_path = os.path.join(raw_dir, safe_id)
    updated = dict(item)
    sess = _make_session(ua)

    got_pdf = False
    got_html = False

    # vòng 1: thử PDF theo thứ tự ưu tiên
    for loc in locs:
        pdf_path = _try_pdf(
            sess, loc, base_path, timeout=timeout, verify_ssl=verify_ssl
        )
        if pdf_path:
            updated["pdf_path"] = pdf_path
            got_pdf = True
            break

    # vòng 2: nếu chưa có PDF, thử HTML (lấy lần đầu tiên thành công)
    if not got_pdf:
        for loc in locs:
            html_path = _try_html(
                sess, loc, base_path, timeout=timeout, verify_ssl=verify_ssl
            )
            if html_path:
                updated["html_path"] = html_path
                got_html = True
                break

    if got_pdf or got_html:
        db.upsert_item(updated)
    return updated
