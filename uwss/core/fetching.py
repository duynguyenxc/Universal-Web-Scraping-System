# uwss/core/fetching.py
from __future__ import annotations
import os
import json
import time
import re
from typing import Optional, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import certifi

from .storage import DB
from uwss.schemas.location import Location
from uwss.registry import locations_from_meta, enrich_locations_with_unpaywall

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
    try:
        with open(path, "rb") as f:
            return f.read(4) == b"%PDF"
    except OSError:
        return False


def _head_content_type(
    sess: requests.Session, url: str, timeout: int, verify_ssl: bool
) -> str:
    try:
        r = sess.head(
            url,
            timeout=timeout,
            allow_redirects=True,
            verify=(certifi.where() if verify_ssl else False),
        )
        return (r.headers.get("Content-Type") or "").lower()
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
                for chunk in r.iter_content(8192):
                    if not chunk:
                        continue
                    f.write(chunk)
                    total += len(chunk)
            return total > 0
    except requests.RequestException:
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
    is_pdf_like = ("application/pdf" in ctype) if ctype else True
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
    unpaywall_email: Optional[str] = None,
    unpaywall_timeout: int = 20,
    unpaywall_prefer_best: bool = True,
) -> dict:
    """
    Universal fetch:
    - Map meta nguồn → List[Location] (OpenAlex, v.v.)
    - Enrich bằng Unpaywall nếu có DOI + email
    - Ưu tiên PDF, fallback HTML
    """
    try:
        meta = json.loads(item.get("meta_json") or "{}")
    except Exception:
        meta = {}

    locs: List[Location] = locations_from_meta(meta)

    # Enrich bằng Unpaywall (nếu được cấu hình)
    if unpaywall_email:
        locs = enrich_locations_with_unpaywall(
            locs,
            meta,
            email=unpaywall_email,
            timeout=unpaywall_timeout,
            prefer_best=unpaywall_prefer_best,
        )

    safe_id = _safe_name(item["id"])
    base_path = os.path.join(raw_dir, safe_id)
    updated = dict(item)
    sess = _make_session(ua)

    got_pdf = False
    got_html = False

    # Vòng 1: PDF
    for loc in locs:
        p = _try_pdf(sess, loc, base_path, timeout=timeout, verify_ssl=verify_ssl)
        if p:
            updated["pdf_path"] = p
            got_pdf = True
            break

    # Vòng 2: HTML
    if not got_pdf:
        for loc in locs:
            h = _try_html(sess, loc, base_path, timeout=timeout, verify_ssl=verify_ssl)
            if h:
                updated["html_path"] = h
                got_html = True
                break

    if got_pdf or got_html:
        db.upsert_item(updated)
    return updated
