from __future__ import annotations
import time
import urllib.parse
from typing import Dict, Iterable, List

import requests

OPENALEX = "https://api.openalex.org/works"


def _build_query(keywords: List[str]) -> str:
    """
    Ghép chuỗi search cho OpenAlex.
    - Cụm có khoảng trắng sẽ được đặt trong dấu ngoặc kép.
    - Nhiều từ/cụm nối bằng OR.
    Ví dụ: reinforced OR "chloride diffusion test"
    """
    if not keywords:
        return ""
    parts: List[str] = []
    for k in keywords:
        k = str(k).strip()
        if not k:
            continue
        if " " in k:
            parts.append(f'"{k}"')
        else:
            parts.append(k)
    return " OR ".join(parts)


def discover_openalex(
    keywords: List[str],
    max_results: int = 50,
    per_page: int = 25,
    timeout: int = 30,
) -> Iterable[Dict]:
    """
    Trả về iterator các bản ghi OpenAlex (dict).
    - Không ghi DB tại đây (để giữ pure logic).
    - Dùng search= chuỗi tự do, phân trang bằng cursor.
    """
    params = {"per-page": per_page}
    q = _build_query(keywords)
    if q:
        params["search"] = q

    cursor = "*"
    fetched = 0

    session = requests.Session()
    session.headers.update({"User-Agent": "UWSS/1.0 (OpenAlex discovery)"})

    while fetched < max_results:
        params["cursor"] = cursor
        url = OPENALEX + "?" + urllib.parse.urlencode(params)
        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", []) or []
        for item in results:
            yield item
            fetched += 1
            if fetched >= max_results:
                break

        # dừng nếu hết trang
        next_cursor = (data.get("meta") or {}).get("next_cursor")
        if not next_cursor or not results:
            break

        cursor = next_cursor
        time.sleep(0.3)  # tránh spam API
