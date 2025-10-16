# uwss/core/discovery.py
from __future__ import annotations
import time
from typing import Iterable, List, Optional
import requests
import certifi

BASE = "https://api.openalex.org/works"


def _build_search_query(keywords: List[str]) -> Optional[str]:
    """
    Tạo truy vấn search theo dạng: ("kw1") OR ("kw2").
    Nếu không có từ khoá thì trả None để không gửi param 'search'.
    """
    kws = [k.strip() for k in (keywords or []) if k and k.strip()]
    if not kws:
        return None
    terms = [f'("{k}")' for k in kws]
    return " OR ".join(terms)


def _build_filter_clause(filter_oa: bool, min_year: Optional[int]) -> Optional[str]:
    """
    Ghép filter theo OpenAlex, dùng dấu phẩy để nối nhiều điều kiện.
    Ví dụ: open_access.is_oa:true,from_publication_date:2000-01-01
    """
    parts: List[str] = []
    if filter_oa:
        parts.append("open_access.is_oa:true")
    if min_year:
        parts.append(f"from_publication_date:{int(min_year)}-01-01")
    if not parts:
        return None
    return ",".join(parts)


def discover_openalex(
    keywords: List[str],
    max_results: int = 50,
    per_page: int = 25,
    timeout: int = 30,
    filter_oa: bool = False,
    min_year: Optional[int] = None,
    mailto: Optional[str] = None,
    request_delay_s: float = 0.25,
) -> Iterable[dict]:
    """
    Trả về iterator các bản ghi OpenAlex (dict) theo từ khoá + filter.
    - Cursor-based pagination (ổn định)
    - per_page: 1..200
    - filter_oa: nếu True → chỉ bài OA
    - min_year: năm tối thiểu
    - mailto: email lịch sự theo khuyến nghị OpenAlex
    """
    session = requests.Session()
    session.headers.update(
        {"Accept": "application/json", "User-Agent": "UWSS/1.0 (+research)"}
    )

    params = {}
    search = _build_search_query(keywords)
    if search:
        params["search"] = search

    filt = _build_filter_clause(filter_oa=filter_oa, min_year=min_year)
    if filt:
        params["filter"] = filt

    params["per-page"] = max(1, min(int(per_page), 200))
    params["cursor"] = "*"
    if mailto:
        params["mailto"] = mailto

    fetched = 0
    verify_param = certifi.where()

    while fetched < max_results and params.get("cursor"):
        try:
            resp = session.get(
                BASE, params=params, timeout=timeout, verify=verify_param
            )
            if resp.status_code != 200:
                time.sleep(request_delay_s)
                break
            data = resp.json()
        except requests.RequestException:
            time.sleep(request_delay_s)
            break
        except ValueError:
            time.sleep(request_delay_s)
            break

        results = data.get("results") or []
        if not results:
            break

        for w in results:
            yield w
            fetched += 1
            if fetched >= max_results:
                break

        params["cursor"] = data.get("meta", {}).get("next_cursor")
        time.sleep(request_delay_s)
