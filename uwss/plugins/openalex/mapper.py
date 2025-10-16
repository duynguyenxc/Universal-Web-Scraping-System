# uwss/plugins/openalex/mapper.py
from __future__ import annotations
from typing import List, Dict, Optional, Tuple, Any
from uwss.schemas.location import Location, normalize_locations

PLUGIN_NAME = "openalex"


def _pick(
    loc: Dict[str, Any]
) -> Tuple[Optional[str], Optional[str], Optional[bool], Optional[str]]:
    # OpenAlex có thể dùng các key khác nhau cho PDF/landing
    pdf = loc.get("url_for_pdf") or loc.get("pdf_url")
    html = loc.get("url") or loc.get("landing_page_url")
    is_oa = loc.get("is_oa")
    lic = loc.get("license")
    return pdf, html, is_oa, lic


def map_openalex_locations(meta: Dict[str, Any]) -> List[Location]:
    """
    Chuyển metadata OpenAlex → danh sách Location theo thứ tự ưu tiên:
    1) best_oa_location (priority 0)
    2) primary_location (priority 5)
    3) mọi entry trong locations[] (priority 10 + index)
    """
    out: List[Location] = []

    best = meta.get("best_oa_location") or {}
    if best:
        pdf, html, is_oa, lic = _pick(best)
        out.append(
            Location(
                pdf_url=pdf,
                html_url=html,
                priority=0,
                source=PLUGIN_NAME,
                is_oa=is_oa,
                license=lic,
            )
        )

    primary = meta.get("primary_location") or {}
    if primary:
        pdf, html, is_oa, lic = _pick(primary)
        out.append(
            Location(
                pdf_url=pdf,
                html_url=html,
                priority=5,
                source=PLUGIN_NAME,
                is_oa=is_oa,
                license=lic,
            )
        )

    for i, loc in enumerate(meta.get("locations") or []):
        if not isinstance(loc, dict):
            continue
        pdf, html, is_oa, lic = _pick(loc)
        out.append(
            Location(
                pdf_url=pdf,
                html_url=html,
                priority=10 + i,
                source=PLUGIN_NAME,
                is_oa=is_oa,
                license=lic,
            )
        )

    return normalize_locations(out)
