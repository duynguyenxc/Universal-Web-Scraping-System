# uwss/plugins/unpaywall/mapper.py
from __future__ import annotations
from typing import List, Optional, Dict, Any, Tuple
import requests
import certifi
from uwss.schemas.location import Location, normalize_locations

PLUGIN_NAME = "unpaywall"
API = "https://api.unpaywall.org/v2/"


def _pick(loc: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    pdf = loc.get("url_for_pdf")
    html = loc.get("url")
    lic = loc.get("license")
    return pdf, html, lic


def map_unpaywall_by_doi(
    doi: str,
    email: str,
    timeout: int = 20,
    prefer_best: bool = True,
) -> List[Location]:
    """
    Gọi Unpaywall bằng DOI, trả về danh sách Location (PDF/HTML) theo thứ tự ưu tiên.
    prefer_best: nếu True, best_oa_location có priority thấp nhất (ưu tiên cao nhất).
    """
    if not doi or not email:
        return []

    doi_norm = doi.strip().lower()
    if doi_norm.startswith("https://doi.org/"):
        doi_norm = doi_norm.replace("https://doi.org/", "")

    url = f"{API}{doi_norm}"
    params = {"email": email}
    try:
        r = requests.get(url, params=params, timeout=timeout, verify=certifi.where())
        if r.status_code != 200:
            return []
        data = r.json()
    except Exception:
        return []

    out: List[Location] = []
    pr_best = 0 if prefer_best else 5

    best = data.get("best_oa_location") or {}
    if best:
        pdf, html, lic = _pick(best)
        out.append(
            Location(
                pdf_url=pdf,
                html_url=html,
                priority=pr_best,
                source=PLUGIN_NAME,
                license=lic,
            )
        )

    for i, loc in enumerate(data.get("oa_locations") or []):
        if not isinstance(loc, dict):
            continue
        pdf, html, lic = _pick(loc)
        # tránh trùng với best
        out.append(
            Location(
                pdf_url=pdf,
                html_url=html,
                priority=(10 + i),
                source=PLUGIN_NAME,
                license=lic,
            )
        )

    return normalize_locations(out)
