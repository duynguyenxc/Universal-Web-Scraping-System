# uwss/registry.py
from __future__ import annotations
from typing import Dict, Any, List

from uwss.schemas.location import Location, normalize_locations
from uwss.plugins.openalex.mapper import map_openalex_locations, PLUGIN_NAME as OA

# NEW:
from uwss.plugins.unpaywall.mapper import map_unpaywall_by_doi


def detect_source_from_meta(meta: Dict[str, Any]) -> str:
    mid = str(meta.get("id", "")).lower()
    if "openalex.org/" in mid or ("authorships" in meta and "primary_location" in meta):
        return OA
    return ""  # unknown


def locations_from_meta(meta: Dict[str, Any]) -> List[Location]:
    src = detect_source_from_meta(meta)
    if src == OA:
        return map_openalex_locations(meta)
    return normalize_locations([])


# NEW: hàm bổ sung location từ các “enricher” ngoài meta nguồn (ví dụ Unpaywall)
def enrich_locations_with_unpaywall(
    locs: List[Location],
    meta: Dict[str, Any],
    email: str,
    timeout: int = 20,
    prefer_best: bool = True,
) -> List[Location]:
    doi = meta.get("doi") or ""
    if not doi or not email:
        return locs

    # lấy thêm location từ Unpaywall
    upw_locs = map_unpaywall_by_doi(
        doi, email=email, timeout=timeout, prefer_best=prefer_best
    )

    # ghép & sắp xếp lại theo priority
    all_locs = (locs or []) + (upw_locs or [])
    return normalize_locations(all_locs)
