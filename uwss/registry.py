# uwss/registry.py
from __future__ import annotations
from typing import Dict, Any, List
from uwss.schemas.location import Location, normalize_locations
from uwss.plugins.openalex.mapper import map_openalex_locations, PLUGIN_NAME as OA


def detect_source_from_meta(meta: Dict[str, Any]) -> str:
    # phát hiện OpenAlex rất dễ: id chứa openalex.org hoặc có cấu trúc đặc trưng
    mid = str(meta.get("id", "")).lower()
    if "openalex.org/" in mid or ("authorships" in meta and "primary_location" in meta):
        return OA
    return ""  # unknown


def locations_from_meta(meta: Dict[str, Any]) -> List[Location]:
    src = detect_source_from_meta(meta)
    if src == OA:
        return map_openalex_locations(meta)
    # các nguồn khác sẽ bổ sung ở đây sau:
    # if src == "crossref": return map_crossref_locations(meta)
    # if src == "arxiv": return map_arxiv_locations(meta)
    return normalize_locations([])  # mặc định rỗng
