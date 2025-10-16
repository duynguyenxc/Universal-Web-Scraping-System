# uwss/schemas/location.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Iterable, List, Dict, Any


@dataclass
class Location:
    pdf_url: Optional[str] = None  # URL tải PDF trực tiếp (nếu có)
    html_url: Optional[str] = None  # URL landing/HTML (fallback)
    priority: int = 100  # số càng nhỏ càng ưu tiên
    source: str = ""  # tên plugin/nguồn (vd: "openalex")
    is_oa: Optional[bool] = None  # open-access?
    license: Optional[str] = None  # thông tin license nếu có


def normalize_locations(locs: Iterable[Dict[str, Any] | Location]) -> List[Location]:
    """
    Chuyển mọi input (dict/Location) thành list[Location],
    lọc bỏ entry rỗng và sắp xếp theo priority tăng dần.
    """
    out: List[Location] = []
    for x in locs:
        if isinstance(x, Location):
            loc = x
        else:
            d = dict(x or {})
            loc = Location(
                pdf_url=d.get("pdf_url"),
                html_url=d.get("html_url") or d.get("url"),
                priority=int(d.get("priority", 100)),
                source=str(d.get("source", "")),
                is_oa=d.get("is_oa"),
                license=d.get("license"),
            )
        # bỏ entry trống cả pdf/html
        if not (loc.pdf_url or loc.html_url):
            continue
        out.append(loc)
    # sort theo priority
    out.sort(key=lambda z: z.priority)
    return out
