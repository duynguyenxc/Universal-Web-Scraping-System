from __future__ import annotations
import json
import math
import re
from typing import Iterable, Dict, Any, List

from ..logger import get_logger
from .storage import DB


_WORD_SPLIT = re.compile(r"[^\w]+", re.UNICODE)


def _normalize_kw(keywords: Iterable[str]) -> List[str]:
    out = []
    for k in keywords or []:
        k = (k or "").strip().lower()
        if not k:
            continue
        out.append(k)
    # bỏ trùng, giữ thứ tự
    seen = set()
    uniq = []
    for k in out:
        if k in seen:
            continue
        seen.add(k)
        uniq.append(k)
    return uniq


def _count_phrase(text: str, phrase: str) -> int:
    """Đếm số lần xuất hiện của phrase (không phân biệt hoa/thường), match rời rạc theo word-boundary khi khả thi."""
    if not text or not phrase:
        return 0
    text_l = text.lower()
    ph_l = phrase.lower()
    # nếu phrase là một từ đơn giản, dùng word boundary; còn lại dùng tìm substring thường
    if re.match(r"^[\w\-]+$", ph_l):
        pat = re.compile(rf"\b{re.escape(ph_l)}\b", flags=re.IGNORECASE)
        return len(pat.findall(text_l))
    return text_l.count(ph_l)


def _reconstruct_openalex_abstract(inv_index: Dict[str, List[int]] | None) -> str:
    """OpenAlex: abstract_inverted_index -> text. An toàn nếu không có."""
    if not inv_index:
        return ""
    # lấy độ dài tối đa
    max_pos = 0
    for positions in inv_index.values():
        if positions:
            max_pos = max(max_pos, max(positions))
    words = [""] * (max_pos + 1)
    for word, positions in inv_index.items():
        for p in positions or []:
            if 0 <= p < len(words):
                words[p] = word
    return " ".join(w for w in words if w)


def _extract_abstract_from_meta(meta_json: str) -> str:
    try:
        meta = json.loads(meta_json or "{}")
    except Exception:
        return ""
    # OpenAlex có abstract_inverted_index
    if "abstract_inverted_index" in meta and isinstance(
        meta["abstract_inverted_index"], dict
    ):
        return _reconstruct_openalex_abstract(meta["abstract_inverted_index"])
    # fallback một số nguồn khác
    return (meta.get("abstract") or "").strip()


def _read_text_file(path: str, max_chars: int = 200_000) -> str:
    if not path:
        return ""
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            s = f.read(max_chars + 1)
            if len(s) > max_chars:
                s = s[:max_chars]
            return s
    except Exception:
        return ""


def compute_score_for_row(
    row: Dict[str, Any], keywords: List[str], cfg: Dict[str, Any]
) -> float:
    """Tính điểm 0..1 dựa trên (title, abstract, text)."""
    title = (row.get("title") or "").strip()
    abstract = _extract_abstract_from_meta(row.get("meta_json") or "")
    text = _read_text_file(
        row.get("text_path") or "",
        max_chars=int(cfg.get("scoring", {}).get("max_text_chars", 200_000)),
    )

    w_title = float(cfg.get("scoring", {}).get("w_title", 3.0))
    w_abs = float(cfg.get("scoring", {}).get("w_abstract", 2.0))
    w_txt = float(cfg.get("scoring", {}).get("w_text", 1.0))

    hits_title = sum(_count_phrase(title, k) for k in keywords)
    hits_abs = sum(_count_phrase(abstract, k) for k in keywords)
    hits_txt = sum(_count_phrase(text, k) for k in keywords)

    raw = w_title * hits_title + w_abs * hits_abs + w_txt * hits_txt

    # chất lượng tối thiểu theo độ dài text
    min_chars = int(cfg.get("scoring", {}).get("min_chars", 800))
    bonus_cap = float(cfg.get("scoring", {}).get("quality_bonus_cap", 0.25))
    qbonus = 0.0
    if text:
        qbonus = min(
            bonus_cap, max(0.0, (len(text) - min_chars) / (5 * min_chars))
        )  # tăng dần tới trần

    # ánh xạ về [0,1]: 1 - exp(-raw/alpha)
    alpha = float(cfg.get("scoring", {}).get("alpha", 6.0))
    base = 1.0 - math.exp(-raw / max(alpha, 1e-6))
    score = max(0.0, min(1.0, base + qbonus))
    return score


def score_db(db: DB, cfg: Dict[str, Any], log_level: str = "INFO") -> Dict[str, int]:
    log = get_logger("uwss.score", log_level)
    keywords = _normalize_kw(cfg.get("domain", {}).get("keywords", []))
    if not keywords:
        log.warning("No keywords in config.domain.keywords; scoring will be 0 for all.")
    threshold = float(cfg.get("scoring", {}).get("keep_threshold", 0.5))

    n_total = n_updated = n_kept = 0
    for row in db.iter_items():
        n_total += 1
        score = compute_score_for_row(row, keywords, cfg)
        kept = 1 if score >= threshold else 0
        if abs(score - float(row.get("score") or 0.0)) > 1e-6 or kept != int(
            row.get("kept") or 0
        ):
            row["score"] = float(score)
            row["kept"] = kept
            db.upsert_item(row)
            n_updated += 1
            if kept:
                n_kept += 1

    log.info(
        "scoring finished: total=%d updated=%d kept>=%s = %d",
        n_total,
        n_updated,
        threshold,
        n_kept,
    )
    return {"total": n_total, "updated": n_updated, "kept": n_kept}
