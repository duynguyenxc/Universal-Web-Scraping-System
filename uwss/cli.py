from __future__ import annotations

import argparse
import hashlib
import json
import time

from .config_loader import load_config
from .logger import get_logger
from .core.discovery import discover_openalex
from .core.storage import DB
from .core.fetching import fetch_one
from .core.scoring import score_db
from .core.exporter import export_rows

# ===== Helpers =====


def _mk_id(title: str, year: int) -> str:
    base = f"{(title or '').strip()}_{year or 0}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _get_openalex_opts(cfg: dict) -> dict:
    """
    Lấy options cho nguồn openalex từ config.sources (kiểu plugin list).
    Hỗ trợ cả dạng legacy nếu bạn vẫn còn.
    """
    out = {
        "max_results": 50,
        "per_page": 25,
        "timeout": 30,
        "filter_oa": True,
        "min_year": None,
        "mailto": None,
    }
    srcs = cfg.get("sources", [])
    if isinstance(srcs, list):
        for s in srcs:
            if s.get("plugin") == "openalex" or s.get("name") == "openalex":
                opts = s.get("options", {}) or {}
                if "max_results" in opts:
                    out["max_results"] = int(
                        opts.get("max_results", out["max_results"])
                    )
                if "per_page" in opts:
                    out["per_page"] = int(opts.get("per_page", out["per_page"]))
                if "timeout" in opts:
                    out["timeout"] = int(opts.get("timeout", out["timeout"]))
                if "filter_oa" in opts:
                    out["filter_oa"] = bool(opts.get("filter_oa", out["filter_oa"]))
                if "min_year" in opts:
                    try:
                        out["min_year"] = int(opts.get("min_year"))
                    except Exception:
                        out["min_year"] = None
                if "mailto" in opts:
                    out["mailto"] = opts.get("mailto")
                break
    return out


def _get_unpaywall_opts(cfg: dict) -> dict:
    srcs = cfg.get("sources", [])
    if isinstance(srcs, list):
        for s in srcs:
            if s.get("plugin") == "unpaywall" or s.get("name") == "unpaywall":
                opts = s.get("options", {}) or {}
                return {
                    "email": opts.get("email"),
                    "timeout": int(opts.get("timeout", 20)),
                    "prefer_best": bool(opts.get("prefer_best", True)),
                }
    return {"email": None, "timeout": 20, "prefer_best": True}


# ===== Commands =====


def cmd_doctor(cfg: dict):
    log = get_logger("uwss.doctor", cfg["runtime"]["log_level"])
    log.info("UWSS is ready.")
    log.info(
        "Storage raw_dir=%s text_dir=%s out_dir=%s",
        cfg["storage"]["raw_dir"],
        cfg["storage"]["text_dir"],
        cfg["storage"]["out_dir"],
    )


def cmd_config(cfg: dict, show: bool):
    if show:
        print(json.dumps(cfg, ensure_ascii=False, indent=2))


def cmd_db_init(cfg: dict):
    db = DB(cfg["storage"]["database"])
    # chèn 1 record mẫu để xác nhận schema/ghi chép hoạt động
    sample = {
        "id": f"sample-{int(time.time())}",
        "title": "Sample placeholder",
        "year": 0,
        "venue": "",
        "doi": "",
        "source_url": "",
        "pdf_path": "",
        "html_path": "",
        "text_path": "",
        "score": 0.0,
        "kept": 0,
        "meta_json": json.dumps({"note": "init row"}, ensure_ascii=False),
    }
    db.upsert_item(sample)
    print("DB initialized and sample row inserted.")


def cmd_db_peek(cfg: dict, limit: int = 3):
    db = DB(cfg["storage"]["database"])
    for i, row in enumerate(db.iter_items()):
        if i >= limit:
            break
        print(
            json.dumps(
                {k: row[k] for k in ("id", "title", "year", "doi", "score", "kept")},
                ensure_ascii=False,
            )
        )


def cmd_discover(cfg: dict):
    log = get_logger("uwss.discover", cfg["runtime"]["log_level"])
    kw = cfg["domain"]["keywords"]
    oa_opts = _get_openalex_opts(cfg)

    log.info(
        "discovering from OpenAlex: keywords=%s max=%s per_page=%s timeout=%s filter_oa=%s min_year=%s",
        kw,
        oa_opts["max_results"],
        oa_opts["per_page"],
        oa_opts["timeout"],
        oa_opts["filter_oa"],
        oa_opts["min_year"],
    )

    db = DB(cfg["storage"]["database"])
    count = 0

    for w in discover_openalex(
        kw,
        max_results=oa_opts["max_results"],
        per_page=oa_opts["per_page"],
        timeout=oa_opts["timeout"],
        filter_oa=oa_opts["filter_oa"],
        min_year=oa_opts["min_year"],
        mailto=oa_opts["mailto"],
    ):
        host = w.get("host_venue") or {}
        primary = w.get("primary_location") or {}
        row = {
            "id": w.get("id")
            or _mk_id(w.get("title") or "", w.get("publication_year") or 0),
            "title": (w.get("title") or "").strip(),
            "year": w.get("publication_year") or 0,
            "venue": host.get("display_name") or "",
            "doi": w.get("doi") or "",
            "source_url": primary.get("landing_page_url") or "",
            "pdf_path": "",
            "html_path": "",
            "text_path": "",
            "score": 0.0,
            "kept": 0,
            # lưu raw metadata để dùng sau (fetch/unpaywall/sequence)
            "meta_json": json.dumps(w, ensure_ascii=False),
        }
        db.upsert_item(row)
        count += 1

    log.info("discovered %d records into DB %s", count, cfg["storage"]["database"])


def cmd_fetch(cfg: dict, limit: int = 20):
    log = get_logger("uwss.fetch", cfg["runtime"]["log_level"])
    db = DB(cfg["storage"]["database"])
    ua = cfg["runtime"]["user_agent"]
    raw_dir = cfg["storage"]["raw_dir"]
    verify_ssl = cfg.get("runtime", {}).get("ssl_verify", True)

    upw = _get_unpaywall_opts(cfg)

    done = 0
    pdfs = htmls = none = 0

    for row in db.iter_items():
        if done >= limit:
            break
        # chỉ fetch những bản chưa có file
        if (row.get("pdf_path") or "").strip() or (row.get("html_path") or "").strip():
            continue
        new_row = fetch_one(
            db,
            row,
            raw_dir=raw_dir,
            ua=ua,
            verify_ssl=verify_ssl,
            unpaywall_email=upw.get("email"),
            unpaywall_timeout=upw.get("timeout", 20),
            unpaywall_prefer_best=upw.get("prefer_best", True),
        )
        got = (
            "pdf"
            if (new_row.get("pdf_path") or "").strip()
            else ("html" if (new_row.get("html_path") or "").strip() else "none")
        )
        if got == "pdf":
            pdfs += 1
        elif got == "html":
            htmls += 1
        else:
            none += 1
        log.info("fetched %s → %s", row["id"], got)
        done += 1

    log.info(
        "fetch finished: %d attempted | pdf=%d html=%d none=%d",
        done,
        pdfs,
        htmls,
        none,
    )


def cmd_extract(cfg: dict, limit: int = 50):
    """
    Gọi các hàm extract trong uwss.core.parsing một cách linh hoạt để không phá code hiện tại của bạn.
    Ưu tiên các batch API nếu có; nếu không, fallback sang vòng lặp từng row.
    """
    log = get_logger("uwss.extract", cfg["runtime"]["log_level"])
    db = DB(cfg["storage"]["database"])
    text_dir = cfg["storage"]["text_dir"]
    attempted = ok = 0

    # import linh hoạt:
    extract_fn = None
    single_fn = None
    try:
        from .core.parsing import extract_all as extract_fn  # type: ignore
    except Exception:
        try:
            from .core.parsing import extract_batch as extract_fn  # type: ignore
        except Exception:
            try:
                from .core.parsing import extract_texts as extract_fn  # type: ignore
            except Exception:
                # tìm single row API
                try:
                    from .core.parsing import extract_one as single_fn  # type: ignore
                except Exception:
                    single_fn = None

    # Nếu có batch function, cứ gọi thẳng với các tham số phổ biến:
    if extract_fn is not None:
        try:
            # thử các chữ ký phổ biến
            res = None
            try:
                # (db, out_dir, limit)
                res = extract_fn(db, text_dir, limit)  # type: ignore
            except TypeError:
                try:
                    # (db, cfg, limit)
                    res = extract_fn(db, cfg, limit)  # type: ignore
                except TypeError:
                    # (db,) tối giản
                    res = extract_fn(db)  # type: ignore
            # nếu result có thống kê, log gọn:
            if isinstance(res, dict) and "attempted" in res and "ok" in res:
                attempted = int(res.get("attempted") or 0)
                ok = int(res.get("ok") or 0)
            log.info(
                "extract finished: attempted=%s ok=%s",
                attempted or "?",
                ok or "?",
            )
            return
        except Exception as e:
            log.warning("batch extract failed, fallback to single: %s", e)

    # Fallback: xử lý từng row nếu có single_fn
    if single_fn is not None:
        for row in db.iter_items():
            if limit and attempted >= limit:
                break
            # chỉ extract nếu có file gốc nhưng chưa có text
            has_src = (row.get("pdf_path") or row.get("html_path") or "").strip()
            has_txt = (row.get("text_path") or "").strip()
            if not has_src or has_txt:
                continue
            attempted += 1
            try:
                new_row = single_fn(db, row, out_dir=text_dir)  # type: ignore
                if (new_row.get("text_path") or "").strip():
                    ok += 1
            except TypeError:
                # chữ ký khác: thử ít tham số hơn
                try:
                    new_row = single_fn(db, row)  # type: ignore
                    if (new_row.get("text_path") or "").strip():
                        ok += 1
                except Exception:
                    pass
            except Exception:
                pass
        log.info("extract finished: attempted=%d ok=%d", attempted, ok)
        return

    # Nếu không tìm thấy API nào:
    log.error(
        "No compatible extract function found in uwss.core.parsing. "
        "Please ensure one of: extract_all, extract_batch, extract_texts, extract_one exists."
    )


# ==== New: scoring / export / stats ====


def cmd_score(cfg: dict):
    db = DB(cfg["storage"]["database"])
    log_level = cfg["runtime"]["log_level"]
    stats = score_db(db, cfg, log_level=log_level)
    print(
        f"Scoring done: total={stats['total']} updated={stats['updated']} kept={stats['kept']}"
    )


def cmd_export(cfg: dict, fmt: str = "csv", only_kept: bool = False):
    db = DB(cfg["storage"]["database"])
    out_dir = cfg["storage"]["out_dir"]
    log_level = cfg["runtime"]["log_level"]
    path = export_rows(db, out_dir, fmt=fmt, only_kept=only_kept, log_level=log_level)
    print(f"Exported -> {path}")


def cmd_db_stats(cfg: dict):
    db = DB(cfg["storage"]["database"])
    total = pdf = html = text = kept = 0
    for row in db.iter_items():
        total += 1
        if (row.get("pdf_path") or "").strip():
            pdf += 1
        if (row.get("html_path") or "").strip():
            html += 1
        if (row.get("text_path") or "").strip():
            text += 1
        if int(row.get("kept") or 0) == 1:
            kept += 1
    print(
        json.dumps(
            {
                "total": total,
                "with_pdf": pdf,
                "with_html": html,
                "with_text": text,
                "kept": kept,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


# ===== main =====


def main():
    ap = argparse.ArgumentParser(prog="uwss", description="UWSS CLI")
    ap.add_argument(
        "cmd",
        choices=[
            "doctor",
            "config",
            "db-init",
            "db-peek",
            "discover",
            "fetch",
            "extract",
            "score",
            "export",
            "db-stats",
        ],
    )
    ap.add_argument("--config", default="config/default.yaml")
    ap.add_argument("--show", action="store_true", help="print config (with 'config')")
    ap.add_argument("--limit", type=int, default=50, help="item limit for some cmds")
    ap.add_argument(
        "--fmt",
        choices=["csv", "jsonl"],
        default="csv",
        help="output format for 'export'",
    )
    ap.add_argument(
        "--only-kept",
        action="store_true",
        help="export only kept=1 rows (for 'export')",
    )

    args = ap.parse_args()
    cfg = load_config(args.config)

    if args.cmd == "doctor":
        cmd_doctor(cfg)
    elif args.cmd == "config":
        cmd_config(cfg, args.show)
    elif args.cmd == "db-init":
        cmd_db_init(cfg)
    elif args.cmd == "db-peek":
        cmd_db_peek(cfg, args.limit)
    elif args.cmd == "discover":
        cmd_discover(cfg)
    elif args.cmd == "fetch":
        cmd_fetch(cfg, args.limit)
    elif args.cmd == "extract":
        cmd_extract(cfg, args.limit)
    elif args.cmd == "score":
        cmd_score(cfg)
    elif args.cmd == "export":
        cmd_export(cfg, fmt=args.fmt, only_kept=args.only_kept)
    elif args.cmd == "db-stats":
        cmd_db_stats(cfg)


if __name__ == "__main__":
    main()
