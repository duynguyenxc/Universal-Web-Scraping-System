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


def _mk_id(title: str, year: int) -> str:
    base = f"{(title or '').strip()}_{year or 0}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _get_openalex_opts(cfg: dict) -> dict:
    """
    Đọc cấu hình OpenAlex cho cả 2 kiểu:
    - cũ: cfg["sources"]["openalex"]["max_results"]
    - mới (plugin list): cfg["sources"] = [{name/plugin: "openalex", options: {...}}]
    """
    srcs = cfg.get("sources", {})
    # kiểu cũ: dict
    if isinstance(srcs, dict):
        oa = srcs.get("openalex", {}) or {}
        return {
            "max_results": int(oa.get("max_results", 50)),
            "per_page": int(oa.get("per_page", 25)),
            "timeout": int(oa.get("timeout", 30)),
        }
    # kiểu mới: list plugin
    if isinstance(srcs, list):
        for s in srcs:
            if s.get("plugin") == "openalex" or s.get("name") == "openalex":
                opts = s.get("options", {}) or {}
                return {
                    "max_results": int(opts.get("max_results", 50)),
                    "per_page": int(opts.get("per_page", 25)),
                    "timeout": int(opts.get("timeout", 30)),
                }
    # mặc định an toàn
    return {"max_results": 50, "per_page": 25, "timeout": 30}


def cmd_discover(cfg: dict):
    log = get_logger("uwss.discover", cfg["runtime"]["log_level"])
    kw = cfg.get("domain", {}).get("keywords", []) or []
    oa = _get_openalex_opts(cfg)
    maxn, per_page, timeout = oa["max_results"], oa["per_page"], oa["timeout"]
    log.info(
        "discovering from OpenAlex: keywords=%s max=%s per_page=%s timeout=%s",
        kw,
        maxn,
        per_page,
        timeout,
    )

    db = DB(cfg["storage"]["database"])
    count = 0

    for w in discover_openalex(
        kw, max_results=maxn, per_page=per_page, timeout=timeout
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
    # NEW: đọc cờ ssl_verify (mặc định True nếu không có)
    verify_ssl = cfg.get("runtime", {}).get("ssl_verify", True)

    done = 0
    for row in db.iter_items():
        if done >= limit:
            break
        if row.get("pdf_path") or row.get("html_path"):
            continue
        new_row = fetch_one(
            db, row, raw_dir=raw_dir, ua=ua, verify_ssl=verify_ssl
        )  # <-- truyền flag
        got = (
            "pdf"
            if new_row.get("pdf_path")
            else ("html" if new_row.get("html_path") else "none")
        )
        log.info("fetched %s → %s", row["id"], got)
        done += 1

    log.info("fetch finished: %d items attempted", done)


def main():
    ap = argparse.ArgumentParser(prog="uwss", description="UWSS minimal CLI")
    ap.add_argument(
        "cmd", choices=["doctor", "config", "db-init", "db-peek", "discover", "fetch"]
    )
    ap.add_argument("--config", default="config/default.yaml")
    ap.add_argument(
        "--show", action="store_true", help="print config (with 'config' cmd)"
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=3,
        help="rows to peek (with 'db-peek') and items to fetch (with 'fetch')",
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
        # dùng args.limit cho fetch, mặc định ở trên mình để 3; bạn có thể set --limit 10 khi chạy
        cmd_fetch(cfg, args.limit)


if __name__ == "__main__":
    main()
