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


# ---------------------------
# Helpers
# ---------------------------


def _mk_id(title: str, year: int) -> str:
    base = f"{(title or '').strip()}_{year or 0}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _get_openalex_opts(cfg: dict) -> dict:
    """
    Đọc cấu hình OpenAlex cho cả 2 kiểu:
    - cũ (dict): cfg["sources"]["openalex"]["..."]
    - mới (list plugin): cfg["sources"] = [{name/plugin: "openalex", options: {...}}]
    """
    srcs = cfg.get("sources", {})

    def _mk(d: dict) -> dict:
        return {
            "max_results": int(d.get("max_results", 50)),
            "per_page": int(d.get("per_page", 25)),
            "timeout": int(d.get("timeout", 30)),
            "filter_oa": bool(d.get("filter_oa", False)),
            "min_year": d.get("min_year", None),
            "mailto": d.get("mailto", None),
        }

    # legacy dict style
    if isinstance(srcs, dict):
        oa = srcs.get("openalex", {}) or {}
        return _mk(oa)

    # plugin list style
    if isinstance(srcs, list):
        for s in srcs:
            if s.get("plugin") == "openalex" or s.get("name") == "openalex":
                opts = s.get("options", {}) or {}
                return _mk(opts)

    # defaults
    return {
        "max_results": 50,
        "per_page": 25,
        "timeout": 30,
        "filter_oa": False,
        "min_year": None,
        "mailto": None,
    }


def _get_unpaywall_opts(cfg: dict) -> dict:
    """
    Đọc cấu hình Unpaywall từ schema plugin-list:
    sources:
      - name: unpaywall
        plugin: unpaywall
        options:
          email: "you@domain"
          timeout: 20
          prefer_best: true
    """
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


# ---------------------------
# Commands
# ---------------------------


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

    oa = _get_openalex_opts(cfg)
    maxn = int(oa.get("max_results", 50))
    per_page = int(oa.get("per_page", 25))
    timeout = int(oa.get("timeout", 30))
    filter_oa = bool(oa.get("filter_oa", False))
    min_year = oa.get("min_year")
    mailto = oa.get("mailto")

    log.info(
        "discovering from OpenAlex: keywords=%s max=%s per_page=%s timeout=%s filter_oa=%s min_year=%s",
        kw,
        maxn,
        per_page,
        timeout,
        filter_oa,
        min_year,
    )

    db = DB(cfg["storage"]["database"])
    count = 0

    for w in discover_openalex(
        kw,
        max_results=maxn,
        per_page=per_page,
        timeout=timeout,
        filter_oa=filter_oa,
        min_year=min_year,
        mailto=mailto,
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
            # lưu raw metadata để dùng sau (fetch/adapters/sequence)
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
        if row.get("pdf_path") or row.get("html_path"):
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
            if new_row.get("pdf_path")
            else ("html" if new_row.get("html_path") else "none")
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
        "fetch finished: %d attempted | pdf=%d html=%d none=%d", done, pdfs, htmls, none
    )


# ---------------------------
# Entrypoint
# ---------------------------


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
        help="rows to peek (with 'db-peek') or items to fetch (with 'fetch')",
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


if __name__ == "__main__":
    main()
