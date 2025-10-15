from __future__ import annotations
import argparse
import json
import time
from .config_loader import load_config
from .logger import get_logger
from .core.storage import DB


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


def main():
    ap = argparse.ArgumentParser(prog="uwss", description="UWSS minimal CLI")
    ap.add_argument("cmd", choices=["doctor", "config", "db-init", "db-peek"])
    ap.add_argument("--config", default="config/default.yaml")
    ap.add_argument(
        "--show", action="store_true", help="print config (with 'config' cmd)"
    )
    ap.add_argument(
        "--limit", type=int, default=3, help="rows to peek (with 'db-peek')"
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


if __name__ == "__main__":
    main()
