from __future__ import annotations
import argparse
import json
from .config_loader import load_config
from .logger import get_logger


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


def main():
    ap = argparse.ArgumentParser(prog="uwss", description="UWSS minimal CLI")
    ap.add_argument("cmd", choices=["doctor", "config"])
    ap.add_argument("--config", default="config/default.yaml")
    ap.add_argument(
        "--show", action="store_true", help="print config (with 'config' cmd)"
    )
    args = ap.parse_args()

    cfg = load_config(args.config)

    if args.cmd == "doctor":
        cmd_doctor(cfg)
    elif args.cmd == "config":
        cmd_config(cfg, args.show)


if __name__ == "__main__":
    main()
