from __future__ import annotations
import os
import yaml

_DEFAULTS = {
    "domain": {"keywords": [], "boost": []},
    "sources": {"openalex": {"enabled": True, "max_results": 10}},
    "storage": {
        "database": "sqlite:///data/uwss.db",
        "raw_dir": "data/raw",
        "text_dir": "data/text",
        "out_dir": "data/out",
    },
    "runtime": {
        "user_agent": "UWSS/1.0",
        "log_level": "INFO",
        "respect_robots": True,
        "retries": 2,
    },
}


def _deep_merge(a: dict, b: dict) -> dict:
    """merge b into a (shallow-safe for our small config)"""
    out = dict(a)
    for k, v in b.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        user_cfg = yaml.safe_load(f) or {}
    cfg = _deep_merge(_DEFAULTS, user_cfg)

    # đảm bảo các thư mục lưu trữ tồn tại
    for p in [
        cfg["storage"]["raw_dir"],
        cfg["storage"]["text_dir"],
        cfg["storage"]["out_dir"],
        "data/logs",
    ]:
        os.makedirs(p, exist_ok=True)
    return cfg
