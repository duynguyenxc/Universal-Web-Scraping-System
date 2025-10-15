import os
import json
import sqlite3
from typing import Dict, Any, Iterable, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
  id TEXT PRIMARY KEY,
  title TEXT,
  year INTEGER,
  venue TEXT,
  doi TEXT,
  source_url TEXT,
  pdf_path TEXT,
  html_path TEXT,
  text_path TEXT,
  score REAL,
  kept INTEGER DEFAULT 0,
  meta_json TEXT
);
"""


class DB:
    """SQLite wrapper cực gọn cho local. DSN dạng sqlite:///data/uwss.db"""

    def __init__(self, dsn: str):
        assert dsn.startswith("sqlite:///")
        path = dsn.replace("sqlite:///", "", 1)
        # bảo đảm folder tồn tại
        dir_ = os.path.dirname(path) or "."
        os.makedirs(dir_, exist_ok=True)
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.execute(SCHEMA)
        self.conn.commit()

    def upsert_item(self, row: Dict[str, Any]) -> None:
        cols = list(row.keys())
        placeholders = ":" + ",:".join(cols)
        sql = f"INSERT OR REPLACE INTO items ({','.join(cols)}) VALUES ({placeholders})"
        self.conn.execute(sql, row)
        self.conn.commit()

    def get_item(self, id_: str) -> Optional[Dict[str, Any]]:
        cur = self.conn.execute("SELECT * FROM items WHERE id=?", (id_,))
        one = cur.fetchone()
        if not one:
            return None
        cols = [c[0] for c in cur.description]
        return dict(zip(cols, one))

    def iter_items(self) -> Iterable[Dict[str, Any]]:
        cur = self.conn.execute("SELECT * FROM items ORDER BY rowid DESC")
        cols = [c[0] for c in cur.description]
        for r in cur.fetchall():
            yield dict(zip(cols, r))


def export_jsonl(rows: Iterable[Dict[str, Any]], out_path: str) -> str:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return out_path
