from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Dict, Tuple

DEFAULT_DB = Path("ecommerce.db")
SUMMARY_TABLE = "orders_summary"

class DB:
    """Wrapper minimalista em torno de `sqlite3.Connection`.  Usa transações."""

    def __init__(self, db_path: str | Path):
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._lock = threading.Lock()
        self._create_tables()

    def _create_tables(self) -> None:
        cur = self.conn.cursor()
        for t in ("T1", "T2", "T3", "T4", "T5", "T6"):
            cur.execute(f"CREATE TABLE IF NOT EXISTS {t} (datetime TEXT PRIMARY KEY, count FLOAT);")
        self.conn.commit()

    def insert_data(self, table: str, datetime_str: str, count: float) -> None:
        with self._lock:
            cur = self.conn.cursor()
            cur.execute(
                f"INSERT OR REPLACE INTO {table} (datetime, count) VALUES (?, ?);",
                (datetime_str, count),
            )
            self.conn.commit()

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    # Para usar com `with DB(...) as db:` ---------------------------------
    def __enter__(self) -> "DB":
        return self

    def __exit__(self, exc_type, exc, tb):  # noqa: D401
        self.close()

def ensure_table(conn: sqlite3.Connection):
    conn.executescript(
        f"""
        CREATE TABLE IF NOT EXISTS {SUMMARY_TABLE} (
            produto TEXT,
            centro  TEXT,
            total_valor REAL,
            total_qtd  INTEGER,
            PRIMARY KEY (produto, centro)
        );
        CREATE TABLE IF NOT EXISTS canal_summary (
            canal TEXT PRIMARY KEY,
            total_valor REAL
        );
        CREATE TABLE IF NOT EXISTS estado_summary (
            estado TEXT PRIMARY KEY,
            total_qtd INTEGER
        );
        """
    )
    conn.commit()


def upsert(
    conn: sqlite3.Connection,
    pc: Dict[Tuple[str, str], Tuple[float, int]],
    canal: Dict[str, float],
    estado: Dict[str, int],
):
    cur = conn.cursor()
    cur.executemany(
        f"""INSERT INTO {SUMMARY_TABLE}
              (produto,centro,total_valor,total_qtd)
            VALUES (?,?,?,?)
            ON CONFLICT(produto,centro) DO UPDATE SET
              total_valor=total_valor+excluded.total_valor,
              total_qtd  =total_qtd  +excluded.total_qtd""",
        [(p, c, v, q) for (p, c), (v, q) in pc.items()],
    )
    cur.executemany(
        """INSERT INTO canal_summary (canal,total_valor)
           VALUES (?,?)
           ON CONFLICT(canal) DO UPDATE SET total_valor=total_valor+excluded.total_valor""",
        list(canal.items()),
    )
    cur.executemany(
        """INSERT INTO estado_summary (estado,total_qtd)
           VALUES (?,?)
           ON CONFLICT(estado) DO UPDATE SET total_qtd=total_qtd+excluded.total_qtd""",
        list(estado.items()),
    )
    conn.commit()