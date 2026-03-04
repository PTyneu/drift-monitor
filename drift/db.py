"""Lightweight PostgreSQL client.

Design goals
------------
* One short-lived connection per query (no connection pool eating RAM inside
  Streamlit).
* Read-only, parameterised queries — safe and low-load.
* Returns plain ``pandas.DataFrame`` so the rest of the code never touches SQL.
"""

from __future__ import annotations

import pandas as pd
import psycopg2
import psycopg2.extras

from .config import DbConfig

# Columns we actually need — explicit SELECT avoids transferring blobs / wide
# text columns that may exist in the table but are irrelevant for drift stats.
_COLUMNS = (
    "coilid",
    "defectclass",
    "rawdefectclass",
    "bbox_xtl",
    "bbox_ytl",
    "bbox_xbr",
    "bbox_ybr",
    "confidence",
)


def _connect(cfg: DbConfig):
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.name,
        user=cfg.user,
        password=cfg.password,
        options="-c statement_timeout=30000",  # 30 s hard limit
    )


def fetch_new_coils(cfg: DbConfig, after: str | None = None) -> list[str]:
    """Return coil IDs that appeared after the given watermark.

    Uses ``WHERE coilid > %s`` — a single index seek, regardless of how many
    coils have already been processed.  When *after* is ``None`` (first run),
    returns all distinct coil IDs.
    """
    if after is not None:
        sql = f"SELECT DISTINCT coilid FROM {cfg.table} WHERE coilid > %s ORDER BY coilid"
        params: tuple = (after,)
    else:
        sql = f"SELECT DISTINCT coilid FROM {cfg.table} ORDER BY coilid"
        params = ()

    conn = _connect(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def fetch_coil_data(cfg: DbConfig, coil_id: str) -> pd.DataFrame:
    """Fetch all defect rows for a single coil.

    Only the columns needed for statistics are selected, and the query is
    parameterised to prevent injection.
    """
    cols = ", ".join(_COLUMNS)
    sql = f"SELECT {cols} FROM {cfg.table} WHERE coilid = %s"
    conn = _connect(cfg)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (coil_id,))
            rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        return pd.DataFrame(columns=list(_COLUMNS))
    return pd.DataFrame(rows)
