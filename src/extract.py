"""
Extraction and source preparation module for Workshop 2.

Handles:
- Operational extraction of Spotify from CSV at documented path.
- Operational extraction of Grammy Awards from MySQL source database.
- Initial preparation of Grammy source table from CSV into MySQL (Python-only, no LOAD DATA).
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text


def prepare_grammy_source(
    csv_path: str,
    mysql_url: str,
    chunksize: int = 1000,
) -> int:
    """
    Populates grammy_source.grammy_awards from the original CSV using Python only.
    No LOAD DATA, LOAD DATA LOCAL INFILE, or INTO OUTFILE is used.

    Converts:
    - published_at and updated_at ISO strings with timezone offsets to clean UTC DATETIME.
    - winner boolean (True/False to 1/0).
    - empty strings to NULL.
    - Records row count (must be exactly 4,810) into grammy_source_control table.
    """
    p = Path(csv_path)
    if not p.is_file():
        raise FileNotFoundError(f"Grammy CSV not found at {csv_path}")

    # Read CSV with standard parser
    df = pd.read_csv(p)
    raw_row_count = len(df)

    # 1. Standardize timestamps to clean UTC DATETIME
    for col in ("published_at", "updated_at"):
        if col in df.columns:
            # Parse ISO 8601 with tz, convert to UTC, format as string without tz suffix
            parsed = pd.to_datetime(df[col], utc=True, errors="coerce")
            df[col] = parsed.dt.strftime("%Y-%m-%d %H:%M:%S")

    # 2. Boolean winner to integer 0/1
    if "winner" in df.columns:
        df["winner"] = df["winner"].map(lambda v: 1 if str(v).strip().lower() in ("true", "1") else 0)

    # 3. Empty strings as None (NULL in MySQL)
    df = df.replace({"": None, pd.NA: None})
    df = df.where(pd.notna(df), None)

    # 4. Insert into MySQL table in chunks
    engine = create_engine(mysql_url)
    with engine.begin() as conn:
        # Create tables if not exist
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS grammy_awards (
                year INT NOT NULL,
                title VARCHAR(255) NULL,
                published_at DATETIME NULL,
                updated_at DATETIME NULL,
                category VARCHAR(255) NOT NULL,
                nominee VARCHAR(500) NULL,
                artist VARCHAR(500) NULL,
                workers TEXT NULL,
                img TEXT NULL,
                winner BOOLEAN NOT NULL DEFAULT 1
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS grammy_source_control (
                source_name VARCHAR(64) PRIMARY KEY,
                expected_rows INT NOT NULL,
                imported_rows INT NOT NULL,
                reconciled BOOLEAN NOT NULL DEFAULT 0,
                imported_at DATETIME NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """))
        # Clear previous records
        conn.execute(text("TRUNCATE TABLE grammy_awards;"))

        # Chunked insert using executemany
        insert_sql = text("""
            INSERT INTO grammy_awards (
                year, title, published_at, updated_at, category,
                nominee, artist, workers, img, winner
            ) VALUES (
                :year, :title, :published_at, :updated_at, :category,
                :nominee, :artist, :workers, :img, :winner
            )
        """)
        records = df.to_dict(orient="records")
        for i in range(0, len(records), chunksize):
            chunk = records[i:i + chunksize]
            conn.execute(insert_sql, chunk)

        # Verify imported count
        count_res = conn.execute(text("SELECT COUNT(*) FROM grammy_awards;")).scalar_one()
        is_reconciled = (count_res == 4810 and count_res == raw_row_count)

        # Record control entry
        conn.execute(text("""
            INSERT INTO grammy_source_control (
                source_name, expected_rows, imported_rows, reconciled, imported_at
            ) VALUES (
                'the_grammy_awards.csv', :expected, :imported, :rec, :now
            ) ON DUPLICATE KEY UPDATE
                expected_rows = VALUES(expected_rows),
                imported_rows = VALUES(imported_rows),
                reconciled = VALUES(reconciled),
                imported_at = VALUES(imported_at);
        """), {
            "expected": 4810,
            "imported": count_res,
            "rec": int(is_reconciled),
            "now": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        })

    if not is_reconciled:
        raise ValueError(
            f"Grammy source reconciliation failed: expected 4810 rows (CSV had {raw_row_count}), "
            f"but imported {count_res} rows into MySQL."
        )

    return count_res


def get_grammy_imported_count(mysql_url: str) -> int:
    """
    Reads the recorded imported row count from grammy_source_control table.

    Raises RuntimeError if the control record is missing or the database cannot
    be reached.  A missing control row is never treated as a valid DQ07 baseline.
    Run `python main.py prepare-source` first to populate the control table.
    """
    try:
        engine = create_engine(mysql_url)
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT imported_rows FROM grammy_source_control "
                    "WHERE source_name = 'the_grammy_awards.csv';"
                )
            ).fetchone()
    except Exception as exc:
        raise RuntimeError(
            f"[DQ07] Cannot connect to grammy_source_control at '{mysql_url}'. "
            f"Run 'python main.py prepare-source' first. Error: {exc}"
        ) from exc

    if row is None:
        raise RuntimeError(
            "[DQ07] No grammy_source_control record found for 'the_grammy_awards.csv'. "
            "Run 'python main.py prepare-source' first to populate the control row."
        )
    return int(row[0])


def extract_spotify(
    source_path: str,
    work_dir: str,
    batch_id: str,
) -> str:
    """
    Extracts raw Spotify dataset from CSV and persists it into the batch work folder.
    Preserves all columns without premature projection or cleaning.
    """
    p = Path(source_path)
    if not p.is_file():
        raise FileNotFoundError(f"Spotify source CSV not found at {source_path}")

    target_dir = Path(work_dir) / batch_id
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / "spotify_raw.parquet"

    df = pd.read_csv(p)
    df.to_parquet(target_file, index=False)
    return str(target_file)


def extract_grammys(
    mysql_url: str,
    work_dir: str,
    batch_id: str,
) -> str:
    """
    Extracts raw Grammy records from the MySQL source table (never from the CSV!).
    Preserves all columns and stores the result in the batch work folder.
    """
    target_dir = Path(work_dir) / batch_id
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / "grammy_raw.parquet"

    engine = create_engine(mysql_url)
    with engine.connect() as conn:
        df = pd.read_sql_query(text("SELECT * FROM grammy_awards;"), conn)

    df.to_parquet(target_file, index=False)
    return str(target_file)
