"""
Data Warehouse loading module for Workshop 2.

Handles:
- DDL execution and DW schema creation/reset.
- Transactional loading and upserting of dimensions (DIM_TRACK, DIM_ARTIST, DIM_GENRE).
- Resolution of surrogate keys.
- Bulk loading and idempotent upserting of FACT_TRACK_GENRE.
- Recording of batch audit metrics in etl_control_batch and quarantine_spotify.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import create_engine, text


def create_dw_schema(mysql_url: str, schema_sql_path: Optional[str] = None) -> None:
    """Executes the DDL script to create star schema and control tables in dw database."""
    if not schema_sql_path:
        schema_sql_path = str(Path(__file__).resolve().parent.parent / "sql" / "dw_schema.sql")

    with open(schema_sql_path, "r", encoding="utf-8") as f:
        ddl_script = f.read()

    engine = create_engine(mysql_url)
    with engine.begin() as conn:
        # Split on semicolon, omitting empty statements and USE statements
        for statement in ddl_script.split(";"):
            stmt = statement.strip()
            if stmt and not stmt.upper().startswith("USE "):
                conn.execute(text(stmt))


def reset_dw(mysql_url: str, schema_sql_path: Optional[str] = None) -> None:
    """
    Drops and recreates all DW tables and control tables.
    Never touches grammy_source or airflow_db!
    """
    engine = create_engine(mysql_url)
    with engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        conn.execute(text("DROP TABLE IF EXISTS FACT_TRACK_GENRE;"))
        conn.execute(text("DROP TABLE IF EXISTS DIM_TRACK;"))
        conn.execute(text("DROP TABLE IF EXISTS DIM_ARTIST;"))
        conn.execute(text("DROP TABLE IF EXISTS DIM_GENRE;"))
        conn.execute(text("DROP TABLE IF EXISTS etl_control_batch;"))
        conn.execute(text("DROP TABLE IF EXISTS quarantine_spotify;"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

    create_dw_schema(mysql_url, schema_sql_path)


def load_dw(
    prepared_metadata: Dict[str, Any],
    mysql_url: str,
    chunksize: int = 5000,
) -> Dict[str, Any]:
    """
    Loads prepared Parquet datasets into the MySQL Data Warehouse inside a single transaction.
    Uses chunked INSERT ... ON DUPLICATE KEY UPDATE to guarantee idempotency and safe reruns.
    """
    batch_id = prepared_metadata["batch_id"]
    dim_track_df = pd.read_parquet(prepared_metadata["dim_track_path"])
    dim_artist_df = pd.read_parquet(prepared_metadata["dim_artist_path"])
    dim_genre_df = pd.read_parquet(prepared_metadata["dim_genre_path"])
    fact_df = pd.read_parquet(prepared_metadata["fact_path"])

    quarantine_df = None
    if prepared_metadata.get("quarantine_path") and Path(prepared_metadata["quarantine_path"]).is_file():
        quarantine_df = pd.read_parquet(prepared_metadata["quarantine_path"])

    engine = create_engine(mysql_url)

    with engine.begin() as conn:
        # 1. Upsert DIM_GENRE
        genre_records = dim_genre_df.to_dict(orient="records")
        conn.execute(text("""
            INSERT INTO DIM_GENRE (track_genre)
            VALUES (:track_genre)
            ON DUPLICATE KEY UPDATE track_genre = VALUES(track_genre);
        """), genre_records)

        # 2. Upsert DIM_ARTIST
        artist_records = dim_artist_df.to_dict(orient="records")
        for i in range(0, len(artist_records), chunksize):
            chunk = artist_records[i:i + chunksize]
            conn.execute(text("""
                INSERT INTO DIM_ARTIST (
                    artist_key_norm, artist_name, grammy_wins_1958_2018,
                    grammy_records_2019, grammy_status, has_grammy_record, wins_band
                ) VALUES (
                    :artist_key_norm, :artist_name, :grammy_wins_1958_2018,
                    :grammy_records_2019, :grammy_status, :has_grammy_record, :wins_band
                ) ON DUPLICATE KEY UPDATE
                    artist_name = VALUES(artist_name),
                    grammy_wins_1958_2018 = VALUES(grammy_wins_1958_2018),
                    grammy_records_2019 = VALUES(grammy_records_2019),
                    grammy_status = VALUES(grammy_status),
                    has_grammy_record = VALUES(has_grammy_record),
                    wins_band = VALUES(wins_band);
            """), chunk)

        # 3. Upsert DIM_TRACK
        track_records = dim_track_df.to_dict(orient="records")
        for i in range(0, len(track_records), chunksize):
            chunk = track_records[i:i + chunksize]
            conn.execute(text("""
                INSERT INTO DIM_TRACK (
                    track_id, track_name, album_name, artists_raw, n_artists
                ) VALUES (
                    :track_id, :track_name, :album_name, :artists_raw, :n_artists
                ) ON DUPLICATE KEY UPDATE
                    track_name = VALUES(track_name),
                    album_name = VALUES(album_name),
                    artists_raw = VALUES(artists_raw),
                    n_artists = VALUES(n_artists);
            """), chunk)

        # 4. Fetch Surrogate Keys
        genre_lookup = dict(conn.execute(text("SELECT track_genre, genre_key FROM DIM_GENRE;")).fetchall())
        artist_lookup = dict(conn.execute(text("SELECT artist_key_norm, artist_key FROM DIM_ARTIST;")).fetchall())
        track_lookup = dict(conn.execute(text("SELECT track_id, track_key FROM DIM_TRACK;")).fetchall())

        # Map surrogate keys onto fact dataframe
        fact_df["genre_key"] = fact_df["track_genre"].map(genre_lookup)
        fact_df["artist_key"] = fact_df["artist_key_norm"].map(artist_lookup)
        fact_df["track_key"] = fact_df["track_id"].map(track_lookup)

        # Check for unmapped keys
        if fact_df["genre_key"].isna().any() or fact_df["artist_key"].isna().any() or fact_df["track_key"].isna().any():
            raise ValueError("Surrogate key resolution failed for one or more fact rows.")

        fact_df["genre_key"] = fact_df["genre_key"].astype(int)
        fact_df["artist_key"] = fact_df["artist_key"].astype(int)
        fact_df["track_key"] = fact_df["track_key"].astype(int)

        fact_cols = [
            "track_key", "genre_key", "artist_key", "popularity",
            "is_top10_global", "is_zero_traction", "source_row_id",
            "etl_batch_id", "loaded_at"
        ]
        fact_insert_records = fact_df[fact_cols].to_dict(orient="records")

        # 5. Upsert FACT_TRACK_GENRE
        fact_sql = text("""
            INSERT INTO FACT_TRACK_GENRE (
                track_key, genre_key, artist_key, popularity,
                is_top10_global, is_zero_traction, source_row_id,
                etl_batch_id, loaded_at
            ) VALUES (
                :track_key, :genre_key, :artist_key, :popularity,
                :is_top10_global, :is_zero_traction, :source_row_id,
                :etl_batch_id, :loaded_at
            ) ON DUPLICATE KEY UPDATE
                artist_key = VALUES(artist_key),
                popularity = VALUES(popularity),
                is_top10_global = VALUES(is_top10_global),
                is_zero_traction = VALUES(is_zero_traction),
                source_row_id = VALUES(source_row_id),
                etl_batch_id = VALUES(etl_batch_id),
                loaded_at = VALUES(loaded_at);
        """)

        for i in range(0, len(fact_insert_records), chunksize):
            chunk = fact_insert_records[i:i + chunksize]
            conn.execute(fact_sql, chunk)

        # 6. Insert quarantine records if any (idempotent upsert)
        if quarantine_df is not None and not quarantine_df.empty:
            q_records = quarantine_df.to_dict(orient="records")
            conn.execute(text("""
                INSERT INTO quarantine_spotify (
                    etl_batch_id, source_row_id, track_id, reason, raw_data, quarantined_at
                ) VALUES (
                    :etl_batch_id, :source_row_id, :track_id, :reason, :raw_data, :quarantined_at
                ) ON DUPLICATE KEY UPDATE
                    etl_batch_id = VALUES(etl_batch_id),
                    track_id = VALUES(track_id),
                    raw_data = VALUES(raw_data),
                    quarantined_at = VALUES(quarantined_at);
            """), q_records)

        # 7. Record batch metrics in etl_control_batch
        conn.execute(text("""
            INSERT INTO etl_control_batch (
                batch_id, started_at, completed_at, raw_spotify_rows,
                exact_copies_removed, quarantined_rows, candidate_fact_rows,
                fact_rows_loaded, dim_track_count, dim_artist_count, dim_genre_count,
                grammy_imported_rows, grammy_extracted_rows, grammy_resolvable_rows,
                grammy_matched_rows, grammy_match_rate_pct, p90_popularity_threshold,
                status
            ) VALUES (
                :batch_id, :now, :now, :raw_spotify_rows,
                :exact_copies_removed, :quarantined_rows, :candidate_fact_rows,
                :fact_rows_loaded, :dim_track_count, :dim_artist_count, :dim_genre_count,
                :grammy_imported_rows, :grammy_extracted_rows, :grammy_resolvable_rows,
                :grammy_matched_rows, :grammy_match_rate_pct, :p90_popularity_threshold,
                'SUCCESS'
            ) ON DUPLICATE KEY UPDATE
                completed_at = VALUES(completed_at),
                raw_spotify_rows = VALUES(raw_spotify_rows),
                exact_copies_removed = VALUES(exact_copies_removed),
                quarantined_rows = VALUES(quarantined_rows),
                candidate_fact_rows = VALUES(candidate_fact_rows),
                fact_rows_loaded = VALUES(fact_rows_loaded),
                dim_track_count = VALUES(dim_track_count),
                dim_artist_count = VALUES(dim_artist_count),
                dim_genre_count = VALUES(dim_genre_count),
                grammy_imported_rows = VALUES(grammy_imported_rows),
                grammy_extracted_rows = VALUES(grammy_extracted_rows),
                grammy_resolvable_rows = VALUES(grammy_resolvable_rows),
                grammy_matched_rows = VALUES(grammy_matched_rows),
                grammy_match_rate_pct = VALUES(grammy_match_rate_pct),
                p90_popularity_threshold = VALUES(p90_popularity_threshold),
                status = VALUES(status);
        """), {
            "batch_id": batch_id,
            "now": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "raw_spotify_rows": prepared_metadata["raw_spotify_rows"],
            "exact_copies_removed": prepared_metadata["exact_copies_removed"],
            "quarantined_rows": prepared_metadata["quarantined_rows"],
            "candidate_fact_rows": prepared_metadata["candidate_fact_rows"],
            "fact_rows_loaded": len(fact_df),
            "dim_track_count": len(dim_track_df),
            "dim_artist_count": len(dim_artist_df),
            "dim_genre_count": len(dim_genre_df),
            "grammy_imported_rows": prepared_metadata.get("grammy_imported_rows", prepared_metadata["raw_grammy_rows"]),
            "grammy_extracted_rows": prepared_metadata["raw_grammy_rows"],
            "grammy_resolvable_rows": prepared_metadata["grammy_resolvable_rows"],
            "grammy_matched_rows": prepared_metadata["grammy_matched_rows"],
            "grammy_match_rate_pct": prepared_metadata["grammy_match_rate_pct"],
            "p90_popularity_threshold": prepared_metadata["p90_popularity_threshold"],
        })

    return {
        "batch_id": batch_id,
        "fact_rows_loaded": len(fact_df),
        "dim_track_count": len(dim_track_df),
        "dim_artist_count": len(dim_artist_df),
        "dim_genre_count": len(dim_genre_df),
        "status": "SUCCESS",
    }
