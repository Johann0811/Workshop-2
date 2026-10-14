"""
Transformation and integration module for Workshop 2.

Handles:
- Shared Unicode key normalization for artists (used by pipeline and profiling notebook).
- Exact copies detection and removal for Spotify.
- Explicit quarantine of invalid Spotify rows (null artists).
- Grammy artist key resolution (including Best New Artist exception and lead-artist 'Featuring' handling).
- Grammy aggregation and status/band attribute derivation.
- Construction of prepared dimension and fact datasets.
"""

from __future__ import annotations

import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd


# =====================================================================
# 1. Shared Artist Normalization
# =====================================================================

def normalize_artist_name(name: Optional[str]) -> Optional[str]:
    """
    Shared Unicode normalization for artist names.

    Specification (§6.8, §10):
    1. Unicode NFKD decomposition.
    2. Remove combining diacritical marks.
    3. Lowercase.
    4. Remove non-word characters except spaces ([^\\w\\s]).
    5. Collapse multiple whitespace characters into a single space and strip.
    6. Empty result becomes None / NA (no key).
    """
    if name is None or not isinstance(name, str):
        return None
    # 1. NFKD
    nfkd = unicodedata.normalize("NFKD", name)
    # 2. Strip combining marks
    no_marks = "".join(c for c in nfkd if not unicodedata.combining(c))
    # 3. Lowercase
    lowered = no_marks.lower()
    # 4. Remove non-word chars except spaces
    no_punct = re.sub(r"[^\w\s]", "", lowered)
    # 5. Collapse whitespace and strip
    collapsed = re.sub(r"\s+", " ", no_punct).strip()
    return collapsed if collapsed else None


# =====================================================================
# 2. Business Status and Band Derivations
# =====================================================================

def derive_wins_band(wins: int) -> str:
    """
    Map Grammy wins count (1958-2018) to the standard band.
    Values: 'none', '1', '2-4', '5-9', '10+'.
    """
    if wins is None or wins <= 0:
        return "none"
    if wins == 1:
        return "1"
    if 2 <= wins <= 4:
        return "2-4"
    if 5 <= wins <= 9:
        return "5-9"
    return "10+"


def derive_grammy_status(wins_1958_2018: int, records_2019: int) -> str:
    """
    Derive artist Grammy status per §6.1 business definitions.
    - historical winner: wins in 1958-2018, no 2019 records.
    - winner + 2019: wins in 1958-2018 AND records in 2019.
    - 2019 only: 0 wins in 1958-2018, has 2019 records.
    - no record: 0 wins and 0 records.
    """
    has_wins = (wins_1958_2018 or 0) > 0
    has_2019 = (records_2019 or 0) > 0

    if has_wins and has_2019:
        return "winner + 2019"
    if has_wins and not has_2019:
        return "historical winner"
    if not has_wins and has_2019:
        return "2019 only"
    return "no record"


def resolve_grammy_artist_name(artist: Any, nominee: Any, category: Any) -> Optional[str]:
    """
    Resolve Grammy artist string per §6.8 / §10:
    - If category is 'Best New Artist' (case-insensitive) and artist is null/empty,
      nominee holds the artist name.
    - If artist contains ' Featuring ', use the lead artist before ' Featuring '.
    - Exclude generic '(Various Artists)' marker.
    """
    cat_str = str(category or "").strip().lower()
    is_bna = "best new artist" in cat_str

    raw_name: Optional[str] = None
    if isinstance(artist, str) and artist.strip():
        raw_name = artist.strip()
    elif is_bna and isinstance(nominee, str) and nominee.strip():
        raw_name = nominee.strip()

    if not raw_name:
        return None

    # Filter out generic marker
    if raw_name.lower() in ("(various artists)", "various artists"):
        return None

    # Split on " Featuring " (case-insensitive) and take lead artist
    featuring_split = re.split(r"\s+featuring\s+", raw_name, flags=re.IGNORECASE)
    lead_artist = featuring_split[0].strip()

    if lead_artist.lower() in ("(various artists)", "various artists"):
        return None

    return lead_artist if lead_artist else None


# =====================================================================
# 3. Main Transformation & Integration Pipeline Function
# =====================================================================

def transform_and_integrate(
    spotify_raw_path: str,
    grammy_raw_path: str,
    output_dir: str,
    batch_id: str,
) -> Dict[str, Any]:
    """
    Executes the single transform_and_integrate phase (§6.8).

    Reads extracted raw files, cleans/deduplicates Spotify, resolves and
    aggregates Grammy records, joins at grain (N:1), calculates analytical flags,
    and writes prepared datasets as Parquet files.
    """
    work_path = Path(output_dir) / batch_id
    work_path.mkdir(parents=True, exist_ok=True)

    # 1. Load extracted raw data
    if spotify_raw_path.endswith(".parquet"):
        spotify_df = pd.read_parquet(spotify_raw_path)
    else:
        spotify_df = pd.read_csv(spotify_raw_path)

    if grammy_raw_path.endswith(".parquet"):
        grammy_df = pd.read_parquet(grammy_raw_path)
    else:
        grammy_df = pd.read_csv(grammy_raw_path)

    raw_spotify_rows = len(spotify_df)
    raw_grammy_rows = len(grammy_df)

    # 2. Spotify: identify row index for traceability
    index_col = "Unnamed: 0" if "Unnamed: 0" in spotify_df.columns else None
    if index_col:
        spotify_df["source_row_id"] = pd.to_numeric(spotify_df[index_col], errors="coerce").fillna(0).astype(int)
    elif "source_row_id" not in spotify_df.columns:
        spotify_df["source_row_id"] = np.arange(len(spotify_df), dtype=int)

    # Data columns to inspect for exact copies (all columns except the index/source_row_id)
    data_cols = [c for c in spotify_df.columns if c not in ("Unnamed: 0", "source_row_id")]

    # Exact copies: identical across all data columns
    is_duplicate = spotify_df.duplicated(subset=data_cols, keep="first")
    exact_copies_count = int(is_duplicate.sum())

    dedup_spotify = spotify_df.loc[~is_duplicate].copy()

    # 3. Quarantine rows with missing artists (explicit business rule)
    quarantine_mask = dedup_spotify["artists"].isna() | (dedup_spotify["artists"].astype(str).str.strip() == "")
    quarantined_rows_count = int(quarantine_mask.sum())

    quarantine_records = []
    if quarantined_rows_count > 0:
        quarantined_df = dedup_spotify.loc[quarantine_mask].copy()
        for _, q_row in quarantined_df.iterrows():
            quarantine_records.append({
                "etl_batch_id": batch_id,
                "source_row_id": int(q_row["source_row_id"]),
                "track_id": str(q_row.get("track_id", "")),
                "reason": "missing_artist",
                "raw_data": str(q_row.to_dict()),
                "quarantined_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            })

    valid_spotify = dedup_spotify.loc[~quarantine_mask].copy()
    candidate_fact_rows = len(valid_spotify)

    # 4. Derive primary artist and artist normalized keys
    valid_spotify["artists_raw"] = valid_spotify["artists"].astype(str)
    artist_tokens = valid_spotify["artists_raw"].str.split(";")
    valid_spotify["n_artists"] = artist_tokens.apply(len).astype(np.int8)
    valid_spotify["primary_artist_name"] = artist_tokens.str[0].str.strip()
    valid_spotify["artist_key_norm"] = valid_spotify["primary_artist_name"].map(normalize_artist_name)

    # 5. Process Grammy source: resolution and aggregation
    grammy_resolvable_count = 0
    grammy_work = grammy_df.copy()
    grammy_work["resolved_artist"] = grammy_work.apply(
        lambda r: resolve_grammy_artist_name(r.get("artist"), r.get("nominee"), r.get("category")),
        axis=1
    )
    grammy_work["artist_key_norm"] = grammy_work["resolved_artist"].map(normalize_artist_name)

    # Resolvable rows: non-null normalized key
    resolvable_mask = grammy_work["artist_key_norm"].notna()
    grammy_resolvable_count = int(resolvable_mask.sum())

    resolvable_grammy = grammy_work.loc[resolvable_mask].copy()
    resolvable_grammy["year_int"] = pd.to_numeric(resolvable_grammy["year"], errors="coerce").fillna(0).astype(int)

    # Aggregate by artist_key_norm
    grammy_agg = resolvable_grammy.groupby("artist_key_norm").agg(
        grammy_wins_1958_2018=("year_int", lambda s: int((s <= 2018).sum())),
        grammy_records_2019=("year_int", lambda s: int((s == 2019).sum())),
    ).reset_index()

    grammy_agg["grammy_status"] = grammy_agg.apply(
        lambda r: derive_grammy_status(r["grammy_wins_1958_2018"], r["grammy_records_2019"]),
        axis=1
    )
    grammy_agg["has_grammy_record"] = 1
    grammy_agg["wins_band"] = grammy_agg["grammy_wins_1958_2018"].map(derive_wins_band)

    # 6. Build DIM_ARTIST: only Spotify primary artists!
    # Display name rule: most frequent original spelling among names mapping to key
    spelling_counts = valid_spotify.groupby(["artist_key_norm", "primary_artist_name"]).size().reset_index(name="cnt")
    best_spellings = spelling_counts.sort_values(["artist_key_norm", "cnt"], ascending=[True, False]).drop_duplicates(
        subset=["artist_key_norm"], keep="first"
    )[["artist_key_norm", "primary_artist_name"]].rename(columns={"primary_artist_name": "artist_name"})

    dim_artist = best_spellings.merge(grammy_agg, on="artist_key_norm", how="left")
    dim_artist["grammy_wins_1958_2018"] = dim_artist["grammy_wins_1958_2018"].fillna(0).astype(np.int16)
    dim_artist["grammy_records_2019"] = dim_artist["grammy_records_2019"].fillna(0).astype(np.int16)
    dim_artist["grammy_status"] = dim_artist["grammy_status"].fillna("no record").astype(str)
    dim_artist["has_grammy_record"] = dim_artist["has_grammy_record"].fillna(0).astype(np.int8)
    dim_artist["wins_band"] = dim_artist["wins_band"].fillna("none").astype(str)

    # Matching metrics
    grammy_matched_rows = int(
        resolvable_grammy["artist_key_norm"].isin(set(dim_artist["artist_key_norm"])).sum()
    )
    match_rate_pct = (
        round((grammy_matched_rows / grammy_resolvable_count) * 100, 2)
        if grammy_resolvable_count > 0 else 0.0
    )

    # 7. Build DIM_TRACK: unique tracks in valid Spotify
    dim_track = (
        valid_spotify
        .sort_values("source_row_id")
        .drop_duplicates(subset=["track_id"], keep="first")
        [[
            "track_id",
            "track_name",
            "album_name",
            "artists_raw",
            "n_artists",
        ]]
        .copy()
    )
    dim_track["track_name"] = dim_track["track_name"].fillna("").astype(str)
    dim_track["album_name"] = dim_track["album_name"].fillna("").astype(str)

    # 8. Build DIM_GENRE: unique genres in valid Spotify
    dim_genre = pd.DataFrame({
        "track_genre": sorted(valid_spotify["track_genre"].dropna().unique())
    })

    # 9. Build FACT_TRACK_GENRE
    # Popularity percentile 90 across all candidate fact rows
    popularity_p90 = int(np.percentile(valid_spotify["popularity"], 90))

    valid_spotify["is_top10_global"] = (valid_spotify["popularity"] >= popularity_p90).astype(np.int8)
    valid_spotify["is_zero_traction"] = (valid_spotify["popularity"] == 0).astype(np.int8)
    valid_spotify["etl_batch_id"] = batch_id
    valid_spotify["loaded_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    fact_df = valid_spotify[[
        "track_id",
        "track_genre",
        "artist_key_norm",
        "popularity",
        "is_top10_global",
        "is_zero_traction",
        "source_row_id",
        "etl_batch_id",
        "loaded_at",
    ]].copy()

    # 10. Persist prepared datasets as Parquet
    dim_track_path = work_path / "dim_track.parquet"
    dim_artist_path = work_path / "dim_artist.parquet"
    dim_genre_path = work_path / "dim_genre.parquet"
    fact_path = work_path / "fact_track_genre.parquet"
    quarantine_path = work_path / "quarantine.parquet"

    dim_track.to_parquet(dim_track_path, index=False)
    dim_artist.to_parquet(dim_artist_path, index=False)
    dim_genre.to_parquet(dim_genre_path, index=False)
    fact_df.to_parquet(fact_path, index=False)

    quarantine_df = pd.DataFrame(quarantine_records)
    if not quarantine_df.empty:
        quarantine_df.to_parquet(quarantine_path, index=False)

    metadata = {
        "batch_id": batch_id,
        "raw_spotify_rows": raw_spotify_rows,
        "raw_grammy_rows": raw_grammy_rows,
        "exact_copies_removed": exact_copies_count,
        "quarantined_rows": quarantined_rows_count,
        "candidate_fact_rows": candidate_fact_rows,
        "dim_track_count": len(dim_track),
        "dim_artist_count": len(dim_artist),
        "dim_genre_count": len(dim_genre),
        "fact_rows_count": len(fact_df),
        "grammy_resolvable_rows": grammy_resolvable_count,
        "grammy_matched_rows": grammy_matched_rows,
        "grammy_match_rate_pct": match_rate_pct,
        "p90_popularity_threshold": popularity_p90,
        "dim_track_path": str(dim_track_path),
        "dim_artist_path": str(dim_artist_path),
        "dim_genre_path": str(dim_genre_path),
        "fact_path": str(fact_path),
        "quarantine_path": str(quarantine_path) if not quarantine_df.empty else None,
    }

    # 11. Persist batch metadata
    batch_meta_file = work_path / "batch_metadata.json"
    with open(batch_meta_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    return metadata
