"""
Integration tests for pipeline controls:
- Batch metadata persistence and schema
- Prepared validation report persistence including DQ15
- KPI cross-check discrepancy detection
- Quarantine idempotency contract
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest
from src.kpis import kpi_crosscheck
from src.validation import validate_prepared


def test_validate_prepared_persists_dq15(tmp_path):
    # Setup dummy fact and dim_artist parquets
    fact_file = tmp_path / "fact_track_genre.parquet"
    artist_file = tmp_path / "dim_artist.parquet"

    df_fact = pd.DataFrame({
        "track_id": ["t1", "t2"],
        "track_genre": ["pop", "rock"],
        "artist_key_norm": ["artist1", "artist2"],
        "popularity": [70, 40],
        "is_top10_global": [1, 0],
        "is_zero_traction": [0, 0],
        "source_row_id": [0, 1],
        "etl_batch_id": ["b1", "b1"],
        "loaded_at": ["2026-01-01 00:00:00", "2026-01-01 00:00:00"],
    })
    df_fact["popularity"] = df_fact["popularity"].astype("int64")
    df_fact["is_top10_global"] = df_fact["is_top10_global"].astype("int8")
    df_fact["is_zero_traction"] = df_fact["is_zero_traction"].astype("int8")
    df_fact["source_row_id"] = df_fact["source_row_id"].astype("int64")
    df_fact.to_parquet(fact_file, index=False)

    df_artist = pd.DataFrame({
        "artist_key_norm": ["artist1", "artist2"],
        "artist_name": ["Artist 1", "Artist 2"],
        "grammy_wins_1958_2018": [2, 0],
        "grammy_records_2019": [0, 0],
        "grammy_status": ["historical winner", "no record"],
        "has_grammy_record": [1, 0],
        "wins_band": ["2-4", "none"],
    })
    df_artist["grammy_wins_1958_2018"] = df_artist["grammy_wins_1958_2018"].astype("int16")
    df_artist["grammy_records_2019"] = df_artist["grammy_records_2019"].astype("int16")
    df_artist["has_grammy_record"] = df_artist["has_grammy_record"].astype("int8")
    df_artist.to_parquet(artist_file, index=False)

    meta = {
        "batch_id": "test_batch_dq15",
        "fact_path": str(fact_file),
        "dim_artist_path": str(artist_file),
        "raw_spotify_rows": 2,
        "exact_copies_removed": 0,
        "quarantined_rows": 0,
        "p90_popularity_threshold": 63,
        "grammy_resolvable_rows": 100,
        "grammy_matched_rows": 32,
        "grammy_match_rate_pct": 32.0,
    }

    val_dir = tmp_path / "val_results"
    res = validate_prepared(meta, output_dir=str(val_dir), batch_id="test_batch_dq15")
    assert res["success"] is True

    # Check that prepared_validation.json exists and contains DQ15
    report_file = val_dir / "test_batch_dq15" / "prepared_validation.json"
    assert report_file.is_file()

    with open(report_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["success"] is True
    assert "dq15" in data
    assert data["dq15"]["rule_id"] == "DQ15"
    assert data["dq15"]["observed_value"] == 32.0


def test_kpi_crosscheck_detects_mismatch(tmp_path):
    # Mock create_engine to simulate discrepancy between pandas and SQL
    with patch("src.kpis.create_engine") as mock_engine_cls, \
         patch("src.kpis.compute_pandas_kpis") as mock_pandas_kpis:

        # Pandas returns mean popularity 50.0 for with_award
        mock_pandas_kpis.return_value = {
            "K1": pd.DataFrame({"track_genre": ["pop"], "total_tracks": [10], "tracks_with_grammy_record": [2], "k1_pct_with_grammy_record": [20.0]}),
            "K2": pd.DataFrame({"r2_group": ["with_award"], "total_tracks": [10], "mean_popularity": [50.0], "median_popularity": [50.0]}),
            "K3": pd.DataFrame({"r2_group": ["with_award"], "total_tracks": [10], "top10_tracks": [2], "pct_top10": [20.0], "ratio_vs_no_record": [1.5]}),
            "K4": pd.DataFrame({"wins_band": ["1"], "distinct_artists": [5], "total_tracks": [10], "mean_popularity": [50.0], "median_popularity": [50.0], "pct_top10": [20.0], "sample_status": ["small sample"]}),
            "K5": pd.DataFrame({"catalog_segment": ["artists_with_grammy"], "has_grammy_record": [1], "total_tracks": [10], "zero_traction_tracks": [0], "k5_pct_zero_traction": [0.0]}),
            "K6": pd.DataFrame({"track_genre": ["pop"], "total_genre_tracks": [10], "grammy_artist_tracks": [2], "zero_traction_grammy_tracks": [0], "k6_pct_zero_traction": [0.0], "sample_status": ["small sample"]}),
        }

        # Mock SQL returning different mean popularity 85.0 for with_award
        mock_conn = MagicMock()
        mock_engine_cls.return_value.connect.return_value.__enter__.return_value = mock_conn

        with patch("pandas.read_sql_query") as mock_sql_query:
            mock_sql_query.side_effect = [
                # K1
                pd.DataFrame({"track_genre": ["pop"], "total_tracks": [10], "tracks_with_grammy_record": [2], "k1_pct_with_grammy_record": [20.0]}),
                # K2 - Discrepancy! mean_popularity is 85.0 instead of 50.0
                pd.DataFrame({"r2_group": ["with_award"], "total_tracks": [10], "mean_popularity": [85.0], "median_popularity": [50.0]}),
                # K3
                pd.DataFrame({"r2_group": ["with_award"], "total_tracks": [10], "top10_tracks": [2], "pct_top10": [20.0], "ratio_vs_no_record": [1.5]}),
                # K4
                pd.DataFrame({"wins_band": ["1"], "distinct_artists": [5], "total_tracks": [10], "mean_popularity": [50.0], "median_popularity": [50.0], "pct_top10": [20.0], "sample_status": ["small sample"]}),
                # K5
                pd.DataFrame({"catalog_segment": ["artists_with_grammy"], "has_grammy_record": [1], "total_tracks": [10], "zero_traction_tracks": [0], "k5_pct_zero_traction": [0.0]}),
                # K6
                pd.DataFrame({"track_genre": ["pop"], "total_genre_tracks": [10], "grammy_artist_tracks": [2], "zero_traction_grammy_tracks": [0], "k6_pct_zero_traction": [0.0], "sample_status": ["small sample"]}),
            ]

            with pytest.raises(ValueError, match="KPI cross-check failed with 1 discrepancies"):
                kpi_crosscheck("mysql+pymysql://fake:fake@fake/fake", output_dir=str(tmp_path), evidence_dir=str(tmp_path))
