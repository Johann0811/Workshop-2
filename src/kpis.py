"""
KPI calculations and cross-check verification module for Workshop 2.

Handles:
- DDL execution to create KPI views in DW.
- Independent Python (pandas) recomputation of K1-K6 from DW tables.
- Cross-check comparison between pandas calculations and SQL views.
- Fails with explicit discrepancy reports if SQL views and pandas diverge.
- Export of sample status alerts (small sample, no data) for K4 and K6.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text


def create_kpi_views(mysql_url: str, kpi_sql_path: Optional[str] = None) -> None:
    """Executes sql/kpi_queries.sql to create uppercase analytical views in dw database."""
    if not kpi_sql_path:
        candidates = [
            Path("/opt/airflow/sql/kpi_queries.sql"),
            Path(__file__).resolve().parent.parent / "sql" / "kpi_queries.sql",
            Path("sql/kpi_queries.sql"),
        ]
        for c in candidates:
            if c.is_file():
                kpi_sql_path = str(c)
                break
        if not kpi_sql_path:
            kpi_sql_path = str(Path("sql/kpi_queries.sql"))

    with open(kpi_sql_path, "r", encoding="utf-8") as f:
        sql_content = f.read()

    engine = create_engine(mysql_url)
    with engine.begin() as conn:
        for statement in sql_content.split(";"):
            stmt = statement.strip()
            if stmt and not stmt.upper().startswith("USE "):
                conn.execute(text(stmt))


def compute_pandas_kpis(conn) -> Dict[str, pd.DataFrame]:
    """
    Independently computes K1 through K6 using pandas by reading DW base tables.
    """
    df_fact = pd.read_sql_query(text("SELECT * FROM FACT_TRACK_GENRE;"), conn)
    df_artist = pd.read_sql_query(text("SELECT * FROM DIM_ARTIST;"), conn)
    df_genre = pd.read_sql_query(text("SELECT * FROM DIM_GENRE;"), conn)

    # Base joined table
    df_base = df_fact.merge(df_artist, on="artist_key").merge(df_genre, on="genre_key")

    def derive_r2_group(r: pd.Series) -> str:
        if r["grammy_wins_1958_2018"] >= 1:
            return "with_award"
        elif r["grammy_status"] == "no record":
            return "no_record"
        return "only_2019"

    df_base["r2_group"] = df_base.apply(derive_r2_group, axis=1)

    # K1: Genre Concentration
    k1 = df_base.groupby("track_genre").agg(
        total_tracks=("track_key", "count"),
        tracks_with_grammy_record=("has_grammy_record", "sum"),
        k1_pct_with_grammy_record=("has_grammy_record", lambda s: round(s.mean() * 100, 2)),
    ).reset_index().sort_values(["k1_pct_with_grammy_record", "total_tracks"], ascending=[False, False])

    # K2: Popularity Comparison
    k2_sub = df_base[df_base["r2_group"].isin(["with_award", "no_record"])]
    k2 = k2_sub.groupby("r2_group").agg(
        total_tracks=("track_key", "count"),
        mean_popularity=("popularity", lambda s: round(s.mean(), 2)),
        median_popularity=("popularity", lambda s: round(float(s.median()), 2)),
    ).reset_index()

    # K3: Top 10% Ratio
    k3_sub = df_base[df_base["r2_group"].isin(["with_award", "no_record"])]
    k3_group = k3_sub.groupby("r2_group").agg(
        total_tracks=("track_key", "count"),
        top10_tracks=("is_top10_global", "sum"),
        pct_top10=("is_top10_global", lambda s: round(s.mean() * 100, 2)),
    ).reset_index()

    no_rec_pct = k3_group.loc[k3_group["r2_group"] == "no_record", "pct_top10"].values
    base_no_rec = no_rec_pct[0] if len(no_rec_pct) > 0 and no_rec_pct[0] > 0 else 1.0
    k3_group["ratio_vs_no_record"] = k3_group["pct_top10"].apply(lambda p: round(p / base_no_rec, 2))
    k3 = k3_group

    # K4: Award Bands
    k4_sub = df_base[df_base["r2_group"] == "with_award"]
    k4 = k4_sub.groupby("wins_band").agg(
        distinct_artists=("artist_key", "nunique"),
        total_tracks=("track_key", "count"),
        mean_popularity=("popularity", lambda s: round(s.mean(), 2)),
        median_popularity=("popularity", lambda s: round(float(s.median()), 2)),
        pct_top10=("is_top10_global", lambda s: round(s.mean() * 100, 2)),
    ).reset_index()

    def sample_status_k4(cnt: int) -> str:
        if cnt >= 30:
            return "ok"
        elif cnt > 0:
            return "small sample"
        return "no data"

    k4["sample_status"] = k4["distinct_artists"].apply(sample_status_k4)
    band_order = {"1": 1, "2-4": 2, "5-9": 3, "10+": 4}
    k4["sort_key"] = k4["wins_band"].map(lambda b: band_order.get(b, 99))
    k4 = k4.sort_values("sort_key").drop(columns=["sort_key"])

    # K5: Zero Traction
    k5 = df_base.groupby("has_grammy_record").agg(
        total_tracks=("track_key", "count"),
        zero_traction_tracks=("is_zero_traction", "sum"),
        k5_pct_zero_traction=("is_zero_traction", lambda s: round(s.mean() * 100, 2)),
    ).reset_index()
    k5["catalog_segment"] = k5["has_grammy_record"].apply(
        lambda h: "artists_with_grammy" if h == 1 else "rest_of_catalog"
    )
    k5 = k5[["catalog_segment", "has_grammy_record", "total_tracks", "zero_traction_tracks", "k5_pct_zero_traction"]].sort_values(
        "has_grammy_record", ascending=False
    )

    # K6: Zero Traction by Genre for Grammy Artists
    def calc_k6_pct(df_g: pd.DataFrame) -> Optional[float]:
        gr_tracks = df_g["has_grammy_record"].sum()
        if gr_tracks == 0:
            return None
        zero_gr = ((df_g["has_grammy_record"] == 1) & (df_g["is_zero_traction"] == 1)).sum()
        return round((zero_gr * 100.0) / gr_tracks, 2)

    k6_records = []
    for genre, g_df in df_base.groupby("track_genre"):
        total_g = len(g_df)
        gr_tracks = int(g_df["has_grammy_record"].sum())
        zero_gr = int(((g_df["has_grammy_record"] == 1) & (g_df["is_zero_traction"] == 1)).sum())
        pct = calc_k6_pct(g_df)
        if gr_tracks >= 30:
            st = "ok"
        elif gr_tracks > 0:
            st = "small sample"
        else:
            st = "no data"
        k6_records.append({
            "track_genre": genre,
            "total_genre_tracks": total_g,
            "grammy_artist_tracks": gr_tracks,
            "zero_traction_grammy_tracks": zero_gr,
            "k6_pct_zero_traction": pct,
            "sample_status": st,
        })
    k6 = pd.DataFrame(k6_records).sort_values(
        ["k6_pct_zero_traction", "grammy_artist_tracks"],
        ascending=[False, False]
    )

    return {
        "K1": k1,
        "K2": k2,
        "K3": k3,
        "K4": k4,
        "K5": k5,
        "K6": k6,
    }


def kpi_crosscheck(
    dw_mysql_url: str,
    output_dir: str = "data/output",
    evidence_dir: str = "docs/evidence",
    tolerance: float = 0.05,
) -> Dict[str, Any]:
    """
    Executes an independent cross-check between SQL views and pandas calculations from DW.
    Verifies that VW_KPI_1 through VW_KPI_6 match pandas calculations within tolerance.
    Reports any discrepancies and raises ValueError if any calculation disagrees.
    Exports non-ok sample status rows to data/output/ and docs/evidence/.
    """
    engine = create_engine(dw_mysql_url)
    out_p = Path(output_dir)
    ev_p = Path(evidence_dir)
    out_p.mkdir(parents=True, exist_ok=True)
    ev_p.mkdir(parents=True, exist_ok=True)

    with engine.connect() as conn:
        # 1. Independent pandas computation
        pandas_kpis = compute_pandas_kpis(conn)

        # 2. Fetch SQL views
        sql_kpis = {
            "K1": pd.read_sql_query(text("SELECT * FROM VW_KPI_1_GENRE_CONCENTRATION;"), conn),
            "K2": pd.read_sql_query(text("SELECT * FROM VW_KPI_2_POPULARITY_COMPARISON;"), conn),
            "K3": pd.read_sql_query(text("SELECT * FROM VW_KPI_3_TOP10_RATIO;"), conn),
            "K4": pd.read_sql_query(text("SELECT * FROM VW_KPI_4_AWARD_BANDS;"), conn),
            "K5": pd.read_sql_query(text("SELECT * FROM VW_KPI_5_ZERO_TRACTION;"), conn),
            "K6": pd.read_sql_query(text("SELECT * FROM VW_KPI_6_ZERO_TRACTION_BY_GENRE;"), conn),
        }

    # 3. Numeric cross-check between pandas and SQL
    discrepancies: List[Dict[str, Any]] = []
    comparisons: Dict[str, Any] = {}

    # Define key, numeric, and categorical columns to verify for each KPI
    kpi_schemas = {
        "K1": {"key": "track_genre", "num_cols": ["total_tracks", "tracks_with_grammy_record", "k1_pct_with_grammy_record"], "cat_cols": []},
        "K2": {"key": "r2_group", "num_cols": ["total_tracks", "mean_popularity", "median_popularity"], "cat_cols": []},
        "K3": {"key": "r2_group", "num_cols": ["total_tracks", "top10_tracks", "pct_top10", "ratio_vs_no_record"], "cat_cols": []},
        "K4": {"key": "wins_band", "num_cols": ["distinct_artists", "total_tracks", "mean_popularity", "median_popularity", "pct_top10"], "cat_cols": ["sample_status"]},
        "K5": {"key": "has_grammy_record", "num_cols": ["total_tracks", "zero_traction_tracks", "k5_pct_zero_traction"], "cat_cols": []},
        "K6": {"key": "track_genre", "num_cols": ["total_genre_tracks", "grammy_artist_tracks", "zero_traction_grammy_tracks", "k6_pct_zero_traction"], "cat_cols": ["sample_status"]},
    }

    for kpi_name, schema in kpi_schemas.items():
        pdf = pandas_kpis[kpi_name]
        sqldf = sql_kpis[kpi_name]
        key = schema["key"]
        num_cols = schema["num_cols"]
        cat_cols = schema.get("cat_cols", [])

        # Normalise key column types to string for consistent comparison
        pdf_keys = set(pdf[key].astype(str))
        sql_keys = set(sqldf[key].astype(str))

        # --- 1. Exact key-set equality ---
        missing_from_sql = pdf_keys - sql_keys
        missing_from_pandas = sql_keys - pdf_keys
        if missing_from_sql or missing_from_pandas:
            discrepancies.append({
                "kpi": kpi_name,
                "type": "key_set_mismatch",
                "missing_from_sql": sorted(missing_from_sql)[:10],
                "missing_from_pandas": sorted(missing_from_pandas)[:10],
                "pandas_key_count": len(pdf_keys),
                "sql_key_count": len(sql_keys),
            })
            comparisons[kpi_name] = {
                "rows_verified": 0,
                "discrepancies_count": 1,
                "status": "FAIL",
                "reason": "key_set_mismatch",
            }
            continue

        # --- 2. Row count equality ---
        if len(pdf) != len(sqldf):
            discrepancies.append({
                "kpi": kpi_name,
                "type": "row_count_mismatch",
                "pandas_rows": len(pdf),
                "sql_rows": len(sqldf),
            })
            comparisons[kpi_name] = {
                "rows_verified": 0,
                "discrepancies_count": 1,
                "status": "FAIL",
                "reason": "row_count_mismatch",
            }
            continue

        # --- 3. Merge on key; verify merged count == both inputs ---
        merged = pdf.merge(sqldf, on=key, suffixes=("_pandas", "_sql"))
        if len(merged) != len(pdf):
            discrepancies.append({
                "kpi": kpi_name,
                "type": "merge_row_loss",
                "pandas_rows": len(pdf),
                "sql_rows": len(sqldf),
                "merged_rows": len(merged),
            })
            comparisons[kpi_name] = {
                "rows_verified": len(merged),
                "discrepancies_count": 1,
                "status": "FAIL",
                "reason": "merge_row_loss",
            }
            continue

        kpi_diffs = []

        # --- 4. Numeric column comparison ---
        for col in num_cols:
            col_p = f"{col}_pandas"
            col_s = f"{col}_sql"
            # Fail if expected column is missing instead of silently skipping
            if col_p not in merged.columns or col_s not in merged.columns:
                discrepancies.append({
                    "kpi": kpi_name,
                    "type": "missing_column",
                    "column": col,
                    "pandas_has": col_p in merged.columns,
                    "sql_has": col_s in merged.columns,
                })
                kpi_diffs.append({"kpi": kpi_name, "column": col, "type": "missing_column"})
                continue

            for _, row in merged.iterrows():
                val_p = row[col_p]
                val_s = row[col_s]

                # Both None / NaN is acceptable
                if pd.isna(val_p) and pd.isna(val_s):
                    continue

                diff = abs(float(val_p) - float(val_s))
                if diff > tolerance:
                    discrepancy = {
                        "kpi": kpi_name,
                        "key": str(row[key]),
                        "column": col,
                        "pandas_val": float(val_p),
                        "sql_val": float(val_s),
                        "diff": round(diff, 4),
                    }
                    discrepancies.append(discrepancy)
                    kpi_diffs.append(discrepancy)

        # --- 5. Categorical column comparison ---
        for col in cat_cols:
            col_p = f"{col}_pandas"
            col_s = f"{col}_sql"
            if col_p not in merged.columns or col_s not in merged.columns:
                discrepancies.append({
                    "kpi": kpi_name,
                    "type": "missing_categorical_column",
                    "column": col,
                })
                kpi_diffs.append({"kpi": kpi_name, "column": col, "type": "missing_column"})
                continue
            mismatches = merged[merged[col_p].astype(str) != merged[col_s].astype(str)]
            for _, row in mismatches.iterrows():
                discrepancy = {
                    "kpi": kpi_name,
                    "key": str(row[key]),
                    "column": col,
                    "pandas_val": str(row[col_p]),
                    "sql_val": str(row[col_s]),
                    "type": "categorical_mismatch",
                }
                discrepancies.append(discrepancy)
                kpi_diffs.append(discrepancy)

        comparisons[kpi_name] = {
            "rows_verified": len(merged),
            "discrepancies_count": len(kpi_diffs),
            "status": "PASS" if len(kpi_diffs) == 0 else "FAIL",
        }

    # 4. Handle sample status alerts for K4 and K6.
    # Always overwrite both CSVs (even if empty) so the evidence files are never stale.
    k4_df = sql_kpis["K4"]
    k4_non_ok = k4_df[k4_df["sample_status"] != "ok"]
    k4_non_ok.to_csv(out_p / "k4_sample_alerts.csv", index=False)
    k4_non_ok.to_csv(ev_p / "k4_sample_alerts.csv", index=False)

    k6_df = sql_kpis["K6"]
    k6_non_ok = k6_df[k6_df["sample_status"] != "ok"]
    k6_non_ok.to_csv(out_p / "k6_sample_alerts.csv", index=False)
    k6_non_ok.to_csv(ev_p / "k6_sample_alerts.csv", index=False)

    # 5. Build report
    success = len(discrepancies) == 0
    report = {
        "status": "SUCCESS" if success else "FAILED",
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "tolerance": tolerance,
        "comparisons": comparisons,
        "discrepancies_count": len(discrepancies),
        "discrepancies": discrepancies,
    }

    # Persist report to data/output/ and docs/evidence/
    with open(out_p / "kpi_crosscheck_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    with open(ev_p / "kpi_crosscheck_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    if not success:
        raise ValueError(
            f"KPI cross-check failed with {len(discrepancies)} discrepancies between pandas and SQL: {discrepancies[:3]}"
        )

    return report
