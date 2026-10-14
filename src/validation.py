"""
Automated Data Quality Validation module using Great Expectations 1.23.1.

Implements rules DQ01 through DQ15 across:
- spotify_raw
- grammy_raw
- prepared (fact and dimensions)

Persists Expectation Suites, Validation Definitions, and Checkpoints in gx/.
Produces machine-readable validation summary JSON files.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import great_expectations as gx
import pandas as pd
from great_expectations.expectations.metadata_types import FailureSeverity


# =====================================================================
# Persistent GX Context Helper
# =====================================================================

def get_gx_context(project_root_dir: Optional[str] = None, ephemeral_ok: bool = False) -> Any:
    """
    Returns a persistent file-based Great Expectations context.

    Raises RuntimeError if the persistent file context cannot be initialised,
    unless ephemeral_ok=True is explicitly passed (test-only override).
    """
    if not project_root_dir:
        candidates = [
            Path("/opt/airflow"),
            Path(__file__).resolve().parent.parent,
            Path("."),
        ]
        for c in candidates:
            if (c / "gx").is_dir() or (c / "gx" / "great_expectations.yml").is_file():
                project_root_dir = str(c)
                break
        if not project_root_dir:
            project_root_dir = str(Path(__file__).resolve().parent.parent)

    last_exc: Optional[Exception] = None
    for mode in ("file", None):
        try:
            if mode:
                return gx.get_context(mode=mode, project_root_dir=project_root_dir)
            else:
                return gx.get_context(project_root_dir=project_root_dir)
        except Exception as e:
            last_exc = e

    if ephemeral_ok:
        import warnings
        warnings.warn(
            "[GX] Persistent file context unavailable; running in ephemeral mode (test-only). "
            f"Root: {project_root_dir}. Error: {last_exc}",
            RuntimeWarning,
            stacklevel=2,
        )
        return gx.get_context(mode="ephemeral")

    raise RuntimeError(
        f"[GX] Cannot initialise persistent file-based context at '{project_root_dir}'. "
        f"GX persistence is required for normal execution. Error: {last_exc}"
    )


# =====================================================================
# GX Helper to build context, suite, validation definition, checkpoint, and run
# =====================================================================

def run_suite_validation(
    dataframe: pd.DataFrame,
    suite_name: str,
    expectations: List[Any],
    output_dir: Optional[str] = None,
    batch_id: Optional[str] = None,
) -> Tuple[Any, Any, Dict[str, Any]]:
    """
    Runs a Great Expectations validation on a pandas DataFrame with a persistent Expectation Suite.
    Uses result_format SUMMARY.
    Returns (result, max_failure_severity, summary_dict).
    """
    context = get_gx_context()

    source_name = f"{suite_name}_source"
    try:
        data_source = context.data_sources.get(source_name)
    except Exception:
        data_source = context.data_sources.add_pandas(name=source_name)

    asset_name = f"{suite_name}_asset"
    try:
        data_asset = data_source.get_asset(asset_name)
    except Exception:
        data_asset = data_source.add_dataframe_asset(name=asset_name)

    batch_def_name = f"{suite_name}_batch"
    try:
        batch_def = data_asset.get_batch_definition(batch_def_name)
    except Exception:
        batch_def = data_asset.add_batch_definition_whole_dataframe(batch_def_name)

    # Persistent suite
    suite = gx.ExpectationSuite(name=suite_name)
    for exp in expectations:
        suite.add_expectation(exp)
    suite = context.suites.add_or_update(suite)

    # Persistent validation definition
    val_def = gx.ValidationDefinition(
        name=f"{suite_name}_validation",
        data=batch_def,
        suite=suite,
    )
    val_def = context.validation_definitions.add_or_update(val_def)

    # Persistent checkpoint
    ckpt_name = f"{suite_name}_checkpoint"
    checkpoint = gx.Checkpoint(
        name=ckpt_name,
        validation_definitions=[val_def],
    )
    context.checkpoints.add_or_update(checkpoint)

    result = val_def.run(
        batch_parameters={"dataframe": dataframe},
        result_format={"result_format": "SUMMARY"},
    )

    max_failure = result.get_max_severity_failure()

    # Build machine-readable summary
    results_summary = {
        "suite_name": suite_name,
        "batch_id": batch_id,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "success": bool(result.success),
        "max_severity_failure": str(max_failure) if max_failure else None,
        "statistics": result.statistics,
        "rule_results": [],
    }

    for res in result.results:
        meta = getattr(res.expectation_config, "meta", {}) or {}
        results_summary["rule_results"].append({
            "rule_id": meta.get("rule_id", "UNKNOWN"),
            "expectation_type": res.expectation_config.type,
            "success": bool(res.success),
            "severity": meta.get("severity", "critical"),
            "result": res.result,
        })

    # Persist JSON report if directory provided
    if output_dir and batch_id:
        out_path = Path(output_dir) / batch_id
        out_path.mkdir(parents=True, exist_ok=True)
        file_path = out_path / f"{suite_name}_validation.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(results_summary, f, indent=2, default=str)

    return result, max_failure, results_summary


# =====================================================================
# 1. Spotify Raw Validation Gate (DQ01 - DQ05)
# =====================================================================

def validate_spotify_raw(
    spotify_path: str,
    output_dir: str = "data/validation_results",
    batch_id: str = "default_batch",
) -> Dict[str, Any]:
    """
    Validates incoming raw Spotify dataset:
    - DQ01: Required columns present (Critical)
    - DQ02: track_id and track_genre not null (Critical)
    - DQ03: popularity in [0, 100], non-null, and integer type (Critical)
    - DQ04: track_id, track_genre pair uniqueness (Warning, expected baseline)
    - DQ05: artists not null with mostly=0.99 (Critical)
    """
    if spotify_path.endswith(".parquet"):
        df = pd.read_parquet(spotify_path)
    else:
        df = pd.read_csv(spotify_path)

    expectations = [
        # DQ01 - Required columns
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=[
                "track_id", "track_name", "album_name", "artists", "popularity", "track_genre"
            ],
            exact_match=False,
            severity="critical",
            meta={"rule_id": "DQ01", "severity": "critical"},
        ),
        # DQ02 - track_id completeness
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="track_id",
            severity="critical",
            meta={"rule_id": "DQ02", "severity": "critical"},
        ),
        # DQ02 - track_genre completeness
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="track_genre",
            severity="critical",
            meta={"rule_id": "DQ02", "severity": "critical"},
        ),
        # DQ03 - popularity non-nullness, integer type, and [0, 100] range
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="popularity",
            severity="critical",
            meta={"rule_id": "DQ03", "severity": "critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="popularity",
            type_="int64",
            severity="critical",
            meta={"rule_id": "DQ03", "severity": "critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="popularity",
            min_value=0,
            max_value=100,
            severity="critical",
            meta={"rule_id": "DQ03", "severity": "critical"},
        ),
        # DQ04 - track_id + track_genre uniqueness (Warning: 450 duplicate rows expected in raw)
        gx.expectations.ExpectCompoundColumnsToBeUnique(
            column_list=["track_id", "track_genre"],
            severity="warning",
            meta={"rule_id": "DQ04", "severity": "warning"},
        ),
        # DQ05 - artists completeness (>= 99%)
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="artists",
            mostly=0.99,
            severity="critical",
            meta={"rule_id": "DQ05", "severity": "critical"},
        ),
    ]

    result, max_failure, summary = run_suite_validation(
        dataframe=df,
        suite_name="spotify_raw",
        expectations=expectations,
        output_dir=output_dir,
        batch_id=batch_id,
    )

    if max_failure == FailureSeverity.CRITICAL:
        raise ValueError(f"Spotify raw validation failed with CRITICAL severity. Summary: {summary['statistics']}")

    return summary


# =====================================================================
# 2. Grammy Raw Validation Gate (DQ01, DQ06, DQ07, DQ08)
# =====================================================================

def validate_grammy_raw(
    grammy_path: str,
    expected_import_count: Optional[int] = None,
    mysql_url: Optional[str] = None,
    output_dir: str = "data/validation_results",
    batch_id: str = "default_batch",
) -> Dict[str, Any]:
    """
    Validates incoming raw Grammy dataset:
    - DQ01: Required columns present (Critical)
    - DQ06: year and category completeness; year in [1958, 2019] (Critical)
    - DQ07: extracted count matches imported count (Critical)
    - DQ08: at most 2 rows per (year, category) for year <= 2018 (Critical)
    """
    if grammy_path.endswith(".parquet"):
        df = pd.read_parquet(grammy_path)
    else:
        df = pd.read_csv(grammy_path)

    # Determine baseline import count for DQ07
    if expected_import_count is None:
        if mysql_url:
            from src.extract import get_grammy_imported_count
            expected_import_count = get_grammy_imported_count(mysql_url)
        else:
            expected_import_count = 4810

    # Helper for DQ08: safely count rows per (year, category) for 1958-2018 without crashing if columns missing
    df_work = df.copy()
    df_work["dq08_row_count"] = 1
    if "year" in df_work.columns and "category" in df_work.columns:
        year_num = pd.to_numeric(df_work["year"], errors="coerce").fillna(9999)
        historical_mask = year_num <= 2018
        historical_counts = df_work.loc[historical_mask].groupby(["year", "category"])["category"].transform("count")
        df_work.loc[historical_mask, "dq08_row_count"] = historical_counts

    expectations = [
        # DQ01 - Required columns
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=["year", "category", "nominee", "artist"],
            exact_match=False,
            severity="critical",
            meta={"rule_id": "DQ01", "severity": "critical"},
        ),
        # DQ06 - year not null and in [1958, 2019]
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="year",
            severity="critical",
            meta={"rule_id": "DQ06", "severity": "critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="year",
            min_value=1958,
            max_value=2019,
            severity="critical",
            meta={"rule_id": "DQ06", "severity": "critical"},
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="category",
            severity="critical",
            meta={"rule_id": "DQ06", "severity": "critical"},
        ),
        # DQ07 - Table row count matches expected import count
        gx.expectations.ExpectTableRowCountToEqual(
            value=expected_import_count,
            severity="critical",
            meta={"rule_id": "DQ07", "severity": "critical"},
        ),
        # DQ08 - At most 2 rows per (year, category) for 1958-2018
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="dq08_row_count",
            min_value=1,
            max_value=2,
            severity="critical",
            meta={"rule_id": "DQ08", "severity": "critical"},
        ),
    ]

    result, max_failure, summary = run_suite_validation(
        dataframe=df_work,
        suite_name="grammy_raw",
        expectations=expectations,
        output_dir=output_dir,
        batch_id=batch_id,
    )

    if max_failure == FailureSeverity.CRITICAL:
        raise ValueError(f"Grammy raw validation failed with CRITICAL severity. Summary: {summary['statistics']}")

    return summary


# =====================================================================
# 3. Prepared Validation Gate (DQ09 - DQ15)
# =====================================================================

def validate_prepared(
    prepared_metadata: Dict[str, Any],
    output_dir: str = "data/validation_results",
    batch_id: str = "default_batch",
) -> Dict[str, Any]:
    """
    Validates prepared analytical datasets before Data Warehouse load:
    - DQ09: Schema structure and types for all four prepared datasets
            (fact, dim_artist, dim_track, dim_genre) (Critical)
    - DQ10: Fact key uniqueness (track_id, track_genre) (Critical)
    - DQ11: Row count conservation (raw - copies - quarantined = prepared) (Critical)
    - DQ12: Primary artist consistency, referential integrity, and DIM_ARTIST uniqueness (Critical)
    - DQ13: Flag consistency with popularity and p90 threshold (Critical)
    - DQ14: Grammy attribute domains, band coherence, and record indicator coherence (Critical)
    - DQ15: Match rate >= 10% between resolvable Grammy rows and primary Spotify artists (Critical)
    """
    fact_df = pd.read_parquet(prepared_metadata["fact_path"])
    dim_artist_df = pd.read_parquet(prepared_metadata["dim_artist_path"])
    dim_track_df = pd.read_parquet(prepared_metadata["dim_track_path"])
    dim_genre_df = pd.read_parquet(prepared_metadata["dim_genre_path"])

    p90 = prepared_metadata["p90_popularity_threshold"]
    raw_spotify = prepared_metadata["raw_spotify_rows"]
    exact_copies = prepared_metadata["exact_copies_removed"]
    quarantined = prepared_metadata["quarantined_rows"]
    expected_fact_count = raw_spotify - exact_copies - quarantined

    # Prepare helper columns for fact validation
    fact_work = fact_df.copy()
    fact_work["dq13_zero_coherent"] = (fact_work["is_zero_traction"] == (fact_work["popularity"] == 0)).astype(int)
    fact_work["dq13_top10_coherent"] = (fact_work["is_top10_global"] == (fact_work["popularity"] >= p90)).astype(int)

    # DQ12 referential integrity: every fact artist_key_norm exists in DIM_ARTIST
    artist_keys_set = set(dim_artist_df["artist_key_norm"].dropna())
    fact_work["dq12_artist_in_dim"] = fact_work["artist_key_norm"].isin(artist_keys_set).astype(int)

    # Prepare helper columns for artist dimension validation
    artist_work = dim_artist_df.copy()
    artist_work["dq14_status_coherent"] = artist_work.apply(
        lambda r: int(
            (r["grammy_wins_1958_2018"] > 0 and r["grammy_records_2019"] > 0 and r["grammy_status"] == "winner + 2019") or
            (r["grammy_wins_1958_2018"] > 0 and r["grammy_records_2019"] == 0 and r["grammy_status"] == "historical winner") or
            (r["grammy_wins_1958_2018"] == 0 and r["grammy_records_2019"] > 0 and r["grammy_status"] == "2019 only") or
            (r["grammy_wins_1958_2018"] == 0 and r["grammy_records_2019"] == 0 and r["grammy_status"] == "no record")
        ), axis=1
    )

    def check_wins_band(r: pd.Series) -> int:
        w = r["grammy_wins_1958_2018"]
        b = str(r["wins_band"])
        if w <= 0:
            return int(b == "none")
        if w == 1:
            return int(b == "1")
        if 2 <= w <= 4:
            return int(b == "2-4")
        if 5 <= w <= 9:
            return int(b == "5-9")
        return int(b == "10+")

    artist_work["dq14_band_coherent"] = artist_work.apply(check_wins_band, axis=1)
    artist_work["dq14_has_record_coherent"] = (
        (artist_work["has_grammy_record"] == 1) == (artist_work["grammy_status"] != "no record")
    ).astype(int)

    # 1. Fact expectations
    fact_expectations = [
        # DQ09 - Schema columns
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=[
                "track_id", "track_genre", "artist_key_norm", "popularity",
                "is_top10_global", "is_zero_traction", "source_row_id",
                "etl_batch_id", "loaded_at"
            ],
            exact_match=False,
            severity="critical",
            meta={"rule_id": "DQ09", "severity": "critical"},
        ),
        # DQ09 - Schema types
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="popularity",
            type_="int64",
            severity="critical",
            meta={"rule_id": "DQ09", "severity": "critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="is_top10_global",
            type_="int8",
            severity="critical",
            meta={"rule_id": "DQ09", "severity": "critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="is_zero_traction",
            type_="int8",
            severity="critical",
            meta={"rule_id": "DQ09", "severity": "critical"},
        ),
        # DQ10 - Fact table key uniqueness
        gx.expectations.ExpectCompoundColumnsToBeUnique(
            column_list=["track_id", "track_genre"],
            severity="critical",
            meta={"rule_id": "DQ10", "severity": "critical"},
        ),
        # DQ11 - Prepared row count conservation
        gx.expectations.ExpectTableRowCountToEqual(
            value=expected_fact_count,
            severity="critical",
            meta={"rule_id": "DQ11", "severity": "critical"},
        ),
        # DQ12 - Primary artist present and referential integrity
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="artist_key_norm",
            severity="critical",
            meta={"rule_id": "DQ12", "severity": "critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="dq12_artist_in_dim",
            value_set=[1],
            severity="critical",
            meta={"rule_id": "DQ12", "severity": "critical"},
        ),
        # DQ13 - Flags coherence
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="dq13_zero_coherent",
            value_set=[1],
            severity="critical",
            meta={"rule_id": "DQ13", "severity": "critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="dq13_top10_coherent",
            value_set=[1],
            severity="critical",
            meta={"rule_id": "DQ13", "severity": "critical"},
        ),
    ]

    res_fact, max_fail_fact, summary_fact = run_suite_validation(
        dataframe=fact_work,
        suite_name="prepared_fact",
        expectations=fact_expectations,
        output_dir=output_dir,
        batch_id=batch_id,
    )

    # 2. Artist dimension expectations
    artist_expectations = [
        # DQ09 - Schema columns
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=[
                "artist_key_norm", "artist_name", "grammy_wins_1958_2018",
                "grammy_records_2019", "grammy_status", "has_grammy_record", "wins_band"
            ],
            exact_match=False,
            severity="critical",
            meta={"rule_id": "DQ09", "severity": "critical"},
        ),
        # DQ09 - Schema types
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="grammy_wins_1958_2018",
            type_="int16",
            severity="critical",
            meta={"rule_id": "DQ09", "severity": "critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="has_grammy_record",
            type_="int8",
            severity="critical",
            meta={"rule_id": "DQ09", "severity": "critical"},
        ),
        # DQ12 - Artist key norm uniqueness in DIM_ARTIST
        gx.expectations.ExpectColumnValuesToBeUnique(
            column="artist_key_norm",
            severity="critical",
            meta={"rule_id": "DQ12", "severity": "critical"},
        ),
        # DQ14 - Grammy attribute domains and coherence
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="grammy_status",
            value_set=["historical winner", "winner + 2019", "2019 only", "no record"],
            severity="critical",
            meta={"rule_id": "DQ14", "severity": "critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="wins_band",
            value_set=["none", "1", "2-4", "5-9", "10+"],
            severity="critical",
            meta={"rule_id": "DQ14", "severity": "critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="has_grammy_record",
            value_set=[0, 1],
            severity="critical",
            meta={"rule_id": "DQ14", "severity": "critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="dq14_status_coherent",
            value_set=[1],
            severity="critical",
            meta={"rule_id": "DQ14", "severity": "critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="dq14_band_coherent",
            value_set=[1],
            severity="critical",
            meta={"rule_id": "DQ14", "severity": "critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="dq14_has_record_coherent",
            value_set=[1],
            severity="critical",
            meta={"rule_id": "DQ14", "severity": "critical"},
        ),
    ]

    res_artist, max_fail_artist, summary_artist = run_suite_validation(
        dataframe=artist_work,
        suite_name="prepared_dim_artist",
        expectations=artist_expectations,
        output_dir=output_dir,
        batch_id=batch_id,
    )

    # 3. DIM_TRACK schema/type expectations (DQ09)
    track_expectations = [
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=["track_id", "track_name", "album_name", "artists_raw", "n_artists"],
            exact_match=True,
            severity="critical",
            meta={"rule_id": "DQ09", "severity": "critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeUnique(
            column="track_id",
            severity="critical",
            meta={"rule_id": "DQ09", "severity": "critical"},
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="track_id",
            severity="critical",
            meta={"rule_id": "DQ09", "severity": "critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="n_artists",
            type_="int8",
            severity="critical",
            meta={"rule_id": "DQ09", "severity": "critical"},
        ),
    ]

    res_track, max_fail_track, summary_track = run_suite_validation(
        dataframe=dim_track_df,
        suite_name="prepared_dim_track",
        expectations=track_expectations,
        output_dir=output_dir,
        batch_id=batch_id,
    )

    # 4. DIM_GENRE schema/type expectations (DQ09)
    genre_expectations = [
        gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=["track_genre"],
            exact_match=True,
            severity="critical",
            meta={"rule_id": "DQ09", "severity": "critical"},
        ),
        gx.expectations.ExpectColumnValuesToBeUnique(
            column="track_genre",
            severity="critical",
            meta={"rule_id": "DQ09", "severity": "critical"},
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="track_genre",
            severity="critical",
            meta={"rule_id": "DQ09", "severity": "critical"},
        ),
    ]

    res_genre, max_fail_genre, summary_genre = run_suite_validation(
        dataframe=dim_genre_df,
        suite_name="prepared_dim_genre",
        expectations=genre_expectations,
        output_dir=output_dir,
        batch_id=batch_id,
    )

    # 5. DQ15 - Match rate threshold check (>= 10%)
    match_rate = float(prepared_metadata.get("grammy_match_rate_pct", 0.0))
    dq15_success = bool(match_rate >= 10.0)
    dq15_rule_record = {
        "rule_id": "DQ15",
        "expectation_type": "ExpectGrammyMatchRateGreaterThanOrEqualTo10Pct",
        "success": dq15_success,
        "severity": "critical",
        "observed_value": match_rate,
        "threshold": 10.0,
        "result": {
            "observed_value": match_rate,
            "threshold": 10.0,
            "grammy_matched_rows": prepared_metadata.get("grammy_matched_rows"),
            "grammy_resolvable_rows": prepared_metadata.get("grammy_resolvable_rows"),
        },
    }
    summary_fact["rule_results"].append(dq15_rule_record)

    # Update on-disk fact validation report with DQ15 included
    if output_dir and batch_id:
        out_path = Path(output_dir) / batch_id
        out_path.mkdir(parents=True, exist_ok=True)
        fact_file = out_path / "prepared_fact_validation.json"
        with open(fact_file, "w", encoding="utf-8") as f:
            json.dump(summary_fact, f, indent=2, default=str)

    # Consolidated prepared validation report
    overall_success = bool(
        res_fact.success
        and res_artist.success
        and res_track.success
        and res_genre.success
        and dq15_success
    )
    prepared_summary = {
        "suite_name": "prepared",
        "batch_id": batch_id,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "success": overall_success,
        "max_severity_failure": str(FailureSeverity.CRITICAL) if not overall_success else None,
        "statistics": {
            "fact_evaluated": summary_fact["statistics"],
            "dim_artist_evaluated": summary_artist["statistics"],
            "dim_track_evaluated": summary_track["statistics"],
            "dim_genre_evaluated": summary_genre["statistics"],
            "dq15_success": dq15_success,
            "grammy_match_rate_pct": match_rate,
        },
        "dq15": dq15_rule_record,
        "rule_results": (
            summary_fact["rule_results"]
            + summary_artist["rule_results"]
            + summary_track["rule_results"]
            + summary_genre["rule_results"]
        ),
    }

    if output_dir and batch_id:
        out_path = Path(output_dir) / batch_id
        prep_file = out_path / "prepared_validation.json"
        with open(prep_file, "w", encoding="utf-8") as f:
            json.dump(prepared_summary, f, indent=2, default=str)

    if not dq15_success:
        raise ValueError(f"DQ15 violated: match rate {match_rate}% is below 10.0% threshold.")

    if (
        max_fail_fact == FailureSeverity.CRITICAL
        or max_fail_artist == FailureSeverity.CRITICAL
        or max_fail_track == FailureSeverity.CRITICAL
        or max_fail_genre == FailureSeverity.CRITICAL
    ):
        raise ValueError("Prepared data validation failed with CRITICAL severity. Loading is blocked.")

    return {
        "fact_summary": summary_fact,
        "artist_summary": summary_artist,
        "track_summary": summary_track,
        "genre_summary": summary_genre,
        "prepared_summary": prepared_summary,
        "success": True,
    }
