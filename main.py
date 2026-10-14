"""
Command Line Interface (CLI) single entry point for Workshop 2.

Supports orchestration outside Airflow, testing, and operational commands:
- prepare-source
- create-dw
- reset-dw
- extract
- validate-raw
- transform
- validate-prepared
- load
- run-pipeline
- create-kpi-views
- kpi-crosscheck
- make-test-batch
- rerun-check
- status
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# Add repo root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.extract import extract_grammys, extract_spotify, prepare_grammy_source
from src.kpis import create_kpi_views, kpi_crosscheck
from src.load import create_dw_schema, load_dw, reset_dw
from src.transform import transform_and_integrate
from src.validation import validate_grammy_raw, validate_prepared, validate_spotify_raw


# Default database URLs
def get_db_urls():
    mysql_host = os.getenv("MYSQL_HOST", "127.0.0.1")
    mysql_port = os.getenv("MYSQL_PORT", "3307")
    grammy_user = os.getenv("GRAMMY_DB_USER", "grammy_user")
    grammy_pass = os.getenv("GRAMMY_DB_PASSWORD", "grammy_pass")
    grammy_db = os.getenv("GRAMMY_DB_NAME", "grammy_source")

    dw_user = os.getenv("DW_DB_USER", "etl_user")
    dw_pass = os.getenv("DW_DB_PASSWORD", "etl_pass")
    dw_db = os.getenv("DW_DB_NAME", "dw")

    grammy_url = f"mysql+pymysql://{grammy_user}:{grammy_pass}@{mysql_host}:{mysql_port}/{grammy_db}"
    dw_url = f"mysql+pymysql://{dw_user}:{dw_pass}@{mysql_host}:{mysql_port}/{dw_db}"
    return grammy_url, dw_url


def find_spotify_path(user_path: Optional[str] = None) -> str:
    if user_path and Path(user_path).is_file():
        return user_path
    candidates = [
        Path("data/raw/spotify_dataset.csv"),
        Path("/opt/airflow/data/raw/spotify_dataset.csv"),
    ]
    for c in candidates:
        if c.is_file():
            return str(c.resolve())
    raise FileNotFoundError("Spotify dataset CSV not found. Expected at data/raw/spotify_dataset.csv.")


def find_grammy_csv(user_path: Optional[str] = None) -> str:
    if user_path and Path(user_path).is_file():
        return user_path
    candidates = [
        Path("data/raw/the_grammy_awards.csv"),
        Path("/opt/airflow/data/raw/the_grammy_awards.csv"),
    ]
    for c in candidates:
        if c.is_file():
            return str(c.resolve())
    raise FileNotFoundError("Grammy dataset CSV not found. Expected at data/raw/the_grammy_awards.csv.")


def main():
    parser = argparse.ArgumentParser(description="Workshop 2 Pipeline CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # prepare-source
    p_prep = subparsers.add_parser("prepare-source")
    p_prep.add_argument("--grammy-path", default=None)

    # create-dw
    subparsers.add_parser("create-dw")

    # reset-dw
    subparsers.add_parser("reset-dw")

    # extract
    p_ext = subparsers.add_parser("extract")
    p_ext.add_argument("--batch-id", default=None)
    p_ext.add_argument("--spotify-path", default=None)

    # validate-raw
    p_val_raw = subparsers.add_parser("validate-raw")
    p_val_raw.add_argument("--batch-id", default=None)

    # transform
    p_trans = subparsers.add_parser("transform")
    p_trans.add_argument("--batch-id", default=None)

    # validate-prepared
    p_val_prep = subparsers.add_parser("validate-prepared")
    p_val_prep.add_argument("--batch-id", default=None)

    # load
    p_load = subparsers.add_parser("load")
    p_load.add_argument("--batch-id", default=None)

    # run-pipeline
    p_all = subparsers.add_parser("run-pipeline")
    p_all.add_argument("--batch-id", default=None)
    p_all.add_argument("--spotify-path", default=None)

    # create-kpi-views
    subparsers.add_parser("create-kpi-views")

    # kpi-crosscheck
    subparsers.add_parser("kpi-crosscheck")

    # make-test-batch
    subparsers.add_parser("make-test-batch")

    # rerun-check
    p_rerun = subparsers.add_parser("rerun-check")
    p_rerun.add_argument("--batch-id", default=None)
    p_rerun.add_argument("--spotify-path", default=None)

    # status
    subparsers.add_parser("status")

    args = parser.parse_args()
    grammy_url, dw_url = get_db_urls()

    batch_id = getattr(args, "batch_id", None) or f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    work_dir = os.getenv("WORK_DIR", "data/work")

    try:
        if args.command == "prepare-source":
            csv_path = find_grammy_csv(args.grammy_path)
            print(f"[PREPARE-SOURCE] Loading Grammy awards from {csv_path} into MySQL...")
            count = prepare_grammy_source(csv_path, grammy_url)
            print(f"[PREPARE-SOURCE] Successfully loaded and reconciled {count} rows in grammy_source.grammy_awards.")
            sys.exit(0)

        elif args.command == "create-dw":
            print("[CREATE-DW] Creating dimensional Data Warehouse schema...")
            create_dw_schema(dw_url)
            print("[CREATE-DW] Schema successfully created.")
            sys.exit(0)

        elif args.command == "reset-dw":
            print("[RESET-DW] Resetting Data Warehouse tables and audit tables...")
            reset_dw(dw_url)
            print("[RESET-DW] Data Warehouse reset complete.")
            sys.exit(0)

        elif args.command == "extract":
            sp_path = find_spotify_path(args.spotify_path)
            print(f"[EXTRACT] Batch {batch_id}: Extracting Spotify from {sp_path}...")
            sp_raw = extract_spotify(sp_path, work_dir, batch_id)
            print(f"[EXTRACT] Batch {batch_id}: Extracting Grammy from MySQL...")
            gr_raw = extract_grammys(grammy_url, work_dir, batch_id)
            print(f"[EXTRACT] Done. Spotify: {sp_raw}, Grammy: {gr_raw}")
            sys.exit(0)

        elif args.command == "validate-raw":
            b_path = Path(work_dir) / batch_id
            sp_raw = str(b_path / "spotify_raw.parquet")
            gr_raw = str(b_path / "grammy_raw.parquet")
            print(f"[VALIDATE-RAW] Validating Spotify raw: {sp_raw}...")
            validate_spotify_raw(sp_raw, batch_id=batch_id)
            print(f"[VALIDATE-RAW] Validating Grammy raw: {gr_raw}...")
            validate_grammy_raw(gr_raw, mysql_url=grammy_url, batch_id=batch_id)
            print("[VALIDATE-RAW] Raw gates passed per policy.")
            sys.exit(0)

        elif args.command == "transform":
            b_path = Path(work_dir) / batch_id
            sp_raw = str(b_path / "spotify_raw.parquet")
            gr_raw = str(b_path / "grammy_raw.parquet")
            print(f"[TRANSFORM] Batch {batch_id}: Running transformation and integration...")
            meta = transform_and_integrate(sp_raw, gr_raw, work_dir, batch_id)
            print(f"[TRANSFORM] Fact candidate rows: {meta['fact_rows_count']}, Artist count: {meta['dim_artist_count']}")
            sys.exit(0)

        elif args.command == "validate-prepared":
            b_path = Path(work_dir) / batch_id
            meta_file = b_path / "batch_metadata.json"
            if not meta_file.is_file():
                print(f"[VALIDATE-PREPARED] Error: batch metadata not found at {meta_file}. Run 'transform' first.")
                sys.exit(2)
            with open(meta_file, "r", encoding="utf-8") as f:
                meta = json.load(f)
            val_dir = os.getenv("VAL_DIR", "data/validation_results")
            print(f"[VALIDATE-PREPARED] Batch {batch_id}: Running prepared validation gate...")
            validate_prepared(meta, output_dir=val_dir, batch_id=batch_id)
            print("[VALIDATE-PREPARED] Prepared validation gate passed.")
            sys.exit(0)

        elif args.command == "load":
            b_path = Path(work_dir) / batch_id
            meta_file = b_path / "batch_metadata.json"
            if not meta_file.is_file():
                print(f"[LOAD] Error: batch metadata not found at {meta_file}. Run 'transform' first.")
                sys.exit(2)
            with open(meta_file, "r", encoding="utf-8") as f:
                meta = json.load(f)

            # Enforce prepared validation gate requirement!
            val_dir = Path(os.getenv("VAL_DIR", "data/validation_results"))
            prep_val_file = val_dir / batch_id / "prepared_validation.json"
            if not prep_val_file.is_file():
                print(f"[LOAD] Error: prepared validation report not found at {prep_val_file}. Prepared gate must be executed and pass before loading.")
                sys.exit(1)
            with open(prep_val_file, "r", encoding="utf-8") as f:
                prep_val = json.load(f)
            if not prep_val.get("success", False):
                print(f"[LOAD] Error: prepared validation did not succeed for batch {batch_id}. Loading is blocked.")
                sys.exit(1)

            print(f"[LOAD] Batch {batch_id}: Loading into Data Warehouse...")
            res = load_dw(meta, dw_url)
            print(f"[LOAD] Loaded {res['fact_rows_loaded']} fact rows.")
            sys.exit(0)

        elif args.command == "run-pipeline":
            sp_path = find_spotify_path(args.spotify_path)
            print(f"[RUN-PIPELINE] Starting batch {batch_id} end-to-end...")
            # 1. Extract
            sp_raw = extract_spotify(sp_path, work_dir, batch_id)
            gr_raw = extract_grammys(grammy_url, work_dir, batch_id)
            # 2. Raw gates
            validate_spotify_raw(sp_raw, batch_id=batch_id)
            validate_grammy_raw(gr_raw, mysql_url=grammy_url, batch_id=batch_id)
            # 3. Transform
            meta = transform_and_integrate(sp_raw, gr_raw, work_dir, batch_id)
            # 4. Prepared gate
            validate_prepared(meta, batch_id=batch_id)
            # 5. Load
            res = load_dw(meta, dw_url)
            print(f"[RUN-PIPELINE] Batch {batch_id} completed successfully. Fact rows loaded: {res['fact_rows_loaded']}.")
            sys.exit(0)

        elif args.command == "create-kpi-views":
            print("[CREATE-KPI-VIEWS] Creating KPI views in Data Warehouse...")
            create_kpi_views(dw_url)
            print("[CREATE-KPI-VIEWS] Views successfully created.")
            sys.exit(0)

        elif args.command == "kpi-crosscheck":
            print("[KPI-CROSSCHECK] Executing KPI queries cross-check...")
            res = kpi_crosscheck(dw_url)
            print("[KPI-CROSSCHECK] Complete. Verified KPI views.")
            sys.exit(0)

        elif args.command == "make-test-batch":
            sp_path = find_spotify_path()
            test_dir = Path("data/test")
            test_dir.mkdir(parents=True, exist_ok=True)
            test_file = test_dir / "spotify_popularity_negativa.csv"
            df = pd.read_csv(sp_path)
            # Add 1 row with popularity = -5
            bad_row = df.iloc[0].to_dict()
            bad_row["track_id"] = "TESTNEGPOP" + "0" * 12
            bad_row["popularity"] = -5
            df_bad = pd.concat([df, pd.DataFrame([bad_row])], ignore_index=True)
            df_bad.to_csv(test_file, index=False)
            print(f"[MAKE-TEST-BATCH] Created {test_file} with {len(df_bad)} rows (appended row with popularity = -5).")
            sys.exit(0)

        elif args.command == "rerun-check":
            import hashlib

            sp_path = find_spotify_path(args.spotify_path)
            engine = create_engine(dw_url)
            ev_p = Path("docs/evidence")
            ev_p.mkdir(parents=True, exist_ok=True)

            # Per-table columns to exclude from hashing (legitimately change on every load)
            AUDIT_COLS_BY_TABLE = {
                "FACT_TRACK_GENRE":   {"etl_batch_id", "loaded_at"},
                "DIM_TRACK":          {"etl_batch_id", "loaded_at"},
                "DIM_ARTIST":         {"etl_batch_id", "loaded_at"},
                "DIM_GENRE":          {"etl_batch_id", "loaded_at"},
                # quarantine_id is AUTO_INCREMENT and changes on rerun;
                # etl_batch_id and quarantined_at are per-batch audit fields.
                "quarantine_spotify": {"etl_batch_id", "quarantined_at", "quarantine_id"},
            }

            # Business-key columns per table (used for duplicate check)
            # quarantine UNIQUE KEY is (source_row_id, reason) per dw_schema.sql
            BIZ_KEYS = {
                "FACT_TRACK_GENRE":   ["track_key", "genre_key"],
                "DIM_TRACK":          ["track_key"],
                "DIM_ARTIST":         ["artist_key"],
                "DIM_GENRE":          ["genre_key"],
                "quarantine_spotify": ["source_row_id", "reason"],
            }

            TABLES = list(BIZ_KEYS.keys())

            def snapshot(conn):
                """Return dict of {table_name: DataFrame} for all monitored tables."""
                snaps = {}
                for tbl in TABLES:
                    snaps[tbl] = pd.read_sql_query(text(f"SELECT * FROM {tbl};"), conn)
                return snaps

            def table_hash(df: pd.DataFrame, tbl: str) -> str:
                """Deterministic, order-independent hash of business columns only."""
                audit = AUDIT_COLS_BY_TABLE.get(tbl, set())
                biz_cols = [c for c in df.columns if c not in audit]
                if not biz_cols:
                    return hashlib.sha256(b"").hexdigest()
                df_biz = df[biz_cols].copy()
                # Sort rows by all business columns for order independence
                df_biz = df_biz.sort_values(by=list(df_biz.columns)).reset_index(drop=True)
                row_hashes = pd.util.hash_pandas_object(df_biz, index=False)
                combined = hashlib.sha256(row_hashes.to_csv().encode()).hexdigest()
                return combined

            print("[RERUN-CHECK] Snapshotting DW tables BEFORE rerun...")
            with engine.connect() as conn:
                before_snaps = snapshot(conn)

            before_counts  = {t: len(df) for t, df in before_snaps.items()}
            before_hashes  = {t: table_hash(df, t) for t, df in before_snaps.items()}

            print("[RERUN-CHECK] Row counts BEFORE:", before_counts)

            print(f"[RERUN-CHECK] Triggering rerun with batch {batch_id}...")
            rerun_exc: Optional[Exception] = None
            try:
                sp_raw = extract_spotify(sp_path, work_dir, batch_id)
                gr_raw = extract_grammys(grammy_url, work_dir, batch_id)
                val_dir = os.getenv("VAL_DIR", "data/validation_results")
                validate_spotify_raw(sp_raw, output_dir=val_dir, batch_id=batch_id)
                validate_grammy_raw(gr_raw, mysql_url=grammy_url, output_dir=val_dir, batch_id=batch_id)
                meta = transform_and_integrate(sp_raw, gr_raw, work_dir, batch_id)
                validate_prepared(meta, output_dir=val_dir, batch_id=batch_id)
                load_dw(meta, dw_url)
                print("[RERUN-CHECK] Rerun ETL completed successfully.")
            except Exception as exc:
                rerun_exc = exc
                print(f"[RERUN-CHECK] Rerun ETL raised an exception: {exc}", file=sys.stderr)

            print("[RERUN-CHECK] Snapshotting DW tables AFTER rerun...")
            with engine.connect() as conn:
                after_snaps = snapshot(conn)

            after_counts  = {t: len(df) for t, df in after_snaps.items()}
            after_hashes  = {t: table_hash(df, t) for t, df in after_snaps.items()}

            print("[RERUN-CHECK] Row counts AFTER:", after_counts)

            # ----- Build report -----
            lines = []
            lines.append("=" * 70)
            lines.append("RERUN-CHECK REPORT")
            lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
            lines.append(f"Rerun batch_id: {batch_id}")
            lines.append("=" * 70)

            all_pass = True

            for tbl in TABLES:
                bc = before_counts[tbl]
                ac = after_counts[tbl]
                bh = before_hashes[tbl]
                ah = after_hashes[tbl]
                count_ok = (bc == ac)
                hash_ok  = (bh == ah)
                tbl_pass = count_ok and hash_ok

                # Duplicate business-key check AFTER rerun
                biz_keys = BIZ_KEYS[tbl]
                after_df = after_snaps[tbl]
                valid_keys = [k for k in biz_keys if k in after_df.columns]
                if valid_keys:
                    dup_count = int(after_df.duplicated(subset=valid_keys).sum())
                else:
                    dup_count = 0
                dup_ok = (dup_count == 0)
                tbl_pass = tbl_pass and dup_ok
                if not tbl_pass:
                    all_pass = False

                lines.append(f"\nTable: {tbl}")
                lines.append(f"  Rows  BEFORE : {bc}")
                lines.append(f"  Rows  AFTER  : {ac}  {'OK' if count_ok else 'MISMATCH *** count changed'}")
                lines.append(f"  Hash  BEFORE : {bh}")
                lines.append(f"  Hash  AFTER  : {ah}  {'OK' if hash_ok else 'MISMATCH *** data changed'}")
                lines.append(f"  Biz-key dups  : {dup_count}  {'OK' if dup_ok else 'FAIL *** duplicates detected'}")
                lines.append(f"  Table result  : {'PASS' if tbl_pass else 'FAIL'}")

            lines.append("\n" + "=" * 70)
            if rerun_exc:
                lines.append(f"ETL rerun exception: {rerun_exc}")
            lines.append(f"OVERALL RESULT: {'PASS — idempotent rerun verified, zero duplication' if all_pass and not rerun_exc else 'FAIL'}")
            lines.append("=" * 70)

            report_text = "\n".join(lines)
            print("\n" + report_text)

            # Write evidence file (always, even on failure)
            out_file = ev_p / "rerun_check.txt"
            with open(out_file, "w", encoding="utf-8") as f:
                f.write(report_text + "\n")
            print(f"\n[RERUN-CHECK] Evidence written to {out_file}")

            if rerun_exc:
                raise rerun_exc
            sys.exit(0 if all_pass else 1)

        elif args.command == "status":
            engine_gr = create_engine(grammy_url)
            engine_dw = create_engine(dw_url)
            with engine_gr.connect() as conn:
                gr_rows = conn.execute(text("SELECT COUNT(*) FROM grammy_awards;")).scalar_one()
            with engine_dw.connect() as conn:
                fact_rows = conn.execute(text("SELECT COUNT(*) FROM FACT_TRACK_GENRE;")).scalar_one()
                track_rows = conn.execute(text("SELECT COUNT(*) FROM DIM_TRACK;")).scalar_one()
                artist_rows = conn.execute(text("SELECT COUNT(*) FROM DIM_ARTIST;")).scalar_one()
                genre_rows = conn.execute(text("SELECT COUNT(*) FROM DIM_GENRE;")).scalar_one()
                quarantine_rows = conn.execute(text("SELECT COUNT(*) FROM quarantine_spotify;")).scalar_one()
            print(f"[STATUS] grammy_source.grammy_awards: {gr_rows}")
            print(f"[STATUS] dw.FACT_TRACK_GENRE: {fact_rows}")
            print(f"[STATUS] dw.DIM_TRACK: {track_rows}")
            print(f"[STATUS] dw.DIM_ARTIST: {artist_rows}")
            print(f"[STATUS] dw.DIM_GENRE: {genre_rows}")
            print(f"[STATUS] dw.quarantine_spotify: {quarantine_rows}")
            sys.exit(0)

    except ValueError as ve:
        print(f"[CRITICAL DATA QUALITY ERROR] {ve}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[OPERATIONAL ERROR] {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
