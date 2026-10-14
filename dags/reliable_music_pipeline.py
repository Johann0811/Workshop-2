"""
Reliable Batch Music Pipeline DAG for Workshop 2.

Course: ETL - Data Engineering and Artificial Intelligence
Orchestrator: Apache Airflow 3.1.8 (TaskFlow API)
Quality Validation: Great Expectations 1.23.1
Databases: MySQL 8.0 (grammy_source, dw)

Architecture:
  extract_spotify  --> validate_spotify_raw  --\\
                                                +--> transform_and_integrate --> validate_prepared --> load_dw
  extract_grammys  --> validate_grammys_raw  --/

Task Interfaces:
  Tasks pass file paths or compact metadata references via XCom.
  No full DataFrames are passed through Airflow metadata.

Failure & Retry Policy:
  - extract_grammys: retries=2, delay=10s (transient database network connection)
  - load_dw: retries=2, delay=10s (transient database network connection)
  - All validation gates and transformation tasks: retries=0 (deterministic contract failures)
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict

import pendulum
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.sdk import dag, task

import sys
# Ensure /opt/airflow is on sys.path for importing src modules
for p in ("/opt/airflow", str(Path(__file__).resolve().parent.parent)):
    if p not in sys.path:
        sys.path.insert(0, p)

# Reusable pipeline modules from src/
from src.extract import extract_grammys, extract_spotify
from src.load import load_dw
from src.transform import transform_and_integrate
from src.validation import validate_grammy_raw, validate_prepared, validate_spotify_raw

logger = logging.getLogger("airflow.task")

# =====================================================================
# GLOBAL CONFIGURATION
# =====================================================================

DATA_DIR = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))
WORK_DIR = Path(os.getenv("WORK_DIR", "/opt/airflow/data/work"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "/opt/airflow/data/output"))
VAL_DIR = DATA_DIR / "validation_results"

DEFAULT_SPOTIFY_PATH = "/opt/airflow/data/raw/spotify_dataset.csv"

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
GRAMMY_USER = os.getenv("GRAMMY_DB_USER", "grammy_user")
GRAMMY_PASS = os.getenv("GRAMMY_DB_PASSWORD", "grammy_pass")
GRAMMY_DB = os.getenv("GRAMMY_DB_NAME", "grammy_source")

DW_USER = os.getenv("DW_DB_USER", "etl_user")
DW_PASS = os.getenv("DW_DB_PASSWORD", "etl_pass")
DW_DB = os.getenv("DW_DB_NAME", "dw")

GRAMMY_URL = f"mysql+pymysql://{GRAMMY_USER}:{GRAMMY_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{GRAMMY_DB}"
DW_URL = f"mysql+pymysql://{DW_USER}:{DW_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{DW_DB}"


# =====================================================================
# Batch-ID Helper
# =====================================================================

def _make_batch_id(run_id: str) -> str:
    """
    Produces a stable, filesystem-safe batch ID (<= 40 chars, no trailing
    punctuation) from an Airflow run_id.  A short hash suffix is appended
    to keep IDs unique even for runs triggered within the same second.

    Examples:
      manual__2026-10-13T20_51_59.276822+00:00 → batch_manual_2026101320515_a3f2c1
    """
    import hashlib
    # Take a 6-char hex hash from the full run_id for uniqueness
    short_hash = hashlib.md5(run_id.encode()).hexdigest()[:6]  # noqa: S324 – not a security hash
    # Strip non-alphanumeric/underscore chars from the prefix
    import re as _re
    safe = _re.sub(r"[^\w]", "_", run_id)
    # Collapse repeated underscores, strip leading/trailing underscores
    safe = _re.sub(r"_+", "_", safe).strip("_")
    # Truncate prefix so total length (len('batch_') [6] + prefix + '_' [1] + hash [6]) <= 40
    # max_prefix = 40 - 6 - 1 - 6 = 27
    max_prefix = 27
    prefix = safe[:max_prefix].rstrip("_")
    batch_id = f"batch_{prefix}_{short_hash}"
    return batch_id[:40]


# =====================================================================
# DAG DEFINITION
# =====================================================================

@dag(
    dag_id="reliable_music_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "workshop-2", "mysql", "great-expectations", "taskflow"],
    params={
        "spotify_path": Param(
            DEFAULT_SPOTIFY_PATH,
            type="string",
            description="Path to the Spotify CSV dataset (use data/test/spotify_popularity_negativa.csv for Test B).",
        ),
    },
)
def reliable_music_pipeline():

    # -----------------------------------------------------------------
    # 1. Spotify Branch: Extract
    # -----------------------------------------------------------------
    @task(task_id="extract_spotify", retries=0)
    def task_extract_spotify(**context) -> str:
        params = context.get("params", {})
        sp_path = params.get("spotify_path", DEFAULT_SPOTIFY_PATH)
        run_id = context.get("run_id", "manual_run")
        batch_id = _make_batch_id(run_id)

        logger.info(f"[EXTRACT_SPOTIFY] Starting extraction from {sp_path} for batch {batch_id}")
        raw_path = extract_spotify(
            source_path=sp_path,
            work_dir=str(WORK_DIR),
            batch_id=batch_id,
        )
        logger.info(f"[EXTRACT_SPOTIFY] Raw Spotify persisted to {raw_path}")
        return raw_path

    # -----------------------------------------------------------------
    # 2. Spotify Branch: Raw Validation Gate (DQ01 - DQ05)
    # -----------------------------------------------------------------
    @task(task_id="validate_spotify_raw", retries=0)
    def task_validate_spotify_raw(raw_path: str, **context) -> str:
        run_id = context.get("run_id", "manual_run")
        batch_id = _make_batch_id(run_id)

        logger.info(f"[VALIDATE_SPOTIFY_RAW] Validating {raw_path} at raw gate...")
        summary = validate_spotify_raw(
            spotify_path=raw_path,
            output_dir=str(VAL_DIR),
            batch_id=batch_id,
        )
        logger.info(f"[VALIDATE_SPOTIFY_RAW] Gate passed per policy. Statistics: {summary['statistics']}")
        return raw_path

    # -----------------------------------------------------------------
    # 3. Grammy Branch: Extract (Bounded retries for transient DB connection)
    # -----------------------------------------------------------------
    @task(task_id="extract_grammys", retries=2, retry_delay=timedelta(seconds=10))
    def task_extract_grammys(**context) -> str:
        from sqlalchemy.exc import OperationalError

        run_id = context.get("run_id", "manual_run")
        batch_id = _make_batch_id(run_id)

        logger.info(f"[EXTRACT_GRAMMYS] Extracting Grammy from MySQL source {GRAMMY_URL}...")
        try:
            raw_path = extract_grammys(
                mysql_url=GRAMMY_URL,
                work_dir=str(WORK_DIR),
                batch_id=batch_id,
            )
        except OperationalError as exc:
            # Transient connection failure — allow Airflow to retry
            logger.warning(f"[EXTRACT_GRAMMYS] Transient DB error (will retry): {exc}")
            raise
        except Exception as exc:
            # Deterministic failure (missing table, bad config, etc.) — skip retries
            logger.error(f"[EXTRACT_GRAMMYS] Deterministic failure (no retry): {exc}")
            raise AirflowFailException(f"Grammy extraction failed with non-transient error: {exc}") from exc
        logger.info(f"[EXTRACT_GRAMMYS] Raw Grammy persisted to {raw_path}")
        return raw_path

    # -----------------------------------------------------------------
    # 4. Grammy Branch: Raw Validation Gate (DQ01, DQ06, DQ07, DQ08)
    # -----------------------------------------------------------------
    @task(task_id="validate_grammys_raw", retries=0)
    def task_validate_grammys_raw(raw_path: str, **context) -> str:
        run_id = context.get("run_id", "manual_run")
        batch_id = _make_batch_id(run_id)

        logger.info(f"[VALIDATE_GRAMMYS_RAW] Validating {raw_path} at raw gate...")
        summary = validate_grammy_raw(
            grammy_path=raw_path,
            mysql_url=GRAMMY_URL,
            output_dir=str(VAL_DIR),
            batch_id=batch_id,
        )
        logger.info(f"[VALIDATE_GRAMMYS_RAW] Gate passed. Statistics: {summary['statistics']}")
        return raw_path

    # -----------------------------------------------------------------
    # 5. Convergence: Transform & Integrate
    # -----------------------------------------------------------------
    @task(task_id="transform_and_integrate", retries=0)
    def task_transform_and_integrate(spotify_raw_path: str, grammy_raw_path: str, **context) -> Dict[str, Any]:
        run_id = context.get("run_id", "manual_run")
        batch_id = _make_batch_id(run_id)

        logger.info(f"[TRANSFORM] Starting transform and integration for batch {batch_id}...")
        meta = transform_and_integrate(
            spotify_raw_path=spotify_raw_path,
            grammy_raw_path=grammy_raw_path,
            output_dir=str(WORK_DIR),
            batch_id=batch_id,
        )
        logger.info(
            f"[TRANSFORM] Completed. Fact rows: {meta['fact_rows_count']}, "
            f"Artists: {meta['dim_artist_count']}, Tracks: {meta['dim_track_count']}"
        )
        return meta

    # -----------------------------------------------------------------
    # 6. Prepared Validation Gate (DQ09 - DQ15)
    # -----------------------------------------------------------------
    @task(task_id="validate_prepared", retries=0)
    def task_validate_prepared(meta: Dict[str, Any], **context) -> Dict[str, Any]:
        run_id = context.get("run_id", "manual_run")
        batch_id = _make_batch_id(run_id)

        logger.info(f"[VALIDATE_PREPARED] Running prepared validation gate for batch {batch_id}...")
        validate_prepared(
            prepared_metadata=meta,
            output_dir=str(VAL_DIR),
            batch_id=batch_id,
        )
        logger.info("[VALIDATE_PREPARED] Prepared validation passed. DW load authorized.")
        return meta

    # -----------------------------------------------------------------
    # 7. Final Load: Data Warehouse (Bounded retries for DB connection)
    # -----------------------------------------------------------------
    @task(task_id="load_dw", retries=2, retry_delay=timedelta(seconds=10))
    def task_load_dw(meta: Dict[str, Any], **context) -> Dict[str, Any]:
        from sqlalchemy.exc import OperationalError

        run_id = context.get("run_id", "manual_run")
        batch_id = _make_batch_id(run_id)

        logger.info(f"[LOAD_DW] Loading batch {batch_id} into MySQL Data Warehouse...")
        try:
            result = load_dw(
                prepared_metadata=meta,
                mysql_url=DW_URL,
            )
        except OperationalError as exc:
            # Transient connection failure — allow Airflow to retry
            logger.warning(f"[LOAD_DW] Transient DB error (will retry): {exc}")
            raise
        except Exception as exc:
            # Deterministic failure (constraint violation, missing parquet, etc.) — skip retries
            logger.error(f"[LOAD_DW] Deterministic failure (no retry): {exc}")
            raise AirflowFailException(f"DW load failed with non-transient error: {exc}") from exc
        logger.info(f"[LOAD_DW] Successfully loaded {result['fact_rows_loaded']} fact rows into DW.")
        return result

    # Wiring task dependencies
    sp_extracted = task_extract_spotify()
    sp_validated = task_validate_spotify_raw(sp_extracted)

    gr_extracted = task_extract_grammys()
    gr_validated = task_validate_grammys_raw(gr_extracted)

    transformed_meta = task_transform_and_integrate(
        spotify_raw_path=sp_validated,
        grammy_raw_path=gr_validated,
    )

    prepared_validated = task_validate_prepared(transformed_meta)
    load_result = task_load_dw(prepared_validated)


# Instantiate DAG
reliable_music_pipeline()
