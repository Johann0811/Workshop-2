"""
Reliable Candidate Pipeline - Guided Programming Practice Part 2

Course: ETL - Data Engineering and Artificial Intelligence
Airflow: 3.1.8
Great Expectations: 1.23.x

Analytical requirement:
R2 - Analyze hiring outcomes according to technology.

The DAG exchanges FILE PATHS between tasks, not complete DataFrames.
All tasks read/write files under /opt/airflow/data, which must be mounted
as a shared Docker volume.
"""

from datetime import timedelta
from pathlib import Path
import shutil

import great_expectations as gx
import pandas as pd
import pendulum

from airflow.sdk import dag, task
from great_expectations.expectations.metadata_types import FailureSeverity


# ============================================================
# GLOBAL CONFIGURATION
# ============================================================

DATA_DIR = Path("/opt/airflow/data")
WORK_DIR = DATA_DIR / "work"
OUTPUT_DIR = DATA_DIR / "output"

# Successful run:
SOURCE_FILENAME = "candidates.csv"

# Controlled-failure run:
# After creating candidates_bad.csv, change the previous line to:
# SOURCE_FILENAME = "candidates_bad.csv"

SOURCE_PATH = DATA_DIR / SOURCE_FILENAME

RAW_PATH = WORK_DIR / "candidates_r2_raw.csv"
PREPARED_PATH = WORK_DIR / "candidates_r2_prepared.csv"
TARGET_PATH = OUTPUT_DIR / "candidates_r2_validated.csv"
SUMMARY_PATH = OUTPUT_DIR / "r2_hiring_by_technology.csv"

REQUIRED_COLUMNS = [
    "Technology",
    "Code Challenge Score",
    "Technical Interview Score",
]


# ============================================================
# HELPER: BUILD AND RUN A GX VALIDATION
# ============================================================

def run_gx_validation(dataframe, stage):
    """
    Build a small ephemeral GX validation for one pipeline stage.

    stage = "raw"
        Validates source attributes required by R2.

    stage = "prepared"
        Validates the transformed analytical dataset.

    The GX context is ephemeral because this guided practice focuses on
    orchestration. In a larger project, suites and validation definitions
    should normally be persisted and maintained as project assets.
    """

    context = gx.get_context(mode="ephemeral")

    data_source = context.data_sources.add_pandas(
        name=f"{stage}_candidate_source"
    )

    data_asset = data_source.add_dataframe_asset(
        name=f"{stage}_candidate_asset"
    )

    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        f"{stage}_candidate_batch"
    )

    suite = gx.ExpectationSuite(
        name=f"{stage}_candidate_suite"
    )
    suite = context.suites.add(suite)

    if stage == "raw":
        # DQ01 - Technology completeness.
        # Warning: a limited amount of missing Technology is tolerated.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="Technology",
                mostly=0.98,
                severity="warning",
            )
        )

        # DQ02 - Code Challenge Score validity.
        # Critical because HIRED depends directly on this attribute.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="Code Challenge Score",
                min_value=0,
                max_value=10,
                severity="critical",
            )
        )

        # DQ03 - Technical Interview Score validity.
        # Critical because HIRED depends directly on this attribute.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="Technical Interview Score",
                min_value=0,
                max_value=10,
                severity="critical",
            )
        )

    elif stage == "prepared":
        # Prepared data must contain Technology because R2 groups by it.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="Technology",
                severity="critical",
            )
        )

        # HIRED is a derived binary attribute.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="HIRED",
                value_set=[0, 1],
                severity="critical",
            )
        )

        # HIRED must be complete before the data are loaded.
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="HIRED",
                severity="critical",
            )
        )

    else:
        raise ValueError(f"Unknown validation stage: {stage}")

    validation_definition = gx.ValidationDefinition(
        name=f"{stage}_candidate_validation",
        data=batch_definition,
        suite=suite,
    )

    validation_definition = context.validation_definitions.add(
        validation_definition
    )

    result = validation_definition.run(
        batch_parameters={"dataframe": dataframe},
        result_format={"result_format": "SUMMARY"},
    )

    print(f"GX stage: {stage}")
    print(f"Validation success: {result.success}")
    print(f"Statistics: {result.statistics}")

    max_failure = result.get_max_severity_failure()
    print(f"Maximum failure severity: {max_failure}")

    return result, max_failure


# ============================================================
# DAG DEFINITION
# ============================================================

@dag(
    dag_id="reliable_candidate_pipeline_r2",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "gx", "reliable-pipeline", "r2"],
)
def reliable_candidate_pipeline_r2():

    # ========================================================
    # STAGE 1 - EXTRACT
    # ========================================================
    @task
    def extract_candidates():
        """
        Read the source CSV, verify the attributes required by R2,
        and persist a raw working file.

        The task returns only a FILE PATH. It does not send the full
        DataFrame through Airflow.
        """

        WORK_DIR.mkdir(parents=True, exist_ok=True)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        if not SOURCE_PATH.exists():
            raise FileNotFoundError(
                f"Source file not found: {SOURCE_PATH}"
            )

        df = pd.read_csv(SOURCE_PATH, sep=";")

        missing_columns = [
            column
            for column in REQUIRED_COLUMNS
            if column not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Required columns are missing: {missing_columns}"
            )

        raw_df = df[REQUIRED_COLUMNS].copy()
        raw_df.to_csv(RAW_PATH, index=False)

        print(f"Source: {SOURCE_PATH}")
        print(f"Rows extracted: {len(raw_df)}")
        print(f"Raw working file: {RAW_PATH}")

        return str(RAW_PATH)

    # ========================================================
    # STAGE 2 - RAW VALIDATION GATE
    # ========================================================
    @task
    def validate_raw_candidates(raw_path):
        """
        Validate the incoming data BEFORE transformation.

        Critical GX failures stop the pipeline.
        Warning-level failures are logged but do not block execution.
        """

        df = pd.read_csv(raw_path)

        result, max_failure = run_gx_validation(
            dataframe=df,
            stage="raw",
        )

        if max_failure == FailureSeverity.CRITICAL:
            raise ValueError(
                "Raw validation failed with CRITICAL severity. "
                "Transformation is blocked."
            )

        if max_failure == FailureSeverity.WARNING:
            print(
                "Raw validation contains WARNING-level failures. "
                "The documented policy allows the pipeline to continue."
            )

        print("Raw validation gate passed according to policy.")

    # ========================================================
    # STAGE 3 - TRANSFORM
    # ========================================================
    @task
    def transform_candidates(raw_path):
        """
        Apply documented transformation rules for R2.

        1. Normalize Technology text.
        2. Remove rows without Technology because they cannot support
           grouping by technology.
        3. Derive HIRED using the documented business rule:
              Code Challenge Score >= 7
              AND
              Technical Interview Score >= 7
        """

        df = pd.read_csv(raw_path)

        # Normalize Technology.
        df["Technology"] = (
            df["Technology"]
            .astype("string")
            .str.strip()
        )

        df["Technology"] = df["Technology"].replace("", pd.NA)

        missing_before = int(df["Technology"].isna().sum())

        # R2 cannot group a record by technology if Technology is missing.
        df = df.dropna(subset=["Technology"]).copy()

        # Derive the analytical attribute HIRED.
        df["HIRED"] = (
            (df["Code Challenge Score"] >= 7)
            & (df["Technical Interview Score"] >= 7)
        ).astype(int)

        df.to_csv(PREPARED_PATH, index=False)

        print(f"Rows with missing Technology removed: {missing_before}")
        print(f"Prepared rows: {len(df)}")
        print(f"Prepared working file: {PREPARED_PATH}")

        return str(PREPARED_PATH)

    # ========================================================
    # STAGE 4 - PREPARED VALIDATION GATE
    # ========================================================
    @task
    def validate_prepared_candidates(prepared_path):
        """
        Validate the result of transformation BEFORE loading.

        At this boundary the pipeline verifies that the dataset is
        analytically usable for R2.
        """

        df = pd.read_csv(prepared_path)

        result, max_failure = run_gx_validation(
            dataframe=df,
            stage="prepared",
        )

        if max_failure == FailureSeverity.CRITICAL:
            raise ValueError(
                "Prepared-data validation failed with CRITICAL severity. "
                "Loading is blocked."
            )

        print("Prepared validation gate passed.")

    # ========================================================
    # STAGE 5 - LOAD
    # ========================================================
    @task
    def load_candidates(prepared_path):
        """
        Persist the validated prepared dataset.

        shutil.copyfile overwrites the target, which makes this simple
        file-based guided example safe to rerun without appending duplicate
        records.
        """

        source = Path(prepared_path)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        shutil.copyfile(source, TARGET_PATH)

        loaded_df = pd.read_csv(TARGET_PATH)

        print(f"Validated dataset loaded to: {TARGET_PATH}")
        print(f"Rows loaded: {len(loaded_df)}")

        return str(TARGET_PATH)

    # ========================================================
    # STAGE 6 - ANALYTICAL OUTPUT FOR R2
    # ========================================================
    @task
    def build_r2_summary(target_path):
        """
        Answer R2 using the validated loaded data.

        Metrics:
        - Applications
        - Hired candidates
        - Hiring rate
        """

        df = pd.read_csv(target_path)

        summary = (
            df.groupby("Technology", as_index=False)
            .agg(
                Applications=("HIRED", "size"),
                Hired=("HIRED", "sum"),
                Hiring_Rate=("HIRED", "mean"),
            )
            .sort_values(
                by="Hiring_Rate",
                ascending=False,
            )
        )

        summary["Hiring_Rate"] = (
            summary["Hiring_Rate"] * 100
        ).round(2)

        summary.to_csv(SUMMARY_PATH, index=False)

        print("R2 - Hiring outcomes according to technology")
        print(summary.head(10).to_string(index=False))
        print(f"Analytical output: {SUMMARY_PATH}")

    # ========================================================
    # BUILD THE WORKFLOW
    # ========================================================

    raw_path = extract_candidates()

    raw_validation = validate_raw_candidates(raw_path)

    prepared_path = transform_candidates(raw_path)

    # Validation is a gate: transformation may only proceed after
    # raw validation satisfies the documented policy.
    raw_validation >> prepared_path

    prepared_validation = validate_prepared_candidates(
        prepared_path
    )

    target_path = load_candidates(prepared_path)

    # The target may only be loaded after prepared validation succeeds.
    prepared_validation >> target_path

    build_r2_summary(target_path)


# Make the DAG discoverable by Airflow.
reliable_candidate_pipeline_r2()

import pandas as pd

df_bad = pd.read_csv(
    "data/candidates.csv",
    sep=";",
)

# Inject one deterministic critical violation.
df_bad.loc[
    df_bad.index[0],
    "Code Challenge Score",
] = 15

df_bad.to_csv(
    "data/candidates_bad.csv",
    sep=";",
    index=False,
)

print("Controlled failure dataset created.")
