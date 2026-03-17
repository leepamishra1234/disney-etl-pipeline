import pandas as pd
import logging
import json
from datetime import datetime
from transformation.data_validation import validate_dataframe, clean_dataframe, print_validation_report
from ingestion.kafka_consumer import get_sample_events
from ingestion.api_ingestion import get_mock_api_response, api_response_to_dataframe
from ingestion.file_ingestion import generate_sample_csv, ingest_csv_file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = ["content_id", "title", "category", "department", "labor_hours", "region"]


def extract(source: str = "kafka") -> pd.DataFrame:
    """
    EXTRACT phase — pull raw data from configured source.
    Supports: 'kafka', 'api', 'file'
    """
    logger.info(f"=== EXTRACT: source={source} ===")

    if source == "kafka":
        raw = get_sample_events()
        df = pd.DataFrame(raw)

    elif source == "api":
        raw = get_mock_api_response()
        df = api_response_to_dataframe(raw)

    elif source == "file":
        path = "/tmp/sample_legacy.csv"
        generate_sample_csv(path)
        df = ingest_csv_file(path)

    else:
        raise ValueError(f"Unsupported source: {source}")

    logger.info(f"Extracted {len(df)} rows from '{source}'")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    TRANSFORM phase — validate, clean, and enrich the data.
    """
    logger.info("=== TRANSFORM ===")

    # Validate
    passed, report = validate_dataframe(df, REQUIRED_COLUMNS)
    print_validation_report(report)

    if not passed:
        raise ValueError(f"Validation failed. Missing columns: {report['missing_columns']}")

    # Clean
    df = clean_dataframe(df)

    # Enrich: classify labor intensity
    df["labor_flag"] = df["labor_hours"].apply(
        lambda x: "HIGH" if x > 100 else ("MEDIUM" if x > 50 else "LOW")
    )

    # Enrich: add pipeline metadata
    df["pipeline_layer"]     = "silver"
    df["processed_at"]       = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    df["pipeline_version"]   = "1.0.0"

    logger.info(f"Transformation complete. {len(df)} rows ready for load.")
    return df


def load(df: pd.DataFrame, output_path: str = "/tmp/disney_silver_output.csv"):
    """
    LOAD phase — write processed data to output (CSV / Data Lake / Warehouse).
    In production this writes to Azure Data Lake (Silver layer).
    """
    logger.info(f"=== LOAD: {output_path} ===")
    df.to_csv(output_path, index=False)
    logger.info(f"Loaded {len(df)} rows to {output_path}")
    return output_path


def generate_gold_layer(df: pd.DataFrame) -> dict:
    """
    Gold layer aggregations for BI/reporting.
    Returns department and category summaries.
    """
    dept_summary = df.groupby("department").agg(
        total_projects  = ("content_id",   "count"),
        total_hours     = ("labor_hours",  "sum"),
        avg_hours       = ("labor_hours",  "mean"),
        high_effort     = ("labor_flag",   lambda x: (x == "HIGH").sum())
    ).round(2).reset_index()

    category_summary = df.groupby("category").agg(
        total_titles    = ("content_id",  "count"),
        total_hours     = ("labor_hours", "sum"),
        avg_hours       = ("labor_hours", "mean")
    ).round(2).reset_index()

    return {
        "department_summary":  dept_summary,
        "category_summary":    category_summary
    }


def run_pipeline(source: str = "kafka") -> pd.DataFrame:
    """
    Full ETL pipeline run: Extract → Transform → Load → Gold
    """
    start = datetime.utcnow()
    logger.info("=" * 50)
    logger.info("   DISNEY CONTENT & LABOR ETL PIPELINE START")
    logger.info("=" * 50)

    # E → T → L
    raw_df    = extract(source)
    silver_df = transform(raw_df)
    load(silver_df)

    # Gold layer
    gold = generate_gold_layer(silver_df)

    end = datetime.utcnow()
    duration = (end - start).total_seconds()

    logger.info(f"Pipeline completed in {duration:.2f}s")
    logger.info("=" * 50)

    # Print results
    print("\n📊 SILVER LAYER (Processed Records):")
    print(silver_df[["content_id", "title", "category", "department",
                      "labor_hours", "labor_flag", "region"]].to_string(index=False))

    print("\n📊 GOLD LAYER — Department Summary:")
    print(gold["department_summary"].to_string(index=False))

    print("\n📊 GOLD LAYER — Category Summary:")
    print(gold["category_summary"].to_string(index=False))

    return silver_df


if __name__ == "__main__":
    run_pipeline(source="kafka")
