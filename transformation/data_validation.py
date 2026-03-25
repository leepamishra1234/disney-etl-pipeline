import pandas as pd
import logging
from typing import Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = ["content_id", "title", "category", "department", "labor_hours", "region"]


def validate_dataframe(df: pd.DataFrame, required_columns: list = REQUIRED_COLUMNS) -> Tuple[bool, dict]:
    """
    Validates incoming DataFrame before transformation.
    Returns (passed: bool, report: dict).
    """
    report = {
        "total_rows":        len(df),
        "null_counts":       {},
        "missing_columns":   [],
        "duplicate_count":   0,
        "invalid_labor":     0,
        "passed":            True,
        "warnings":          []
    }

    # 1. Check required columns
    for col in required_columns:
        if col not in df.columns:
            report["missing_columns"].append(col)
            report["passed"] = False
            logger.error(f"Missing required column: {col}")

    if not report["passed"]:
        return False, report

    # 2. Null checks
    report["null_counts"] = df[required_columns].isnull().sum().to_dict()
    critical_nulls = df[["content_id", "department"]].isnull().sum().sum()
    if critical_nulls > 0:
        report["passed"] = False
        logger.error(f"Critical null values found in content_id or department: {critical_nulls}")

    # 3. Duplicate check
    report["duplicate_count"] = int(df.duplicated(subset=["content_id"]).sum())
    if report["duplicate_count"] > 0:
        report["warnings"].append(f"{report['duplicate_count']} duplicate content_id(s) found")
        logger.warning(f"Duplicate rows: {report['duplicate_count']}")

    # 4. Business rule: labor_hours must be positive
    if "labor_hours" in df.columns:
        invalid = df[df["labor_hours"] <= 0]
        report["invalid_labor"] = len(invalid)
        if report["invalid_labor"] > 0:
            report["warnings"].append(f"{report['invalid_labor']} row(s) with invalid labor_hours (<= 0)")
            logger.warning(f"Invalid labor_hours: {report['invalid_labor']} rows")

    status = "PASSED" if report["passed"] else "FAILED"
    logger.info(f"Validation {status} — {len(df)} rows, {len(report['warnings'])} warnings")
    return report["passed"], report


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans a validated DataFrame:
    - Removes duplicates
    - Drops critical nulls
    - Strips whitespace
    - Normalizes column names
    """
    original_len = len(df)

    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = df.drop_duplicates(subset=["content_id"])
    df = df.dropna(subset=["content_id", "department"])

    # Strip string columns
    str_cols = df.select_dtypes(include="str").columns
    for col in str_cols:
        df[col] = df[col].str.strip()

    logger.info(f"Cleaned: {original_len} → {len(df)} rows ({original_len - len(df)} dropped)")
    return df.reset_index(drop=True)


def print_validation_report(report: dict):
    """Prints a formatted validation report to console."""
    print("\n" + "="*45)
    print("         DATA VALIDATION REPORT")
    print("="*45)
    print(f"  Total rows       : {report['total_rows']}")
    print(f"  Duplicates       : {report['duplicate_count']}")
    print(f"  Invalid labor    : {report['invalid_labor']}")
    print(f"  Missing columns  : {report['missing_columns'] or 'None'}")
    print(f"  Status           : {'✅ PASSED' if report['passed'] else '❌ FAILED'}")
    if report["warnings"]:
        print("\n  Warnings:")
        for w in report["warnings"]:
            print(f"    ⚠️  {w}")
    print("="*45 + "\n")


if __name__ == "__main__":
    sample = pd.DataFrame([
        {"content_id": "DIS-001", "title": "Encanto 2",     "category": "Movie",  "department": "Animation",      "labor_hours": 150, "region": "US"},
        {"content_id": "DIS-002", "title": "Mandalorian S4","category": "Series", "department": "VFX",             "labor_hours": 85,  "region": "EU"},
        {"content_id": "DIS-002", "title": "Mandalorian S4","category": "Series", "department": "VFX",             "labor_hours": 85,  "region": "EU"},  # duplicate
        {"content_id": "DIS-003", "title": "Thor",          "category": "Movie",  "department": "Post-Production", "labor_hours": -5,  "region": "US"},  # invalid hours
    ])

    passed, report = validate_dataframe(sample)
    print_validation_report(report)

    clean = clean_dataframe(sample)
    print("Cleaned DataFrame:")
    print(clean)
