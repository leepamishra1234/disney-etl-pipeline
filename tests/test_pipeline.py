import pytest
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from transformation.data_validation import validate_dataframe, clean_dataframe
from etl_pipeline import transform, generate_gold_layer


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def valid_df():
    return pd.DataFrame([
        {"content_id": "DIS-001", "title": "Encanto 2",      "category": "Movie",  "department": "Animation",      "labor_hours": 150, "region": "US"},
        {"content_id": "DIS-002", "title": "Mandalorian S4", "category": "Series", "department": "VFX",             "labor_hours": 85,  "region": "EU"},
        {"content_id": "DIS-003", "title": "Thor",           "category": "Movie",  "department": "Post-Production", "labor_hours": 45,  "region": "US"},
    ])

@pytest.fixture
def df_with_issues():
    return pd.DataFrame([
        {"content_id": "DIS-001", "title": "Encanto 2", "category": "Movie", "department": "Animation",      "labor_hours": 150, "region": "US"},
        {"content_id": "DIS-001", "title": "Encanto 2", "category": "Movie", "department": "Animation",      "labor_hours": 150, "region": "US"},  # duplicate
        {"content_id": "DIS-002", "title": "Thor",      "category": "Movie", "department": "Post-Production", "labor_hours": -5,  "region": "US"},  # invalid hours
    ])


# ── Validation Tests ───────────────────────────────────────────────────────────

class TestValidation:
    def test_valid_df_passes(self, valid_df):
        passed, report = validate_dataframe(valid_df)
        assert passed is True
        assert report["missing_columns"] == []

    def test_missing_column_fails(self, valid_df):
        df = valid_df.drop(columns=["department"])
        passed, report = validate_dataframe(df)
        assert passed is False
        assert "department" in report["missing_columns"]

    def test_duplicate_detected(self, df_with_issues):
        _, report = validate_dataframe(df_with_issues)
        assert report["duplicate_count"] >= 1

    def test_invalid_labor_hours_flagged(self, df_with_issues):
        _, report = validate_dataframe(df_with_issues)
        assert report["invalid_labor"] >= 1

    def test_empty_df_fails(self):
        df = pd.DataFrame(columns=["content_id", "title", "category", "department", "labor_hours", "region"])
        passed, report = validate_dataframe(df)
        assert report["total_rows"] == 0


# ── Cleaning Tests ─────────────────────────────────────────────────────────────

class TestCleaning:
    def test_duplicates_removed(self, df_with_issues):
        cleaned = clean_dataframe(df_with_issues)
        assert cleaned["content_id"].duplicated().sum() == 0

    def test_column_names_normalized(self, valid_df):
        df = valid_df.copy()
        df.columns = ["Content ID", "Title", "Category", "Department", "Labor Hours", "Region"]
        cleaned = clean_dataframe(df)
        assert "content_id" in cleaned.columns
        assert "labor_hours" in cleaned.columns

    def test_whitespace_stripped(self, valid_df):
        df = valid_df.copy()
        df.loc[0, "title"] = "  Encanto 2  "
        cleaned = clean_dataframe(df)
        assert cleaned.loc[0, "title"] == "Encanto 2"


# ── Transform Tests ────────────────────────────────────────────────────────────

class TestTransform:
    def test_labor_flag_high(self, valid_df):
        result = transform(valid_df)
        high = result[result["labor_hours"] > 100]
        assert (high["labor_flag"] == "HIGH").all()

    def test_labor_flag_medium(self, valid_df):
        result = transform(valid_df)
        medium = result[(result["labor_hours"] > 50) & (result["labor_hours"] <= 100)]
        assert (medium["labor_flag"] == "MEDIUM").all()

    def test_labor_flag_low(self, valid_df):
        result = transform(valid_df)
        low = result[result["labor_hours"] <= 50]
        assert (low["labor_flag"] == "LOW").all()

    def test_pipeline_metadata_added(self, valid_df):
        result = transform(valid_df)
        assert "pipeline_layer" in result.columns
        assert "processed_at" in result.columns
        assert result["pipeline_layer"].iloc[0] == "silver"

    def test_row_count_preserved(self, valid_df):
        result = transform(valid_df)
        assert len(result) == len(valid_df)


# ── Gold Layer Tests ───────────────────────────────────────────────────────────

class TestGoldLayer:
    def test_department_summary_has_correct_columns(self, valid_df):
        silver = transform(valid_df)
        gold = generate_gold_layer(silver)
        dept = gold["department_summary"]
        assert "department" in dept.columns
        assert "total_hours" in dept.columns
        assert "total_projects" in dept.columns

    def test_category_summary_totals_correct(self, valid_df):
        silver = transform(valid_df)
        gold = generate_gold_layer(silver)
        total = gold["category_summary"]["total_titles"].sum()
        assert total == len(valid_df)
