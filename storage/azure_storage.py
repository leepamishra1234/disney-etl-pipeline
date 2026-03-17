import pandas as pd
import logging
import os
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AzureDataLakeSimulator:
    """
    Simulates Azure Data Lake Storage (ADLS Gen2) operations.
    In production, replace with azure-storage-file-datalake SDK calls.

    Medallion Architecture:
        Bronze  → Raw ingested data (no transformations)
        Silver  → Cleaned and validated data
        Gold    → Aggregated, business-ready data
    """

    def __init__(self, base_path: str = "/tmp/disney_data_lake"):
        self.base_path = base_path
        self._init_layers()

    def _init_layers(self):
        for layer in ["bronze", "silver", "gold"]:
            path = os.path.join(self.base_path, layer)
            os.makedirs(path, exist_ok=True)
        logger.info(f"Data Lake initialized at: {self.base_path}")

    def write_bronze(self, df: pd.DataFrame, dataset_name: str) -> str:
        """Writes raw data to Bronze layer (no modifications)."""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename  = f"{dataset_name}_{timestamp}.csv"
        filepath  = os.path.join(self.base_path, "bronze", filename)

        df.to_csv(filepath, index=False)
        logger.info(f"Bronze write: {filepath} ({len(df)} rows)")
        return filepath

    def write_silver(self, df: pd.DataFrame, dataset_name: str) -> str:
        """Writes cleaned/transformed data to Silver layer."""
        filepath = os.path.join(self.base_path, "silver", f"{dataset_name}.csv")
        df.to_csv(filepath, index=False)
        logger.info(f"Silver write: {filepath} ({len(df)} rows)")
        return filepath

    def write_gold(self, df: pd.DataFrame, dataset_name: str) -> str:
        """Writes aggregated/business-ready data to Gold layer."""
        filepath = os.path.join(self.base_path, "gold", f"{dataset_name}.csv")
        df.to_csv(filepath, index=False)
        logger.info(f"Gold write: {filepath} ({len(df)} rows)")
        return filepath

    def read_layer(self, layer: str, filename: str) -> pd.DataFrame:
        """Reads a dataset from the specified layer."""
        filepath = os.path.join(self.base_path, layer, filename)
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found in {layer} layer: {filename}")
        df = pd.read_csv(filepath)
        logger.info(f"Read {layer}/{filename}: {len(df)} rows")
        return df

    def list_files(self, layer: str) -> list:
        """Lists all files in a given layer."""
        path = os.path.join(self.base_path, layer)
        files = os.listdir(path)
        logger.info(f"Files in {layer} layer: {files}")
        return files

    def get_storage_summary(self) -> dict:
        """Returns a summary of file counts per layer."""
        summary = {}
        for layer in ["bronze", "silver", "gold"]:
            files = self.list_files(layer)
            summary[layer] = {"file_count": len(files), "files": files}
        return summary


if __name__ == "__main__":
    lake = AzureDataLakeSimulator()

    # Simulate full medallion write
    raw_df = pd.DataFrame([
        {"content_id": "DIS-001", "title": "Encanto 2", "labor_hours": 150, "department": "Animation", "region": "US", "category": "Movie"},
        {"content_id": "DIS-002", "title": "Thor",      "labor_hours": 45,  "department": "VFX",        "region": "EU", "category": "Movie"},
    ])

    lake.write_bronze(raw_df, "content_events")

    clean_df = raw_df.copy()
    clean_df["labor_flag"] = clean_df["labor_hours"].apply(
        lambda x: "HIGH" if x > 100 else ("MEDIUM" if x > 50 else "LOW")
    )
    lake.write_silver(clean_df, "content_events_clean")

    gold_df = clean_df.groupby("department")["labor_hours"].sum().reset_index()
    gold_df.columns = ["department", "total_labor_hours"]
    lake.write_gold(gold_df, "dept_labor_summary")

    print("\n=== Storage Summary ===")
    summary = lake.get_storage_summary()
    for layer, info in summary.items():
        print(f"  {layer.upper()}: {info['file_count']} file(s) — {info['files']}")
