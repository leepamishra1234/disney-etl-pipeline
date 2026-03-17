import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ingest_csv_file(filepath: str) -> pd.DataFrame:
    """
    Ingests legacy CSV data files (used during Disney system migration).
    Handles encoding issues and malformed rows gracefully.
    """
    try:
        df = pd.read_csv(filepath, encoding='utf-8', on_bad_lines='skip')
        logger.info(f"Ingested {len(df)} rows from {filepath}")
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        raise
    except Exception as e:
        logger.error(f"Failed to ingest file {filepath}: {e}")
        raise


def generate_sample_csv(output_path: str):
    """
    Generates a sample legacy CSV file to simulate migration data.
    """
    sample_data = {
        "content_id":   ["DIS-OLD-001", "DIS-OLD-002", "DIS-OLD-003", "DIS-OLD-004"],
        "title":        ["Aladdin", "Beauty and the Beast", "Mulan", "Hercules"],
        "category":     ["Movie", "Movie", "Movie", "Movie"],
        "department":   ["Animation", "Animation", "VFX", "Post-Production"],
        "labor_hours":  [300, 280, 190, 150],
        "region":       ["US", "EU", "APAC", "US"],
        "release_year": [1992, 1991, 1998, 1997],
        "status":       ["archived", "archived", "archived", "archived"]
    }
    df = pd.DataFrame(sample_data)
    df.to_csv(output_path, index=False)
    logger.info(f"Sample CSV written to {output_path}")
    return df


def ingest_all_files_in_folder(folder_path: str) -> pd.DataFrame:
    """
    Ingests all CSV files from a folder and concatenates them.
    Used for bulk legacy migration from multiple source files.
    """
    all_dfs = []
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            filepath = os.path.join(folder_path, filename)
            df = ingest_csv_file(filepath)
            df["source_file"] = filename
            all_dfs.append(df)

    if not all_dfs:
        logger.warning(f"No CSV files found in {folder_path}")
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Combined {len(all_dfs)} files → {len(combined)} total rows")
    return combined


if __name__ == "__main__":
    print("=== File Ingestion - Legacy Migration ===")
    sample_path = "/tmp/sample_legacy.csv"
    df = generate_sample_csv(sample_path)
    print(df)
