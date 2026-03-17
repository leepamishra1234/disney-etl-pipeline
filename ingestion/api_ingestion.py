import requests
import pandas as pd
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fetch_labor_data_from_api(base_url: str, endpoint: str, headers: dict = None) -> list:
    """
    Fetches labor scheduling data from Disney's internal REST API.
    Handles pagination and error responses.
    """
    url = f"{base_url}{endpoint}"
    all_records = []
    page = 1

    while True:
        try:
            response = requests.get(url, headers=headers, params={"page": page, "size": 100})
            response.raise_for_status()
            data = response.json()

            records = data.get("results", [])
            if not records:
                break

            all_records.extend(records)
            logger.info(f"Fetched page {page} — {len(records)} records")

            if not data.get("next"):
                break
            page += 1

        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            break

    logger.info(f"Total records fetched from API: {len(all_records)}")
    return all_records


def get_mock_api_response() -> list:
    """
    Returns mock API labor data for local testing.
    Simulates Disney labor/scheduling system API.
    """
    return [
        {"employee_id": "EMP001", "department": "Animation",
         "project_id": "DIS-2024-003", "hours_logged": 40,
         "week": "2024-W10", "role": "Animator"},
        {"employee_id": "EMP002", "department": "VFX",
         "project_id": "DIS-2024-002", "hours_logged": 35,
         "week": "2024-W10", "role": "VFX Artist"},
        {"employee_id": "EMP003", "department": "Post-Production",
         "project_id": "DIS-2024-001", "hours_logged": 45,
         "week": "2024-W11", "role": "Editor"},
        {"employee_id": "EMP004", "department": "Editing",
         "project_id": "DIS-2024-004", "hours_logged": 30,
         "week": "2024-W11", "role": "Video Editor"},
        {"employee_id": "EMP005", "department": "Animation",
         "project_id": "DIS-2024-003", "hours_logged": 50,
         "week": "2024-W12", "role": "Lead Animator"},
    ]


def api_response_to_dataframe(records: list) -> pd.DataFrame:
    """Converts raw API records list to a clean Pandas DataFrame."""
    df = pd.DataFrame(records)
    logger.info(f"Converted API response to DataFrame: {df.shape}")
    return df


if __name__ == "__main__":
    print("=== API Ingestion - Mock Labor Data ===")
    records = get_mock_api_response()
    df = api_response_to_dataframe(records)
    print(df)
