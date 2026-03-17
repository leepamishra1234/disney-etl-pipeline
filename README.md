# Disney Content & Labor Analytics — ETL Pipeline

An end-to-end data engineering pipeline that ingests Disney content metadata
and labor events from Kafka streams and REST APIs, transforms them using
PySpark on Databricks, and loads them into Azure Data Lake for analytics.

---

## Architecture

```
[ Kafka Topics ]  [ REST APIs ]  [ CSV / Legacy DB ]
        ↓               ↓                ↓
    ┌───────────────────────────────────────┐
    │           BRONZE LAYER (Raw)          │  ← Azure Data Lake
    └───────────────────────────────────────┘
                        ↓
    ┌───────────────────────────────────────┐
    │     SILVER LAYER (Cleaned/Validated)  │  ← PySpark on Databricks
    └───────────────────────────────────────┘
                        ↓
    ┌───────────────────────────────────────┐
    │      GOLD LAYER (Aggregated / BI)     │  ← Azure SQL / Synapse
    └───────────────────────────────────────┘
                        ↓
              [ Dashboards / Reports ]
```

---

## Tech Stack

| Layer          | Technology                          |
|----------------|-------------------------------------|
| Ingestion      | Apache Kafka, REST APIs, CSV files  |
| Processing     | PySpark, Python, Databricks         |
| Storage        | Azure Data Lake Gen2 (Bronze/Silver/Gold) |
| Warehousing    | Azure SQL / Synapse Analytics       |
| Orchestration  | Databricks Workflows                |
| Monitoring     | Azure Monitor, Python logging       |
| Testing        | Pytest                              |

---

## Project Structure

```
disney-etl-pipeline/
├── ingestion/
│   ├── kafka_consumer.py       # Kafka event consumer
│   ├── api_ingestion.py        # REST API data fetch
│   └── file_ingestion.py       # CSV / legacy file ingestion
├── transformation/
│   ├── spark_transform.py      # PySpark transformations & aggregations
│   └── data_validation.py      # Validation & cleaning framework
├── storage/
│   └── azure_storage.py        # Azure Data Lake (medallion architecture)
├── sql/
│   ├── create_tables.sql       # Table definitions (Bronze/Silver/Gold)
│   └── aggregation_queries.sql # Business reporting queries
├── monitoring/
│   └── pipeline_logger.py      # Pipeline monitoring & logging
├── tests/
│   └── test_pipeline.py        # Pytest unit tests
├── etl_pipeline.py             # Main ETL orchestrator (E→T→L)
├── requirements.txt
└── README.md
```

---

## Pipeline Flow

1. **Extract** — Ingest raw events from Kafka topics (content publish/update events) and REST API (labor scheduling data)
2. **Validate** — Check for required columns, nulls, duplicates, and business rule violations
3. **Transform** — Clean data using PySpark: parse timestamps, standardize fields, classify labor intensity (HIGH/MEDIUM/LOW)
4. **Load (Silver)** — Write cleaned records to Azure Data Lake Silver layer, partitioned by region
5. **Aggregate (Gold)** — Compute department-level and category-level labor summaries for BI reporting

---

## Key Features

- Real-time Kafka consumer for content event streaming
- REST API ingestion with pagination handling
- CSV ingestion for legacy system migration
- PySpark transformations with labor hour classification
- Data validation framework with detailed quality reports
- Medallion architecture: Bronze → Silver → Gold
- Modular, testable design with Pytest coverage

---

## How to Run

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full ETL pipeline
python etl_pipeline.py

# Run unit tests
pytest tests/test_pipeline.py -v
```

---

## Sample Output

```
📊 SILVER LAYER (Processed Records):
 content_id              title   category      department  labor_hours labor_flag region
   DIS-001           Encanto 2      MOVIE       Animation          150       HIGH     US
   DIS-002      Mandalorian S4     SERIES             VFX           85     MEDIUM     EU
   DIS-003  Lion King Remaster      MOVIE  Post-Production         120       HIGH     US

📊 GOLD LAYER — Department Summary:
      department  total_projects  total_hours  avg_hours  high_effort
       Animation               1          150      150.0            1
 Post-Production               1          120      120.0            1
             VFX               1           85       85.0            0
```

---

## Background

This project is inspired by enterprise-scale data workflows supporting content
operations and labor management in the media & entertainment industry. It
demonstrates real-world ETL patterns including streaming ingestion, data quality
validation, cloud storage integration, and aggregation for business reporting.

---

## Author

**Leepa Mishra**  
Data Engineer | Python · PySpark · Azure · Databricks · SQL  
[LinkedIn](https://linkedin.com/in/your-profile)  
[GitHub](https://github.com/your-username)
