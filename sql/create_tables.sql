-- ============================================================
-- Disney Content & Labor Analytics — Table Definitions
-- Azure Synapse Analytics / SQL Data Warehouse
-- ============================================================

-- Bronze: raw ingested content events
CREATE TABLE bronze.content_events_raw (
    content_id      VARCHAR(50)     NOT NULL,
    title           NVARCHAR(255),
    category        VARCHAR(50),
    region          VARCHAR(20),
    department      VARCHAR(100),
    labor_hours     INT,
    event_type      VARCHAR(50),
    event_timestamp DATETIME2,
    source_system   VARCHAR(50),
    ingested_at     DATETIME2       DEFAULT GETUTCDATE()
);

-- Silver: cleaned and validated content data
CREATE TABLE silver.content_events_clean (
    content_id        VARCHAR(50)     NOT NULL PRIMARY KEY,
    title             NVARCHAR(255)   NOT NULL,
    category          VARCHAR(50),
    region            VARCHAR(20),
    department        VARCHAR(100)    NOT NULL,
    labor_hours       INT             CHECK (labor_hours > 0),
    labor_flag        VARCHAR(10)     CHECK (labor_flag IN ('HIGH', 'MEDIUM', 'LOW')),
    event_type        VARCHAR(50),
    event_timestamp   DATETIME2,
    pipeline_layer    VARCHAR(10)     DEFAULT 'silver',
    pipeline_version  VARCHAR(10),
    processed_at      DATETIME2       DEFAULT GETUTCDATE()
);

-- Silver: labor logs from API
CREATE TABLE silver.labor_logs (
    log_id          INT             IDENTITY(1,1) PRIMARY KEY,
    employee_id     VARCHAR(20)     NOT NULL,
    project_id      VARCHAR(50),
    department      VARCHAR(100),
    role            VARCHAR(100),
    hours_logged    FLOAT           CHECK (hours_logged >= 0),
    week            VARCHAR(10),
    processed_at    DATETIME2       DEFAULT GETUTCDATE()
);

-- Gold: department labor summary
CREATE TABLE gold.department_labor_summary (
    department          VARCHAR(100)    NOT NULL,
    region              VARCHAR(20),
    total_projects      INT,
    total_labor_hours   INT,
    avg_labor_hours     FLOAT,
    high_effort_count   INT,
    summary_date        DATE            DEFAULT CAST(GETUTCDATE() AS DATE)
);

-- Gold: content category summary
CREATE TABLE gold.category_summary (
    category            VARCHAR(50)     NOT NULL,
    total_titles        INT,
    total_labor_hours   INT,
    avg_labor_hours     FLOAT,
    summary_date        DATE            DEFAULT CAST(GETUTCDATE() AS DATE)
);
