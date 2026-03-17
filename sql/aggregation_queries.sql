-- ============================================================
-- Disney Content & Labor Analytics — Aggregation Queries
-- ============================================================

-- 1. Total and average labor hours per department
SELECT
    department,
    region,
    COUNT(content_id)           AS total_projects,
    SUM(labor_hours)            AS total_labor_hours,
    ROUND(AVG(labor_hours), 2)  AS avg_labor_hours,
    MAX(labor_hours)            AS max_labor_hours,
    SUM(CASE WHEN labor_flag = 'HIGH' THEN 1 ELSE 0 END) AS high_effort_projects
FROM silver.content_events_clean
GROUP BY department, region
ORDER BY total_labor_hours DESC;


-- 2. Content volume by category and region
SELECT
    category,
    region,
    COUNT(content_id)   AS total_titles,
    SUM(labor_hours)    AS total_labor_hours
FROM silver.content_events_clean
GROUP BY category, region
ORDER BY category, total_titles DESC;


-- 3. Labor flag distribution (HIGH / MEDIUM / LOW)
SELECT
    labor_flag,
    COUNT(*)                                AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM silver.content_events_clean
GROUP BY labor_flag
ORDER BY count DESC;


-- 4. Monthly pipeline throughput
SELECT
    FORMAT(processed_at, 'yyyy-MM')     AS month,
    COUNT(content_id)                   AS records_processed,
    SUM(labor_hours)                    AS total_hours
FROM silver.content_events_clean
GROUP BY FORMAT(processed_at, 'yyyy-MM')
ORDER BY month;


-- 5. Top 5 highest-effort projects
SELECT TOP 5
    content_id,
    title,
    department,
    labor_hours,
    labor_flag,
    region
FROM silver.content_events_clean
ORDER BY labor_hours DESC;


-- 6. Departments with above-average labor hours
SELECT
    department,
    ROUND(AVG(labor_hours), 2) AS avg_hours
FROM silver.content_events_clean
GROUP BY department
HAVING AVG(labor_hours) > (
    SELECT AVG(labor_hours) FROM silver.content_events_clean
)
ORDER BY avg_hours DESC;


-- 7. Join content events with labor logs
SELECT
    c.content_id,
    c.title,
    c.department,
    c.labor_hours         AS event_labor_hours,
    SUM(l.hours_logged)   AS actual_logged_hours,
    c.labor_hours - SUM(l.hours_logged) AS variance
FROM silver.content_events_clean c
LEFT JOIN silver.labor_logs l
    ON c.content_id = l.project_id
GROUP BY c.content_id, c.title, c.department, c.labor_hours
ORDER BY variance DESC;
