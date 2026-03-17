from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, upper, lower, trim,
    when, lit, sum as spark_sum, count,
    avg, max as spark_max, min as spark_min
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType, FloatType
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Schema definition for incoming content events
CONTENT_SCHEMA = StructType([
    StructField("content_id",   StringType(),    False),
    StructField("title",        StringType(),    True),
    StructField("category",     StringType(),    True),
    StructField("region",       StringType(),    True),
    StructField("department",   StringType(),    True),
    StructField("labor_hours",  IntegerType(),   True),
    StructField("timestamp",    StringType(),    True),
    StructField("event_type",   StringType(),    True),
])


def create_spark_session(app_name: str = "DisneyETLPipeline") -> SparkSession:
    """Creates and returns a configured SparkSession."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession created: {app_name}")
    return spark


def transform_content_events(df: DataFrame) -> DataFrame:
    """
    Core PySpark transformation for Disney content events:
    - Parses timestamps
    - Standardizes text fields
    - Classifies labor intensity
    - Removes duplicates
    """
    logger.info("Starting content event transformation...")

    transformed = df \
        .withColumn("event_timestamp",   to_timestamp(col("timestamp"))) \
        .withColumn("category",          upper(trim(col("category")))) \
        .withColumn("region",            upper(trim(col("region")))) \
        .withColumn("department",        trim(col("department"))) \
        .withColumn("title",             trim(col("title"))) \
        .withColumn("labor_flag",
            when(col("labor_hours") > 100, lit("HIGH"))
            .when(col("labor_hours") > 50,  lit("MEDIUM"))
            .otherwise(lit("LOW"))
        ) \
        .withColumn("pipeline_layer", lit("silver")) \
        .drop("timestamp") \
        .dropDuplicates(["content_id"])

    logger.info(f"Transformation complete. Rows after dedup: {transformed.count()}")
    return transformed


def aggregate_labor_by_department(df: DataFrame) -> DataFrame:
    """
    Gold layer aggregation:
    Total and average labor hours per department and region.
    """
    logger.info("Aggregating labor hours by department...")

    agg_df = df.groupBy("department", "region") \
               .agg(
                   spark_sum("labor_hours").alias("total_labor_hours"),
                   avg("labor_hours").alias("avg_labor_hours"),
                   count("content_id").alias("total_projects"),
                   spark_max("labor_hours").alias("max_labor_hours")
               ) \
               .orderBy("total_labor_hours", ascending=False)

    return agg_df


def aggregate_by_category(df: DataFrame) -> DataFrame:
    """Gold layer: content count and labor by category."""
    return df.groupBy("category") \
             .agg(
                 count("content_id").alias("total_titles"),
                 spark_sum("labor_hours").alias("total_labor_hours"),
                 avg("labor_hours").alias("avg_labor_hours")
             ) \
             .orderBy("total_titles", ascending=False)


def write_to_silver_layer(df: DataFrame, output_path: str):
    """Writes transformed data to Silver layer (Delta/Parquet)."""
    df.write \
      .mode("overwrite") \
      .partitionBy("region") \
      .parquet(output_path)
    logger.info(f"Silver layer written to: {output_path}")


def write_to_gold_layer(df: DataFrame, output_path: str):
    """Writes aggregated data to Gold layer."""
    df.write \
      .mode("overwrite") \
      .parquet(output_path)
    logger.info(f"Gold layer written to: {output_path}")


if __name__ == "__main__":
    print("=== PySpark Transformations - Disney ETL ===")

    spark = create_spark_session()

    # Sample data
    sample_data = [
        ("DIS-001", "Encanto 2",           "Movie",       "US",   "Animation",      150, "2024-01-10T09:00:00Z", "content_published"),
        ("DIS-002", "Mandalorian S4",       "Series",      "EU",   "VFX",             85, "2024-02-15T10:00:00Z", "content_updated"),
        ("DIS-003", "Lion King Remastered", "Movie",       "US",   "Post-Production", 120, "2024-03-01T08:00:00Z", "content_published"),
        ("DIS-004", "NatGeo Oceans",        "Documentary", "APAC", "Editing",          60, "2024-03-20T11:00:00Z", "content_published"),
        ("DIS-005", "Thor New Era",         "Movie",       "US",   "Post-Production",  45, "2024-04-01T12:00:00Z", "content_updated"),
    ]

    df_raw = spark.createDataFrame(sample_data, schema=[
        "content_id", "title", "category", "region",
        "department", "labor_hours", "timestamp", "event_type"
    ])

    df_silver = transform_content_events(df_raw)
    print("\n--- Silver Layer (Transformed) ---")
    df_silver.show(truncate=False)

    df_gold = aggregate_labor_by_department(df_silver)
    print("\n--- Gold Layer (Department Aggregation) ---")
    df_gold.show(truncate=False)

    df_category = aggregate_by_category(df_silver)
    print("\n--- Gold Layer (Category Aggregation) ---")
    df_category.show(truncate=False)

    spark.stop()
