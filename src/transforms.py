"""Shared PySpark transformations for the NYC taxi lakehouse.

These are pure DataFrame in/out functions — no dbutils, no storage I/O —
so the exact same logic runs in the Databricks notebooks and in the local
test runner (see ``local/run_local.py``).
"""
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# Columns that must never be null in a valid Silver row.
KEY_COLUMNS = ["pickup_ts", "dropoff_ts", "PULocationID", "fare_amount"]


def parse_timestamps(df: DataFrame) -> DataFrame:
    """Parse the raw TLC datetime strings into proper timestamp columns."""
    return (
        df.withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime"))
        .withColumn("dropoff_ts", F.to_timestamp("tpep_dropoff_datetime"))
    )


def apply_silver_rules(df: DataFrame) -> DataFrame:
    """Clean Bronze rows into Silver: one clean row out per valid row in.

    Rules (each exists because the real NYC TLC data contains that defect):
    - drop-off must not precede pickup (timestamp ordering)
    - trip distance, fare amount and passenger count must be positive
      (catches zero-distance trips and negative fares)
    - key columns must not be null
    - exact duplicate trips are removed on a natural key
    - derived columns: pickup hour of day, trip duration in minutes
    """
    df = parse_timestamps(df)
    return (
        df.filter(F.col("dropoff_ts") >= F.col("pickup_ts"))
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("passenger_count") > 0)
        .dropna(subset=KEY_COLUMNS)
        .dropDuplicates(["pickup_ts", "dropoff_ts", "PULocationID", "fare_amount"])
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn(
            "trip_minutes",
            (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60,
        )
    )


def run_dq_checks(df: DataFrame) -> dict:
    """Fail-loud data-quality checks over the Silver DataFrame.

    Returns a report dict. Raises AssertionError on the first violation so
    bad data never flows silently into Gold or the dashboard.
    """
    total = df.count()
    assert total > 0, "DQ FAILED: Silver DataFrame is empty after cleaning"

    report = {"total_rows": total, "null_counts": {}, "null_pct": {}}
    for col in KEY_COLUMNS:
        n = df.filter(F.col(col).isNull()).count()
        report["null_counts"][col] = n
        report["null_pct"][col] = round(n / total * 100, 2)
        assert n == 0, f"DQ FAILED: {n} nulls in key column '{col}'"

    bad_ts = df.filter(F.col("dropoff_ts") < F.col("pickup_ts")).count()
    assert bad_ts == 0, f"DQ FAILED: {bad_ts} rows with dropoff before pickup"

    bad_fare = df.filter(F.col("fare_amount") <= 0).count()
    assert bad_fare == 0, f"DQ FAILED: {bad_fare} rows with non-positive fare"

    bad_dist = df.filter(F.col("trip_distance") <= 0).count()
    assert bad_dist == 0, f"DQ FAILED: {bad_dist} rows with non-positive distance"

    return report


def build_fare_by_hour(df: DataFrame) -> DataFrame:
    """Gold: average fare and trip count per pickup hour of day."""
    return (
        df.groupBy("pickup_hour")
        .agg(
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.count("*").alias("trip_count"),
        )
        .orderBy("pickup_hour")
    )


def build_busiest_zones(df: DataFrame, limit: int = 20) -> DataFrame:
    """Gold: top pickup zones by trip volume, with average fare."""
    return (
        df.groupBy("PULocationID")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        )
        .orderBy(F.desc("trip_count"))
        .limit(limit)
    )


def build_tip_by_payment(df: DataFrame) -> DataFrame:
    """Gold: average tip percentage by payment type."""
    return df.groupBy("payment_type").agg(
        F.round(F.avg(F.col("tip_amount") / F.col("fare_amount")) * 100, 2).alias(
            "avg_tip_pct"
        ),
        F.count("*").alias("trip_count"),
    )
