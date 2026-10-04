"""Run the lakehouse pipeline locally against the generated sample data.

Requires: pip install -r requirements.txt (pyspark). No Azure needed —
Delta writes are swapped for local Parquet; the transformation logic is the
identical code the Databricks notebooks use (src/transforms.py).

Usage:
    python local/generate_sample.py
    python local/run_local.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pyspark.sql import SparkSession

from transforms import (
    apply_silver_rules,
    build_busiest_zones,
    build_fare_by_hour,
    build_tip_by_payment,
    run_dq_checks,
)

HERE = os.path.dirname(__file__)
SAMPLE = os.path.join(HERE, "data", "yellow_sample.csv")
OUT = os.path.join(HERE, "output")


def main():
    if not os.path.exists(SAMPLE):
        sys.exit("Sample data not found. Run: python local/generate_sample.py")

    spark = (
        SparkSession.builder.appName("nyc-taxi-lakehouse-local")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Bronze (local stand-in): read raw CSV, stamp provenance.
    from pyspark.sql.functions import current_timestamp, lit

    bronze = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(SAMPLE)
        .withColumn("ingested_at", current_timestamp())
        .withColumn("source_file", lit("yellow_sample.csv"))
    )
    print(f"Bronze rows in: {bronze.count()}")

    # Silver: same rules + DQ checks as the Databricks notebook.
    silver = apply_silver_rules(bronze)
    report = run_dq_checks(silver)
    print(f"Silver rows out: {silver.count()}  (DQ passed: {report['total_rows']} rows)")

    os.makedirs(OUT, exist_ok=True)
    silver.write.mode("overwrite").parquet(os.path.join(OUT, "silver"))

    # Gold: same aggregates as the Databricks notebook.
    fare_by_hour = build_fare_by_hour(silver)
    busiest_zones = build_busiest_zones(silver)
    tip_by_payment = build_tip_by_payment(silver)

    fare_by_hour.write.mode("overwrite").parquet(os.path.join(OUT, "fare_by_hour"))
    busiest_zones.write.mode("overwrite").parquet(os.path.join(OUT, "busiest_zones"))
    tip_by_payment.write.mode("overwrite").parquet(os.path.join(OUT, "tip_by_payment"))

    print("\nGold — avg fare by hour (first 8):")
    fare_by_hour.show(8)
    print("Gold — busiest pickup zones (top 5):")
    busiest_zones.show(5)
    print("Gold — tip % by payment type:")
    tip_by_payment.show()

    spark.stop()
    print(f"\nLocal run complete. Tables in {OUT}/")


if __name__ == "__main__":
    main()
