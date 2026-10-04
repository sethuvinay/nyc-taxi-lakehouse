# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze ingestion
# MAGIC
# MAGIC Land the raw NYC yellow-taxi CSVs into ADLS Gen2 as a Delta table, **untouched**.
# MAGIC Bronze does almost nothing on purpose: it is the permanent, replayable copy of the
# MAGIC source. All cleaning is deferred to Silver, so if a cleaning rule ever turns out to
# MAGIC be wrong, we rebuild from Bronze instead of re-downloading.
# MAGIC
# MAGIC Run order: this notebook first, then `02_silver_cleaning`, then `03_gold_aggregates`.

# COMMAND ----------

# Parameters — set via Databricks widgets (or job parameters)
dbutils.widgets.text("storage_account", "", "ADLS Gen2 storage account name")
dbutils.widgets.text("raw_pattern", "yellow_tripdata_*.csv", "Raw file glob in bronze/raw/")

storage_account = dbutils.widgets.get("storage_account")
raw_pattern = dbutils.widgets.get("raw_pattern")
assert storage_account, "Set the 'storage_account' widget before running."

bronze_root = f"abfss://bronze@{storage_account}.dfs.core.windows.net"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

# Read the raw CSVs exactly as downloaded — no cleaning here.
raw = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{bronze_root}/raw/{raw_pattern}")
)

# Stamp provenance so every row can be traced back to its source file.
bronze = (
    raw.withColumn("ingested_at", current_timestamp())
    .withColumn("source_file", input_file_name())
)

# Append: Bronze keeps a growing history of every raw load.
bronze.write.format("delta").mode("append").save(f"{bronze_root}/trips_delta")

print(f"Bronze rows landed this run: {bronze.count():,}")
