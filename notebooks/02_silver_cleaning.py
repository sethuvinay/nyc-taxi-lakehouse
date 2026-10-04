# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver cleaning & validation
# MAGIC
# MAGIC Read Bronze, apply the cleaning rules, run **fail-loud data-quality checks**,
# MAGIC and write one clean Delta table. Silver is where trust is earned: every rule
# MAGIC below exists because the real NYC TLC data actually contains that defect
# MAGIC (drop-offs before pickups, zero-distance trips, negative fares, duplicates).
# MAGIC
# MAGIC Overwrite is safe here — Silver is fully rebuilt from Bronze each run.

# COMMAND ----------

dbutils.widgets.text("storage_account", "", "ADLS Gen2 storage account name")
storage_account = dbutils.widgets.get("storage_account")
assert storage_account, "Set the 'storage_account' widget before running."

bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/trips_delta"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/trips_clean"

# COMMAND ----------

import sys

# Shared transformation logic lives in src/transforms.py (same code the local
# test runner uses). Adjust this path to where the repo is checked out in your
# Databricks workspace (Databricks Repos).
sys.path.insert(0, "/Workspace/Repos/<your-username>/nyc-taxi-lakehouse/src")

from transforms import apply_silver_rules, run_dq_checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean

# COMMAND ----------

bronze = spark.read.format("delta").load(bronze_path)
print(f"Bronze rows in: {bronze.count():,}")

silver = apply_silver_rules(bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate — fail loudly, never silently ship bad data

# COMMAND ----------

report = run_dq_checks(silver)
print(f"Silver rows out: {silver.count():,}")
print("DQ report:")
for column, nulls in report["null_counts"].items():
    print(f"  nulls in {column}: {nulls} ({report['null_pct'][column]}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write

# COMMAND ----------

silver.write.format("delta").mode("overwrite").save(silver_path)
print(f"Silver Delta table written to {silver_path}")
