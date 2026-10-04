# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold aggregates
# MAGIC
# MAGIC Read Silver and build small, business-ready aggregate tables for Power BI.
# MAGIC Gold trades raw detail for answers: each table is tiny and fast, maps to one
# MAGIC dashboard visual, and is safe to share (no raw or sensitive columns).
# MAGIC Keeping aggregation in Gold — not Silver — means the dashboard never
# MAGIC re-computes millions of rows on every refresh.

# COMMAND ----------

dbutils.widgets.text("storage_account", "", "ADLS Gen2 storage account name")
storage_account = dbutils.widgets.get("storage_account")
assert storage_account, "Set the 'storage_account' widget before running."

silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/trips_clean"
gold_base = f"abfss://gold@{storage_account}.dfs.core.windows.net"

# COMMAND ----------

import sys

sys.path.insert(0, "/Workspace/Repos/<your-username>/nyc-taxi-lakehouse/src")

from transforms import build_busiest_zones, build_fare_by_hour, build_tip_by_payment

# COMMAND ----------

silver = spark.read.format("delta").load(silver_path)
print(f"Silver rows in: {silver.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Average fare by hour of day — reveals morning/evening demand peaks

# COMMAND ----------

fare_by_hour = build_fare_by_hour(silver)
fare_by_hour.write.format("delta").mode("overwrite").save(f"{gold_base}/fare_by_hour")
fare_by_hour.show(24)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Busiest pickup zones — where demand concentrates

# COMMAND ----------

busiest_zones = build_busiest_zones(silver, limit=20)
busiest_zones.write.format("delta").mode("overwrite").save(f"{gold_base}/busiest_zones")
busiest_zones.show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tip percentage by payment type — card vs cash behaviour

# COMMAND ----------

tip_by_payment = build_tip_by_payment(silver)
tip_by_payment.write.format("delta").mode("overwrite").save(f"{gold_base}/tip_by_payment")
tip_by_payment.show()

print("Gold tables written: fare_by_hour, busiest_zones, tip_by_payment")
