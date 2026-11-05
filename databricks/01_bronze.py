# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bronze: raw CSV -> Delta (Auto Loader)
# MAGIC Source: /Volumes/workspace/insurance/raw/{customers,policies,claims}
# MAGIC All columns stay strings in bronze. Typing and cleaning happen in silver.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG, SCHEMA = "workspace", "insurance"
BASE = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
CHK = f"{BASE}/_checkpoints"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

def ingest(name):
    """Incrementally load new CSV files of one folder into bronze_<name>."""
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")   # everything as string
        .option("cloudFiles.schemaLocation", f"{CHK}/{name}_schema")
        .load(f"{BASE}/{name}/")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    (
        stream.writeStream
        .option("checkpointLocation", f"{CHK}/{name}_bronze")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.{SCHEMA}.bronze_{name}")
        .awaitTermination()
    )

for n in ["customers", "policies", "claims"]:
    ingest(n)

# COMMAND ----------

# Expected after claims_batch_1.csv: customers 500, policies 800, claims 612
for n in ["customers", "policies", "claims"]:
    print(n, spark.table(f"bronze_{n}").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full reload
# MAGIC If you delete or change files and want a clean reload, drop the table AND delete its checkpoint folder:
# MAGIC ```
# MAGIC DROP TABLE workspace.insurance.bronze_claims;
# MAGIC ```
# MAGIC then remove `/Volumes/workspace/insurance/raw/_checkpoints/claims_bronze` (and `claims_schema`) in Catalog.
# MAGIC To test incremental loading: upload `claims_batch_2.csv` into `raw/claims/` and run this notebook again. Claims should grow to 1224.