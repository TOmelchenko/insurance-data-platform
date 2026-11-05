# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 02 — Bronze Ingestion: Customers

# COMMAND ----------

from pyspark.sql import functions as F
import uuid

CATALOG_NAME = "insurance_lakehouse"
BRONZE_SCHEMA = "bronze"

ingest_run_id = str(uuid.uuid4())
print("Run id:", ingest_run_id)

datasets = ["customers", "policies", "claims", "payments", "agents", "fraud_indicators"]

results = []

for name in datasets:
    staging_table = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.raw_{name}"
    bronze_table = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.bronze_{name}"

    raw_df = spark.table(staging_table)

    bronze_df = (
        raw_df
        .withColumn("ingest_timestamp", F.current_timestamp())
        .withColumn("ingest_run_id", F.lit(ingest_run_id))
        .withColumn("source_file_name", F.lit(staging_table))
    )

    bronze_df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)

    raw_count = raw_df.count()
    bronze_count = spark.table(bronze_table).count()
    status = "PASS" if raw_count == bronze_count else "FAIL"

    print(f"{name} - raw: {raw_count}, bronze: {bronze_count}, status: {status}")
    results.append((name, raw_count, bronze_count, status))

summary = spark.createDataFrame(results, ["dataset", "raw_count", "bronze_count", "status"])
display(summary)

# COMMAND ----------

df_test = spark.createDataFrame([("test",)], ["value"])
df_test.write.mode("overwrite").csv("s3a://insurance-lakehouse-project/test/")
print("done")


