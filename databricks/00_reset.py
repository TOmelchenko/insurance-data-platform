# Databricks notebook source
# MAGIC %md
# MAGIC # Reset: remove bronze data for a full test run
# MAGIC Drops the tables AND deletes the Auto Loader checkpoints. Both are required:
# MAGIC - only tables dropped: Auto Loader thinks files are processed, tables stay empty
# MAGIC - only checkpoints deleted: files are loaded again, duplicates in bronze
# MAGIC
# MAGIC Raw CSV files are NOT touched (except batch 2, see the flag below).

# COMMAND ----------

dbutils.widgets.dropdown("remove_batch_2", "yes", ["yes", "no"], "Remove claims_batch_2.csv")

CATALOG, SCHEMA = "workspace", "insurance"
BASE = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

# COMMAND ----------

tables = ["bronze_customers", "bronze_policies", "bronze_claims"]
# dbt tables (stg_*, fct_*, dim_*, agg_*) are rebuilt by `dbt build`, no reset needed
for t in tables:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{t}")
    print("dropped", t)

# COMMAND ----------

dbutils.fs.rm(f"{BASE}/_checkpoints", True)
print("checkpoints removed")

if dbutils.widgets.get("remove_batch_2") == "yes":
    dbutils.fs.rm(f"{BASE}/claims/claims_batch_2.csv")
    print("claims_batch_2.csv removed (upload it again to test incremental loading)")

# COMMAND ----------

# Raw files that remain
for folder in ["customers", "policies", "claims"]:
    print(folder, [f.name for f in dbutils.fs.ls(f"{BASE}/{folder}")])
