# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: typed, cleaned, deduplicated tables (idempotent MERGE)
# MAGIC Every table is updated with MERGE on its key. Running the notebook twice, or after a reload of bronze,
# MAGIC never creates duplicates.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F, Window

spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA insurance")


def dedupe_latest(df, key):
    """One row per key: the most recently ingested one."""
    w = Window.partitionBy(key).orderBy(F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def upsert(df, table, key):
    """Create the table on the first run, afterwards MERGE on the key."""
    if not spark.catalog.tableExists(table):
        df.write.saveAsTable(table)
        return
    (
        DeltaTable.forName(spark, table).alias("t")
        .merge(df.alias("s"), f"t.{key} = s.{key}")
        .whenMatchedUpdateAll(condition="s._ingested_at > t._ingested_at")  # only newer data wins
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------
# MAGIC %md ## customers

# COMMAND ----------

customers = dedupe_latest(spark.table("bronze_customers"), "customer_id").select(
    F.trim("customer_id").alias("customer_id"),
    F.trim("first_name").alias("first_name"),
    F.trim("last_name").alias("last_name"),
    F.expr("try_cast(birth_date as date)").alias("birth_date"),
    F.trim("city").alias("city"),
    F.trim("postal_code").alias("postal_code"),
    "_ingested_at",
)
upsert(customers, "silver_customers", "customer_id")

# COMMAND ----------
# MAGIC %md ## policies

# COMMAND ----------

policies = dedupe_latest(spark.table("bronze_policies"), "policy_id").select(
    F.trim("policy_id").alias("policy_id"),
    F.trim("customer_id").alias("customer_id"),
    F.trim("product").alias("product"),
    F.expr("try_cast(start_date as date)").alias("start_date"),
    F.expr("try_cast(annual_premium as decimal(10,2))").alias("annual_premium"),
    F.trim("status").alias("status"),
    "_ingested_at",
)
upsert(policies, "silver_policies", "policy_id")

# COMMAND ----------
# MAGIC %md ## claims
# MAGIC - duplicate claim_id: keep the latest ingested row (before MERGE, otherwise MERGE fails on duplicate source keys)
# MAGIC - claim_amount: try_cast, bad values ("n/a", "unknown", "-") become NULL and are flagged
# MAGIC - empty claim_type: set to 'unknown'

# COMMAND ----------

claims = dedupe_latest(spark.table("bronze_claims"), "claim_id").select(
    F.trim("claim_id").alias("claim_id"),
    F.trim("policy_id").alias("policy_id"),
    F.expr("try_cast(claim_date as date)").alias("claim_date"),
    F.expr("try_cast(claim_amount as decimal(12,2))").alias("claim_amount"),
    F.when(F.col("claim_type").isNull() | (F.trim("claim_type") == ""), "unknown")
     .otherwise(F.trim("claim_type")).alias("claim_type"),
    F.trim("status").alias("status"),
    "_ingested_at",
).withColumn("is_amount_valid", F.col("claim_amount").isNotNull())
upsert(claims, "silver_claims", "claim_id")

# COMMAND ----------
# MAGIC %md ## Checks

# COMMAND ----------

display(spark.sql("""
SELECT
  count(*)                                   AS rows,
  count(DISTINCT claim_id)                   AS distinct_ids,
  sum(CASE WHEN is_amount_valid THEN 0 ELSE 1 END) AS bad_amounts,
  sum(CASE WHEN claim_type = 'unknown' THEN 1 ELSE 0 END) AS unknown_types
FROM silver_claims
"""))
# Expected after batch 1: rows = distinct_ids = 600. After batch 2: 1200.
# Run this notebook a second time: the numbers must not change (idempotent).

# Every claim must point to an existing policy
display(spark.sql("""
SELECT count(*) AS orphan_claims FROM silver_claims c
LEFT JOIN silver_policies p USING (policy_id) WHERE p.policy_id IS NULL
"""))
