# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: typed, cleaned, deduplicated tables

# COMMAND ----------

from pyspark.sql import functions as F, Window

spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA insurance")

# COMMAND ----------
# MAGIC %md ## customers

# COMMAND ----------

customers = (
    spark.table("bronze_customers")
    .select(
        F.trim("customer_id").alias("customer_id"),
        F.trim("first_name").alias("first_name"),
        F.trim("last_name").alias("last_name"),
        F.expr("try_cast(birth_date as date)").alias("birth_date"),
        F.trim("city").alias("city"),
        F.trim("postal_code").alias("postal_code"),
    )
    .dropDuplicates(["customer_id"])
)
customers.write.mode("overwrite").saveAsTable("silver_customers")

# COMMAND ----------
# MAGIC %md ## policies

# COMMAND ----------

policies = (
    spark.table("bronze_policies")
    .select(
        F.trim("policy_id").alias("policy_id"),
        F.trim("customer_id").alias("customer_id"),
        F.trim("product").alias("product"),
        F.expr("try_cast(start_date as date)").alias("start_date"),
        F.expr("try_cast(annual_premium as decimal(10,2))").alias("annual_premium"),
        F.trim("status").alias("status"),
    )
    .dropDuplicates(["policy_id"])
)
policies.write.mode("overwrite").saveAsTable("silver_policies")

# COMMAND ----------
# MAGIC %md ## claims
# MAGIC - duplicate claim_id: keep the latest ingested row
# MAGIC - claim_amount: try_cast, bad values ("n/a", "unknown", "-") become NULL and are flagged
# MAGIC - empty claim_type: set to 'unknown'

# COMMAND ----------

w = Window.partitionBy("claim_id").orderBy(F.col("_ingested_at").desc())

claims = (
    spark.table("bronze_claims")
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .drop("_rn")
    .select(
        F.trim("claim_id").alias("claim_id"),
        F.trim("policy_id").alias("policy_id"),
        F.expr("try_cast(claim_date as date)").alias("claim_date"),
        F.expr("try_cast(claim_amount as decimal(12,2))").alias("claim_amount"),
        F.when(F.trim("claim_type") == "", "unknown")
         .when(F.col("claim_type").isNull(), "unknown")
         .otherwise(F.trim("claim_type")).alias("claim_type"),
        F.trim("status").alias("status"),
    )
    .withColumn("is_amount_valid", F.col("claim_amount").isNotNull())
)
claims.write.mode("overwrite").saveAsTable("silver_claims")

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
# Expected after batch 1: rows = distinct_ids = 600, bad_amounts ~16, unknown_types ~17

# Every claim must point to an existing policy
orphans = spark.sql("""
SELECT count(*) AS orphan_claims FROM silver_claims c
LEFT JOIN silver_policies p USING (policy_id) WHERE p.policy_id IS NULL
""")
display(orphans)
