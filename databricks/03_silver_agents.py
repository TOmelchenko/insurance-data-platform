# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "insurance_lakehouse"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
QUARANTINE_SCHEMA = "quarantine"

bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_agents"
silver_table = f"{CATALOG}.{SILVER_SCHEMA}.silver_agents"
quarantine_table = f"{CATALOG}.{QUARANTINE_SCHEMA}.quarantine_invalid_agents"

agents_bronze = spark.table(bronze_table)

agents_prepared = (
    agents_bronze
    .withColumn("agent_id", F.trim(F.col("agent_id")))
    .withColumn("agent_name", F.initcap(F.trim(F.col("agent_name"))))
    .withColumn("region", F.trim(F.col("region")))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("bundesland", F.trim(F.col("bundesland")))
    .withColumn("agent_hash", F.sha2(F.col("agent_id").cast("string"), 256))
)

invalid_agents = (
    agents_prepared
    .filter(
        F.col("agent_id").isNull() |
        F.col("agent_name").isNull() |
        F.col("commission_rate").isNull() |
        (F.col("commission_rate") < 0) |
        (F.col("commission_rate") > 1)
    )
    .withColumn("record_id", F.col("agent_id"))
    .withColumn("source_table", F.lit("bronze_agents"))
    .withColumn(
        "error_reason",
        F.when(F.col("agent_id").isNull(), F.lit("missing_agent_id"))
         .when(F.col("agent_name").isNull(), F.lit("missing_agent_name"))
         .when(F.col("commission_rate").isNull(), F.lit("missing_commission_rate"))
         .when((F.col("commission_rate") < 0) | (F.col("commission_rate") > 1), F.lit("invalid_commission_rate"))
         .otherwise(F.lit("unknown_agent_error"))
    )
    .withColumn("error_severity", F.lit("HIGH"))
    .withColumn("quarantine_timestamp", F.current_timestamp())
    .withColumn("original_record_json", F.to_json(F.struct(*[F.col(c) for c in agents_prepared.columns])))
)

valid_agents = (
    agents_prepared
    .filter(
        F.col("agent_id").isNotNull() &
        F.col("agent_name").isNotNull() &
        F.col("commission_rate").isNotNull() &
        (F.col("commission_rate") >= 0) &
        (F.col("commission_rate") <= 1)
    )
    .dropDuplicates(["agent_id"])
)

valid_agents.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)

(
    invalid_agents
    .select("record_id", "source_table", "error_reason", "error_severity", "quarantine_timestamp",
            "source_file_name", "ingest_run_id", "original_record_json")
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(quarantine_table)
)

print("Bronze agents:", agents_bronze.count())
print("Silver agents:", spark.table(silver_table).count())
print("Quarantine agents:", spark.table(quarantine_table).count())
