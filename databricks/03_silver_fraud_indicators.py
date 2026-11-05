# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "insurance_lakehouse"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
QUARANTINE_SCHEMA = "quarantine"

bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_fraud_indicators"
silver_table = f"{CATALOG}.{SILVER_SCHEMA}.silver_fraud_indicators"
quarantine_table = f"{CATALOG}.{QUARANTINE_SCHEMA}.quarantine_invalid_fraud_indicators"

fraud_bronze = spark.table(bronze_table)

fraud_prepared = (
    fraud_bronze
    .withColumn("claim_id", F.trim(F.col("claim_id")))
    .withColumn("risk_category",
        F.when(F.col("risk_score") >= 80, F.lit("HIGH"))
         .when(F.col("risk_score") >= 50, F.lit("MEDIUM"))
         .otherwise(F.lit("LOW"))
    )
    .withColumn("fraud_indicator_count",
        F.col("suspicious_amount_flag").cast("int") +
        F.col("duplicate_claim_flag").cast("int") +
        F.col("late_report_flag").cast("int") +
        F.col("high_risk_region_flag").cast("int")
    )
)

invalid_fraud = (
    fraud_prepared
    .filter(
        F.col("claim_id").isNull() |
        F.col("risk_score").isNull() |
        (F.col("risk_score") < 0) |
        (F.col("risk_score") > 100)
    )
    .withColumn("record_id", F.col("claim_id"))
    .withColumn("source_table", F.lit("bronze_fraud_indicators"))
    .withColumn(
        "error_reason",
        F.when(F.col("claim_id").isNull(), F.lit("missing_claim_id"))
         .when(F.col("risk_score").isNull(), F.lit("missing_risk_score"))
         .when((F.col("risk_score") < 0) | (F.col("risk_score") > 100), F.lit("invalid_risk_score"))
         .otherwise(F.lit("unknown_fraud_error"))
    )
    .withColumn("error_severity", F.lit("HIGH"))
    .withColumn("quarantine_timestamp", F.current_timestamp())
    .withColumn("original_record_json", F.to_json(F.struct(*[F.col(c) for c in fraud_prepared.columns])))
)

# keep highest risk_score per claim_id
window = Window.partitionBy("claim_id").orderBy(F.col("risk_score").desc())

valid_fraud = (
    fraud_prepared
    .filter(
        F.col("claim_id").isNotNull() &
        F.col("risk_score").isNotNull() &
        (F.col("risk_score") >= 0) &
        (F.col("risk_score") <= 100)
    )
    .withColumn("row_num", F.row_number().over(window))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

valid_fraud.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)

(
    invalid_fraud
    .select("record_id", "source_table", "error_reason", "error_severity", "quarantine_timestamp",
            "source_file_name", "ingest_run_id", "original_record_json")
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(quarantine_table)
)

print("Bronze fraud indicators:", fraud_bronze.count())
print("Distinct claim_ids in bronze:", fraud_bronze.select("claim_id").distinct().count())
print("Silver fraud indicators:", spark.table(silver_table).count())
print("Quarantine fraud indicators:", spark.table(quarantine_table).count())
print("Duplicates removed:", fraud_bronze.count() - spark.table(silver_table).count())
