# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
from pyspark.sql import functions as F

CATALOG = "insurance_lakehouse"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
QUARANTINE_SCHEMA = "quarantine"

bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_customers"
silver_table = f"{CATALOG}.{SILVER_SCHEMA}.silver_customers"
quarantine_table = f"{CATALOG}.{QUARANTINE_SCHEMA}.quarantine_invalid_customers"

customers_bronze = spark.table(bronze_table)

customers_prepared = (
    customers_bronze
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("bundesland", F.trim(F.col("bundesland")))
    .withColumn("country", F.trim(F.col("country")))
    .withColumn("date_of_birth", F.to_date(F.col("date_of_birth")))
    .withColumn("registration_date", F.to_date(F.col("registration_date")))
    .withColumn("gdpr_consent", F.col("gdpr_consent").cast("boolean"))
    .withColumn("customer_hash", F.sha2(F.col("customer_id").cast("string"), 256))
    .withColumn("email_hash", F.sha2(F.lower(F.trim(F.col("email"))), 256))
    .withColumn("phone_hash", F.sha2(F.trim(F.col("phone_number")), 256))
    .withColumn("customer_age", F.floor(F.months_between(F.current_date(), F.col("date_of_birth")) / 12))
)

invalid_customers = (
    customers_prepared
    .filter(F.col("customer_id").isNull() | F.col("gdpr_consent").isNull())
    .withColumn("record_id", F.col("customer_id"))
    .withColumn("source_table", F.lit("bronze_customers"))
    .withColumn(
        "error_reason",
        F.when(F.col("customer_id").isNull(), F.lit("missing_customer_id"))
         .when(F.col("gdpr_consent").isNull(), F.lit("invalid_gdpr_consent"))
         .otherwise(F.lit("unknown_customer_error"))
    )
    .withColumn("error_severity", F.lit("HIGH"))
    .withColumn("quarantine_timestamp", F.current_timestamp())
    .withColumn("original_record_json", F.to_json(F.struct(*[F.col(c) for c in customers_prepared.columns])))
)

valid_customers = (
    customers_prepared
    .filter(F.col("customer_id").isNotNull() & F.col("gdpr_consent").isNotNull())
    .dropDuplicates(["customer_id"])
    # drop all pii_fields per pii_config.yml
    .drop("first_name", "last_name", "email", "phone_number", "street", "postal_code")
)

valid_customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)

(
    invalid_customers
    .select("record_id", "source_table", "error_reason", "error_severity", "quarantine_timestamp",
            "source_file_name", "ingest_run_id", "original_record_json")
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(quarantine_table)
)

print("Bronze customers:", customers_bronze.count())
print("Silver customers:", spark.table(silver_table).count())
print("Quarantine customers:", spark.table(quarantine_table).count())
