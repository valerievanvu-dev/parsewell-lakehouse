# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze: land the raw data as-is
# MAGIC Bronze tables are a faithful copy of the source files. Every column is kept as text and nothing is
# MAGIC cleaned, so we can always trace a number in the dashboard back to the exact file it came from.
# MAGIC Each row also records which file it came from and when it was loaded.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


def with_lineage(df):
    return (df.withColumn("_source_file", F.col("_metadata.file_path"))
              .withColumn("_ingested_at", F.current_timestamp()))


def land_csv(folder, table):
    df = (spark.read.option("header", True).option("inferSchema", False)
          .csv(f"{RAW_PATH}/{folder}"))
    df = with_lineage(df)
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t(table))
    print(f"{t(table)}: {spark.table(t(table)).count():,} rows")


land_csv("customers", "bronze_customers")
land_csv("model_pricing", "bronze_model_pricing")
land_csv("invoices", "bronze_invoices")

# COMMAND ----------

# API events: read every field as text so a bad value can never break the load
event_fields = ["event_id", "event_ts", "customer_id", "api_key", "endpoint", "model", "pages",
                "status_code", "latency_ms", "input_tokens", "output_tokens"]
event_schema = StructType([StructField(c, StringType()) for c in event_fields])

events = (spark.read.schema(event_schema)
          .option("primitivesAsString", True)
          .json(f"{RAW_PATH}/api_events"))
events = with_lineage(events)
(events.write.mode("overwrite").option("overwriteSchema", True)
       .saveAsTable(t("bronze_api_events")))

print(f"{t('bronze_api_events')}: {spark.table(t('bronze_api_events')).count():,} rows")
