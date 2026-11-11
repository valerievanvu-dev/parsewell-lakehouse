# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver: clean, standardize and quarantine
# MAGIC Silver tables are typed, deduplicated and trustworthy. Rows that can't be trusted are not deleted:
# MAGIC they go to a quarantine table with the reason, so nothing disappears silently.
# MAGIC
# MAGIC | Problem in the raw data | Rule applied here |
# MAGIC |---|---|
# MAGIC | Same customer appears twice | Keep the most recently updated row |
# MAGIC | Plan spelled `Growth`, `GROWTH `, ` growth` | Trim and lowercase |
# MAGIC | Blank region | Set to `unknown` |
# MAGIC | Same API call logged twice (client retries) | Keep one row per `event_id` |
# MAGIC | Timestamps in two formats | Parse both; quarantine what neither format can read |
# MAGIC | Model spelled `Parse-Large`, `parse_large` | Standardize to `parse-large` |
# MAGIC | Page count of 0 or negative on a successful call | Quarantine |
# MAGIC | Calls from accounts not in the customer database | Quarantine |
# MAGIC | Test keys (`sk_test_`) and Parsewell's internal account | Quarantine as test traffic: never billed |
# MAGIC | Invoice amounts stored as `$12,000.00` text | Convert to numbers |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

latest_first = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())

customers = (
    spark.table(t("bronze_customers"))
    .withColumn("updated_at", F.expr("try_to_timestamp(updated_at, 'yyyy-MM-dd HH:mm:ss')"))
    .withColumn("_rank", F.row_number().over(latest_first))
    .filter("_rank = 1")
    .select(
        F.trim("customer_id").alias("customer_id"),
        F.trim("company_name").alias("company_name"),
        F.lower(F.trim("plan")).alias("plan"),
        F.trim("industry").alias("industry"),
        F.coalesce(F.expr("nullif(trim(region), '')"), F.lit("unknown")).alias("region"),
        F.expr("try_cast(signup_date AS DATE)").alias("signup_date"),
        F.expr("try_cast(monthly_contract_usd AS DECIMAL(12,2))").alias("monthly_contract_usd"),
        "updated_at",
    )
)
customers.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("silver_customers"))
display(spark.table(t("silver_customers")).groupBy("plan").count().orderBy("plan"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model pricing
# MAGIC Each price gets an end date (the day before the next price starts), so every API call can be costed at
# MAGIC the price in effect on the day it happened.

# COMMAND ----------

next_price = Window.partitionBy("model").orderBy("effective_from")

pricing = (
    spark.table(t("bronze_model_pricing"))
    .select(
        F.lower(F.trim("model")).alias("model"),
        F.col("effective_from").cast("date").alias("effective_from"),
        F.col("input_cost_per_1k_tokens").cast("double").alias("input_cost_per_1k_tokens"),
        F.col("output_cost_per_1k_tokens").cast("double").alias("output_cost_per_1k_tokens"),
    )
    .withColumn("effective_to",
                F.coalesce(F.date_sub(F.lead("effective_from").over(next_price), 1), F.lit("9999-12-31").cast("date")))
)
pricing.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("silver_model_pricing"))
display(spark.table(t("silver_model_pricing")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Invoices

# COMMAND ----------

invoices = (
    spark.table(t("bronze_invoices"))
    .select(
        "invoice_id",
        "customer_id",
        F.to_date(F.concat("invoice_month", F.lit("-01"))).alias("invoice_month"),
        F.expr("try_cast(regexp_replace(amount, '[$,]', '') AS DECIMAL(12,2))").alias("amount_usd"),
        F.lower(F.trim("status")).alias("status"),
    )
)
invoices.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("silver_invoices"))
print(f"{t('silver_invoices')}: {spark.table(t('silver_invoices')).count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## API events

# COMMAND ----------

bronze_events = spark.table(t("bronze_api_events"))

parsed = (
    bronze_events
    .withColumn("event_ts_parsed", F.coalesce(
        F.expr("try_to_timestamp(event_ts, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")"),
        F.expr("try_to_timestamp(event_ts, 'MM/dd/yyyy HH:mm:ss')"),
    ))
    .dropDuplicates(["event_id"])
    .select(
        "event_id",
        F.col("event_ts_parsed").alias("event_ts"),
        F.to_date("event_ts_parsed").alias("event_date"),
        "customer_id",
        "api_key",
        "endpoint",
        F.regexp_replace(F.lower(F.trim("model")), "_", "-").alias("model"),
        F.expr("try_cast(pages AS INT)").alias("pages"),
        F.expr("try_cast(status_code AS INT)").alias("status_code"),
        F.expr("try_cast(latency_ms AS INT)").alias("latency_ms"),
        F.expr("try_cast(input_tokens AS BIGINT)").alias("input_tokens"),
        F.expr("try_cast(output_tokens AS BIGINT)").alias("output_tokens"),
        "_source_file",
    )
)

known = spark.table(t("silver_customers")).select("customer_id", "plan")

classified = (
    parsed.join(known, "customer_id", "left")
    .withColumn("reject_reason",
        F.when(F.col("event_ts").isNull(), "unparseable_timestamp")
         .when(F.col("plan").isNull(), "unknown_customer")
         .when(F.col("api_key").startswith("sk_test_") | (F.col("plan") == "internal"), "test_traffic")
         .when((F.col("status_code") == 200) & (F.coalesce(F.col("pages"), F.lit(0)) <= 0), "invalid_page_count"))
)

(classified.filter("reject_reason IS NULL").drop("reject_reason", "plan")
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("silver_api_events")))

(classified.filter("reject_reason IS NOT NULL").drop("plan")
    .withColumn("_quarantined_at", F.current_timestamp())
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("silver_api_events_quarantine")))

# COMMAND ----------

bronze_rows = bronze_events.count()
unique_rows = bronze_events.select("event_id").distinct().count()
silver_rows = spark.table(t("silver_api_events")).count()

print(f"Bronze rows:            {bronze_rows:>10,}")
print(f"Duplicate retries:      {bronze_rows - unique_rows:>10,}")
print(f"Silver (clean) rows:    {silver_rows:>10,}")
display(spark.table(t("silver_api_events_quarantine")).groupBy("reject_reason").count().orderBy(F.desc("count")))
