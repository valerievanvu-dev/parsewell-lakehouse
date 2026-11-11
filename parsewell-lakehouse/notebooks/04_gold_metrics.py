# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold: the business answers
# MAGIC Gold tables answer the questions Parsewell's leadership asked for:
# MAGIC
# MAGIC | Table | Question it answers |
# MAGIC |---|---|
# MAGIC | `gold_customer_daily_usage` | How much did each customer use, and what did it cost us in model fees? |
# MAGIC | `gold_customer_monthly_margin` | Which customers make us money after model costs, and which lose money? |
# MAGIC | `gold_customer_health` | Who is at risk of leaving, who is ready to upgrade, whose contract needs repricing? |
# MAGIC | `gold_data_quality` | How much of the raw data did we have to exclude, and why? |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

events = spark.table(t("silver_api_events")).alias("e")
pricing = spark.table(t("silver_model_pricing")).alias("p")
customers = spark.table(t("silver_customers")).select("customer_id", "company_name", "plan",
                                                       "monthly_contract_usd")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily usage and model cost per customer
# MAGIC Each call is costed at the model price in effect that day. Failed calls (HTTP 500) still cost money
# MAGIC because the model ran before the error; rejected calls (HTTP 429) cost nothing.

# COMMAND ----------

costed = (
    events.join(pricing, (F.col("e.model") == F.col("p.model"))
                & F.col("e.event_date").between(F.col("p.effective_from"), F.col("p.effective_to")), "left")
    .select("e.*",
            (F.col("e.input_tokens") / 1000 * F.col("p.input_cost_per_1k_tokens")
             + F.col("e.output_tokens") / 1000 * F.col("p.output_cost_per_1k_tokens")).alias("inference_cost_usd"))
)

daily = (
    costed.groupBy("event_date", "customer_id")
    .agg(
        F.count("*").alias("api_calls"),
        F.sum(F.when(F.col("status_code") == 200, 1).otherwise(0)).alias("successful_calls"),
        F.sum(F.when(F.col("status_code") >= 500, 1).otherwise(0)).alias("server_errors"),
        F.sum(F.when(F.col("status_code") == 429, 1).otherwise(0)).alias("quota_rejections"),
        F.sum(F.when(F.col("status_code") == 200, F.col("pages")).otherwise(0)).alias("billable_pages"),
        F.sum(F.when((F.col("status_code") == 200) & (F.col("model") == "parse-large"), F.col("pages"))
              .otherwise(0)).alias("large_model_pages"),
        F.sum("input_tokens").alias("input_tokens"),
        F.sum("output_tokens").alias("output_tokens"),
        F.round(F.sum("inference_cost_usd"), 4).alias("inference_cost_usd"),
    )
    .join(customers.select("customer_id", "company_name", "plan"), "customer_id")
)
daily.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("gold_customer_daily_usage"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monthly gross margin per customer
# MAGIC Revenue comes from the billing system (invoices); cost comes from our own usage data. Gross margin here
# MAGIC means revenue minus model inference cost only: hosting and staff costs are out of scope.

# COMMAND ----------

monthly_usage = (
    spark.table(t("gold_customer_daily_usage"))
    .withColumn("month", F.trunc("event_date", "month"))
    .groupBy("month", "customer_id", "company_name", "plan")
    .agg(F.sum("billable_pages").alias("billable_pages"),
         F.sum("large_model_pages").alias("large_model_pages"),
         F.sum("inference_cost_usd").alias("inference_cost_usd"))
)

revenue = (spark.table(t("silver_invoices"))
           .groupBy("customer_id", F.col("invoice_month").alias("month"))
           .agg(F.sum("amount_usd").cast("double").alias("revenue_usd")))

monthly = (
    monthly_usage.join(revenue, ["customer_id", "month"], "left")
    .withColumn("revenue_usd", F.coalesce("revenue_usd", F.lit(0.0)))
    .withColumn("gross_margin_usd", F.round(F.col("revenue_usd") - F.col("inference_cost_usd"), 2))
    .withColumn("gross_margin_pct",
                F.when(F.col("revenue_usd") > 0,
                       F.round(F.col("gross_margin_usd") / F.col("revenue_usd") * 100, 1)))
    .withColumn("large_model_share_pct",
                F.round(F.col("large_model_pages") / F.greatest(F.col("billable_pages"), F.lit(1)) * 100, 1))
    .withColumn("inference_cost_usd", F.round("inference_cost_usd", 2))
    .select("month", "customer_id", "company_name", "plan", "billable_pages", "large_model_share_pct",
            "revenue_usd", "inference_cost_usd", "gross_margin_usd", "gross_margin_pct")
)
monthly.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("gold_customer_monthly_margin"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer health
# MAGIC Compares each customer's last 28 days with the 28 days before. Rules, in priority order:
# MAGIC 1. **At risk**: usage down 40% or more, or more than 5% of calls failing
# MAGIC 2. **Margin review**: an Enterprise contract that lost money last month
# MAGIC 3. **Expansion opportunity**: a Free customer hitting the monthly cap, or a Growth customer whose usage rose 50% or more
# MAGIC 4. **Healthy**: everyone else

# COMMAND ----------

usage = spark.table(t("gold_customer_daily_usage"))
as_of = usage.agg(F.max("event_date")).first()[0]
window_start = F.date_sub(F.lit(as_of), HEALTH_WINDOW_DAYS - 1)
prior_start = F.date_sub(F.lit(as_of), 2 * HEALTH_WINDOW_DAYS - 1)
in_last = F.col("event_date") >= window_start
in_prior = (F.col("event_date") >= prior_start) & (F.col("event_date") < window_start)

last_month = spark.table(t("gold_customer_monthly_margin")).agg(F.max("month")).first()[0]
latest_margin = (spark.table(t("gold_customer_monthly_margin"))
                 .filter(F.col("month") == F.lit(last_month))
                 .select("customer_id", F.col("gross_margin_pct").alias("last_month_margin_pct")))

health = (
    usage.groupBy("customer_id", "company_name", "plan")
    .agg(
        F.sum(F.when(in_last, F.col("billable_pages")).otherwise(0)).alias("pages_last_28d"),
        F.sum(F.when(in_prior, F.col("billable_pages")).otherwise(0)).alias("pages_prior_28d"),
        F.sum(F.when(in_last, F.col("api_calls")).otherwise(0)).alias("calls_last_28d"),
        F.sum(F.when(in_last, F.col("server_errors")).otherwise(0)).alias("errors_last_28d"),
        F.sum(F.when(in_last, F.col("quota_rejections")).otherwise(0)).alias("quota_rejections_last_28d"),
    )
    .join(latest_margin, "customer_id", "left")
    .withColumn("usage_change_pct",
                F.when(F.col("pages_prior_28d") > 0,
                       F.round((F.col("pages_last_28d") / F.col("pages_prior_28d") - 1) * 100, 1)))
    .withColumn("error_rate_pct",
                F.round(F.col("errors_last_28d") / F.greatest(F.col("calls_last_28d"), F.lit(1)) * 100, 1))
)

health = (
    health
    .withColumn("health_status",
        F.when((F.col("usage_change_pct") <= -40) | (F.col("error_rate_pct") > 5), "at_risk")
         .when((F.col("plan") == "enterprise") & (F.col("last_month_margin_pct") < 0), "margin_review")
         .when(((F.col("plan") == "free") & (F.col("quota_rejections_last_28d") > 0))
               | ((F.col("plan") == "growth") & (F.col("usage_change_pct") >= 50)), "expansion_opportunity")
         .otherwise("healthy"))
    .withColumn("reason",
        F.when(F.col("health_status") == "at_risk",
               F.concat(F.lit("Usage "), F.col("usage_change_pct").cast("string"), F.lit("% vs prior 28 days; "),
                        F.col("error_rate_pct").cast("string"), F.lit("% of calls failing")))
         .when(F.col("health_status") == "margin_review",
               F.concat(F.lit("Lost money last month: margin "), F.col("last_month_margin_pct").cast("string"),
                        F.lit("%")))
         .when((F.col("health_status") == "expansion_opportunity") & (F.col("plan") == "free"),
               F.concat(F.col("quota_rejections_last_28d").cast("string"),
                        F.lit(" calls blocked by the free-plan cap in 28 days")))
         .when(F.col("health_status") == "expansion_opportunity",
               F.concat(F.lit("Usage up "), F.col("usage_change_pct").cast("string"), F.lit("% vs prior 28 days"))))
    .withColumn("as_of_date", F.lit(as_of))
)
health.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("gold_customer_health"))

display(spark.table(t("gold_customer_health")).filter("health_status != 'healthy'")
        .select("company_name", "plan", "health_status", "reason").orderBy("health_status"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality summary

# COMMAND ----------

bronze = spark.table(t("bronze_api_events"))
dupes = bronze.count() - bronze.select("event_id").distinct().count()

quality = (
    spark.table(t("silver_api_events_quarantine")).groupBy("reject_reason").count()
    .union(spark.createDataFrame([("duplicate_retry", dupes)], ["reject_reason", "count"]))
    .union(spark.createDataFrame([("clean", spark.table(t("silver_api_events")).count())],
                                 ["reject_reason", "count"]))
    .withColumnRenamed("reject_reason", "category")
    .withColumnRenamed("count", "rows")
    .withColumn("share_of_raw_pct", F.round(F.col("rows") / F.lit(bronze.count()) * 100, 2))
)
quality.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(t("gold_data_quality"))
display(spark.table(t("gold_data_quality")).orderBy(F.desc("rows")))
