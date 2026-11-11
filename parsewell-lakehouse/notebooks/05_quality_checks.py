# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Quality checks and revenue reconciliation
# MAGIC Automated tests that must pass before anyone trusts the dashboard. If one fails, this notebook stops
# MAGIC with an error, so a scheduled job would fail loudly instead of publishing bad numbers.
# MAGIC
# MAGIC The most important check is the **revenue reconciliation**: page counts from our own usage data,
# MAGIC times the Growth price, should match what the billing system invoiced.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

results = []


def check(name, passed, detail):
    results.append((name, "PASS" if passed else "FAIL", detail))


bronze = spark.table(t("bronze_api_events"))
silver = spark.table(t("silver_api_events"))
quarantine = spark.table(t("silver_api_events_quarantine"))

# 1. No duplicate events survive into silver
dupes = silver.groupBy("event_id").count().filter("count > 1").count()
check("Silver has one row per event_id", dupes == 0, f"{dupes} duplicated event_ids")

# 2. Every raw row is accounted for: clean + quarantined + duplicate retries = raw
raw, unique = bronze.count(), bronze.select("event_id").distinct().count()
clean, held = silver.count(), quarantine.count()
check("Row conservation (raw = clean + quarantined + duplicates)", raw == clean + held + (raw - unique),
      f"raw {raw:,} | clean {clean:,} | quarantined {held:,} | duplicates {raw - unique:,}")

# 3. Every clean event belongs to a known, non-internal customer
customers = spark.table(t("silver_customers"))
orphans = silver.join(customers, "customer_id", "left_anti").count()
check("No clean events from unknown customers", orphans == 0, f"{orphans} orphan events")

# 4. Every clean event could be costed at a model price
uncosted = (silver.join(spark.table(t("silver_model_pricing")).alias("p"),
                        (silver.model == F.col("p.model"))
                        & silver.event_date.between(F.col("p.effective_from"), F.col("p.effective_to")),
                        "left_anti").count())
check("Every clean event has a model price", uncosted == 0, f"{uncosted} events without a price")

# 5. Revenue reconciliation for the Growth plan
recon = (
    spark.table(t("gold_customer_monthly_margin")).filter("plan = 'growth'")
    .agg(F.sum(F.col("billable_pages") * GROWTH_PRICE_PER_PAGE).alias("expected_usd"),
         F.sum("revenue_usd").alias("invoiced_usd"))
    .first()
)
gap_pct = abs(recon.invoiced_usd - recon.expected_usd) / recon.invoiced_usd * 100
check("Growth revenue reconciles to usage within 1%", gap_pct < 1.0,
      f"invoiced ${recon.invoiced_usd:,.2f} vs usage-based ${recon.expected_usd:,.2f} (gap {gap_pct:.2f}%)")

# 6. Genuine data errors stay small: a sudden jump means an upstream system broke.
#    Test traffic is excluded on purpose, so it doesn't count as an error.
errors = quarantine.filter("reject_reason != 'test_traffic'").count()
error_pct = errors / unique * 100
check("Data errors under 2% of unique events", error_pct < 2,
      f"{errors:,} rows ({error_pct:.2f}%) quarantined for bad data, excluding test traffic")

# COMMAND ----------

display(spark.createDataFrame(results, ["check", "result", "detail"]))

failed = [r for r in results if r[1] == "FAIL"]
if failed:
    raise AssertionError(f"{len(failed)} quality check(s) failed: {[r[0] for r in failed]}")
print(f"All {len(results)} checks passed.")
