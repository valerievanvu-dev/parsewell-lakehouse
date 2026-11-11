# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Config
# MAGIC Shared settings for every notebook in this project. Other notebooks load it with `%run ./00_config`.
# MAGIC
# MAGIC It uses whatever catalog your workspace defaults to, then creates a `parsewell` schema and a `raw` volume
# MAGIC (a governed folder in Unity Catalog where the raw files land).

# COMMAND ----------

CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "parsewell"
VOLUME = "raw"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"


def t(name):
    """Fully qualified table name: catalog.schema.table"""
    return f"{CATALOG}.{SCHEMA}.{name}"


# Business rules for the fictional customer, Parsewell AI
GROWTH_PRICE_PER_PAGE = 0.01      # Growth plan: pay per page processed
FREE_PAGES_PER_MONTH = 1000       # Free plan: hard monthly cap, then HTTP 429
HEALTH_WINDOW_DAYS = 28           # Health score compares the last 28 days with the 28 before

print(f"Catalog: {CATALOG} | Schema: {SCHEMA} | Raw files: {RAW_PATH}")
