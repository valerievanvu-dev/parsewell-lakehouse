"""
Run the whole pipeline on local Spark, outside Databricks.

    pip install pyspark==4.0.1
    python tests/run_locally.py

Tables are stored on disk under .local/ instead of in Unity Catalog, and the raw "volume" is a local folder.
Every notebook runs in order, then every dashboard query runs against the gold tables.
"""
import os
import shutil
import time

from pyspark.sql import SparkSession

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOKS = os.path.join(ROOT, "notebooks")
LOCAL = os.path.join(ROOT, ".local")
RAW = os.path.join(LOCAL, "volume")

shutil.rmtree(LOCAL, ignore_errors=True)
os.makedirs(RAW)

spark = (SparkSession.builder.master("local[*]")
         .config("spark.sql.warehouse.dir", os.path.join(LOCAL, "warehouse"))
         .config("spark.sql.ansi.enabled", "true")       # same strict behavior as Databricks serverless
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

real_sql = spark.sql


def sql_without_volumes(query, *args, **kwargs):
    """Local Spark has no Unity Catalog volumes, so skip CREATE VOLUME."""
    return None if "CREATE VOLUME" in query else real_sql(query, *args, **kwargs)


spark.sql = sql_without_volumes
namespace = {"spark": spark, "display": lambda df: df.show(60, truncate=False)}


def notebook_source(path):
    """Turn a Databricks .py notebook into plain Python: drop markdown and cell markers, handle %run."""
    lines = []
    for line in open(path):
        if line.startswith("# MAGIC %run ./00_config"):
            lines.append("_run_config()\n")
        elif not line.startswith(("# MAGIC", "# COMMAND", "# Databricks notebook source")):
            lines.append(line)
    return "".join(lines)


def _run_config():
    exec(notebook_source(os.path.join(NOTEBOOKS, "00_config.py")), namespace)
    namespace["RAW_PATH"] = RAW


namespace["_run_config"] = _run_config

for nb in ["01_generate_synthetic_data", "02_bronze_ingest", "03_silver_clean", "04_gold_metrics",
           "05_quality_checks"]:
    start = time.time()
    print(f"\n===== {nb} =====")
    exec(compile(notebook_source(os.path.join(NOTEBOOKS, f"{nb}.py")), nb, "exec"), namespace)
    print(f"----- finished in {time.time() - start:.1f}s")

queries = open(os.path.join(ROOT, "dashboard", "dashboard_queries.sql")).read().split(";")
for i, query in enumerate([q for q in queries if "SELECT" in q], 1):
    body = "\n".join(line for line in query.splitlines() if not line.strip().startswith("--"))
    print(f"\n===== dashboard query {i} =====")
    real_sql(body).show(40, truncate=False)

print("\nAll notebooks and dashboard queries ran successfully.")
