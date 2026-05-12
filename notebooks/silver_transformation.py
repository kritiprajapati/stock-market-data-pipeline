# Databricks notebook source
# MAGIC %pip install azure-storage-blob

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, DateType
import datetime

# ── Configuration ─────────────────────────────────────────────────────────────
STORAGE_ACCOUNT = "stockpipelinedl"
STORAGE_KEY = dbutils.secrets.get(scope="stock-pipeline", key="azure-storage-key")
CONTAINER_BRONZE = "bronze"
CONTAINER_SILVER = "silver"
# DATE = "2026-05-05"

try:
    DATE = dbutils.widgets.get("execution_date")
except:
    # DATE = "2026-05-08"
    DATE = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

print(f"Processing date: {DATE}")

# ── Set Azure Storage key in Spark ────────────────────────────────────────────
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)

print("✅ Spark connected to Azure Storage")

# COMMAND ----------

# ── Read raw JSON from Bronze via Spark ───────────────────────────────────────
bronze_path = f"abfss://{CONTAINER_BRONZE}@{STORAGE_ACCOUNT}.dfs.core.windows.net/stock_data/{DATE}/raw_stocks.json"

raw_df = spark.read \
    .option("multiline", "true") \
    .json(bronze_path)

print(f"✅ Read {raw_df.count()} records from Bronze")
raw_df.printSchema()
raw_df.display(truncate=False)

# COMMAND ----------

# ── Select and rename columns ─────────────────────────────────────────────────
cleaned_df = raw_df.select(
    F.col("symbol").cast(StringType()).alias("ticker"),
    F.col("from").cast(DateType()).alias("trade_date"),
    F.col("open").cast(DoubleType()).alias("open_price"),
    F.col("high").cast(DoubleType()).alias("high_price"),
    F.col("low").cast(DoubleType()).alias("low_price"),
    F.col("close").cast(DoubleType()).alias("close_price"),
    F.col("volume").cast(DoubleType()).alias("volume"),
    F.col("status").cast(StringType()).alias("status")
)

# ── Add metadata columns ──────────────────────────────────────────────────────
cleaned_df = cleaned_df \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .withColumn("source", F.lit("polygon.io")) \
    .withColumn("pipeline_version", F.lit("1.0"))

print("✅ Cleaning done")
cleaned_df.display(truncate=False)

# COMMAND ----------

dq_results = []

null_tickers = cleaned_df.filter(F.col("ticker").isNull()).count()
dq_results.append({"check": "No null tickers", "passed": null_tickers == 0, "failed_count": null_tickers})

null_dates = cleaned_df.filter(F.col("trade_date").isNull()).count()
dq_results.append({"check": "No null trade dates", "passed": null_dates == 0, "failed_count": null_dates})

invalid_open = cleaned_df.filter(F.col("open_price") <= 0).count()
dq_results.append({"check": "Open price > 0", "passed": invalid_open == 0, "failed_count": invalid_open})

invalid_close = cleaned_df.filter(F.col("close_price") <= 0).count()
dq_results.append({"check": "Close price > 0", "passed": invalid_close == 0, "failed_count": invalid_close})

invalid_high_low = cleaned_df.filter(F.col("high_price") < F.col("low_price")).count()
dq_results.append({"check": "High price >= Low price", "passed": invalid_high_low == 0, "failed_count": invalid_high_low})

total_records = cleaned_df.count()
distinct_records = cleaned_df.select("ticker", "trade_date").distinct().count()
dq_results.append({"check": "No duplicate ticker+date", "passed": total_records == distinct_records, "failed_count": total_records - distinct_records})

print("=" * 50)
print("DATA QUALITY REPORT")
print("=" * 50)
all_passed = True
for result in dq_results:
    status = "✅ PASS" if result["passed"] else "❌ FAIL"
    print(f"{status} | {result['check']} | Failed records: {result['failed_count']}")
    if not result["passed"]:
        all_passed = False
print("=" * 50)
print(f"Overall: {'✅ ALL CHECKS PASSED' if all_passed else '❌ SOME CHECKS FAILED'}")

# COMMAND ----------

# ── Write to Silver as Delta table on ADLS ────────────────────────────────────
silver_path = f"abfss://{CONTAINER_SILVER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/stock_data/"

cleaned_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("trade_date") \
    .save(silver_path)

print(f"✅ Silver Delta table written to ADLS!")
print(f"   Records written: {cleaned_df.count()}")

# ── Verify by reading back ────────────────────────────────────────────────────
verify_df = spark.read.format("delta").load(silver_path)
print(f"\n✅ Verification - Records in Silver: {verify_df.count()}")

from IPython.display import display
display(verify_df.toPandas())
