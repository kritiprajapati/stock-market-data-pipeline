# Databricks notebook source
# MAGIC %pip install azure-storage-blob

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.window import Window
import datetime

# ── Configuration ─────────────────────────────────────────────────────────────
STORAGE_ACCOUNT = "stockpipelinedl"
STORAGE_KEY = dbutils.secrets.get(scope="stock-pipeline", key="azure-storage-key")
CONTAINER_SILVER = "silver"
CONTAINER_GOLD = "gold"

# ── Dynamic date ──────────────────────────────────────────────────────────────
try:
    DATE = dbutils.widgets.get("execution_date")
except:
    DATE = "2026-05-08"
    # DATE = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

print(f"Processing date: {DATE}")

# ── Connect Spark to Azure Storage ────────────────────────────────────────────
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)

print("✅ Spark connected to Azure Storage")

# COMMAND ----------

# ── Read entire Silver Delta table (all dates) ────────────────────────────────
silver_path = f"abfss://{CONTAINER_SILVER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/stock_data/"

silver_df = spark.read.format("delta").load(silver_path)

print(f"✅ Read {silver_df.count()} total records from Silver")
print(f"   Dates available: {silver_df.select('trade_date').distinct().count()}")
print("\nSample data:")
silver_df.display(truncate=False)

# COMMAND ----------

# ── Define window for calculations ────────────────────────────────────────────
# Window partitioned by ticker, ordered by date
# This means calculations happen per stock, in date order
ticker_window = Window.partitionBy("ticker").orderBy("trade_date")

# Window for 7-day moving average
# rows between 6 preceding and current row = last 7 days including today
moving_avg_window = Window.partitionBy("ticker") \
    .orderBy("trade_date") \
    .rowsBetween(-6, 0)

# ── Calculate metrics ─────────────────────────────────────────────────────────
gold_df = silver_df \
    .withColumn(
        "prev_close",
        F.lag("close_price", 1).over(ticker_window)
    ) \
    .withColumn(
        "daily_return_pct",
        F.round(
            ((F.col("close_price") - F.col("prev_close")) / F.col("prev_close")) * 100,
            2
        )
    ) \
    .withColumn(
        "moving_avg_7day",
        F.round(F.avg("close_price").over(moving_avg_window), 2)
    ) \
    .withColumn(
        "price_volatility",
        F.round(
            F.col("high_price") - F.col("low_price"),
            2
        )
    ) \
    .withColumn(
        "price_range_pct",
        F.round(
            ((F.col("high_price") - F.col("low_price")) / F.col("low_price")) * 100,
            2
        )
    ) \
    .withColumn("processed_timestamp", F.current_timestamp())

# ── Select final Gold columns ─────────────────────────────────────────────────
gold_df = gold_df.select(
    "ticker",
    "trade_date",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "daily_return_pct",
    "moving_avg_7day",
    "price_volatility",
    "price_range_pct",
    "processed_timestamp"
)

print(f"✅ Gold metrics calculated!")
print(f"   Records: {gold_df.count()}")
print("\nGold data:")
gold_df.display(truncate=False)

# COMMAND ----------

# ── Write to Gold as Delta table ──────────────────────────────────────────────
gold_path = f"abfss://{CONTAINER_GOLD}@{STORAGE_ACCOUNT}.dfs.core.windows.net/stock_data/"

gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("trade_date") \
    .save(gold_path)

print(f"✅ Gold Delta table written to ADLS!")
print(f"   Path: {gold_path}")
print(f"   Records written: {gold_df.count()}")

# ── Verify by reading back ────────────────────────────────────────────────────
verify_df = spark.read.format("delta").load(gold_path)
print(f"\n✅ Verification - Records in Gold: {verify_df.count()}")

from IPython.display import display
display(verify_df.toPandas())
