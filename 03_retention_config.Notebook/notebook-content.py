# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a97b8068-0342-42ee-b834-abd384020030",
# META       "default_lakehouse_name": "RetentionConfig",
# META       "default_lakehouse_workspace_id": "5b2bb4c7-8207-4b2a-8ef0-fb835358c361",
# META       "known_lakehouses": [
# META         {
# META           "id": "a97b8068-0342-42ee-b834-abd384020030"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 03 — Retention Config & Readiness Report
# <p style="color:orange;">MICROSOFT DISCLAIMER - Information provided in this file is provided "as is" without Warranty Representation or Condition of any kind, either express or implied, including but not limited to conditions or other terms of merchantability and/or fitness for a particular purpose. The user assumes the entire risk as to the accuracy and use of the information produced by this script.</p>
# 
# **Purpose:** Build a `retention_config` Delta table from distinct item types found in the `workspace_inventory` table (produced by notebook 01). Then join against inventory and activity data to show which items exceed their retention period.
# 
# **⚠️ This notebook does NOT delete, archive, or modify any objects.**  
# It is a **read-only readiness report** — it only shows what *would* be flagged.
# 
# **Prerequisites:**
# - Run **notebook 01** first to populate `workspace_inventory`
# - Run **notebook 02** first to populate `activity_last_modified` (optional — enriches the report)
# 
# **Output:**
# - Delta table `retention_config` — one row per item type with retention period in days
# - Delta table `retention_readiness` — readiness report showing items that exceed their configured retention period
# 
# Fully recomputed every time. It reads the current workspace_inventory + activity_last_modified, joins them, and overwrites retention_readiness with a fresh snapshot.


# MARKDOWN ********************

# #### Cell 2 — Parameters

# CELL ********************

# ── Parameters ──
inventory_table   = "workspace_inventory"       # From notebook 01
activity_table    = "activity_last_modified"     # From notebook 02
config_table      = "retention_config"           # Output: one row per item type
readiness_table   = "retention_readiness"        # Output: final joined readiness report

# Retention days are controlled by Notebook 01 Cell 4 (single source of truth).
# This notebook reads retention_period_days from the workspace_inventory table.

print(f"📋 Config table:     {config_table}")
print(f"📋 Readiness table:  {readiness_table}")
print(f"📋 Inventory source: {inventory_table}")
print(f"📋 Activity source:  {activity_table}")
print(f"📋 Retention days:   controlled by Notebook 01 Cell 4")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 3 — Discover Item Types from Inventory

# CELL ********************

# ── Discover all item types from notebook 01's inventory table ──
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, first

spark = SparkSession.builder.getOrCreate()

# Read the inventory table that notebook 01 produced
# Filter out internal Fabric noise while keeping real tables and files:
#   - System tables: Files, Tables, TableMaintenance (auto-created in every lakehouse)
#   - Delta internals: _delta_log/, _stats/, _metadata/, .snappy.parquet under Files/Tables/
df_inv = spark.table(inventory_table).filter(
    ~(
        (col("item_type") == "LakehouseTable") &
        col("item_name").rlike("/(Files|Tables|TableMaintenance)$")
    )
).filter(
    ~(
        (col("item_type") == "LakehouseFile") &
        col("item_name").rlike("/Files/Tables/")
    )
)
total_items = df_inv.count()

# Get distinct item types with counts AND retention_period_days from NB01
df_types = df_inv.groupBy("item_type").agg(
    count("*").alias("item_count"),
    first("retention_period_days").alias("retention_days")
).orderBy(col("item_count").desc())

item_types = df_types.collect()

print(f"📊 Found {len(item_types)} distinct item type(s) across {total_items} items:\n")
for row in item_types:
    print(f"   • {row['item_type']:30s}  ({row['item_count']} items, {row['retention_days']} day retention)")
print(f"\n   Retention days inherited from Notebook 01 Cell 4.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 4 — Create Retention Config Table

# CELL ********************

# ── Create the retention_config Delta table ──
# One row per item type with a retention period in days.
# >>> DEMO: all types set to 2 days so items appear overdue <<<
# >>> PRODUCTION: update each row to the client's actual retention policy <<<

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime

config_rows = []
for row in item_types:
    ret_days = row["retention_days"]
    config_rows.append({
        "item_type":       row["item_type"],
        "retention_days":  ret_days,
        "action":          "flag_only",           # demo: flag only, never delete
        "description":     f"Retain {row['item_type']} items for {ret_days} day(s)",
        "updated_date":    datetime.utcnow().strftime("%Y-%m-%d"),
    })

config_schema = StructType([
    StructField("item_type",      StringType(),  False),
    StructField("retention_days", IntegerType(), False),
    StructField("action",         StringType(),  True),   # flag_only | archive | delete
    StructField("description",    StringType(),  True),
    StructField("updated_date",   StringType(),  True),
])

df_config = spark.createDataFrame(config_rows, schema=config_schema)

df_config.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(config_table)

print(f"✅ Retention config table '{config_table}' created with {len(config_rows)} type(s):\n")
df_config.show(30, truncate=False)
print(f"   📌 Retention days inherited from Notebook 01 Cell 4")
print(f"   📌 To customize: UPDATE {config_table} SET retention_days = <N> WHERE item_type = '<type>'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 5 — Retention Readiness Report

# CELL ********************

# ── Retention Readiness Report ──
# Join inventory + activity data + config to show which items exceed retention.
# Two-pass join: (1) by item_id = artifact_id, (2) by workspace_name + item_name
# COALESCE priority: activity_id > activity_name > fabric_api > created_date
# ⚠️ READ-ONLY — nothing is deleted, archived, or modified.

from pyspark.sql.functions import (
    datediff, current_date, when, col, count, coalesce, lower, trim, lit,
    sum as spark_sum, avg, max as spark_max, min as spark_min, row_number
)
from pyspark.sql.window import Window

# ── Read all three source tables ──
# Filter out internal Fabric noise (system tables + delta storage files)
df_inv = spark.table(inventory_table).filter(
    ~(
        (col("item_type") == "LakehouseTable") &
        col("item_name").rlike("/(Files|Tables|TableMaintenance)$")
    )
).filter(
    ~(
        (col("item_type") == "LakehouseFile") &
        col("item_name").rlike("/Files/Tables/")
    )
)
df_config   = spark.table(config_table)
df_activity = spark.table(activity_table)    # from notebook 02

inv_count  = df_inv.count()
act_count  = df_activity.count()
cfg_count  = df_config.count()

print(f"📋 Source tables:")
print(f"   {inventory_table}:       {inv_count} items")
print(f"   {activity_table}:   {act_count} tracked artifacts")
print(f"   {config_table}:         {cfg_count} type rules")

# ── Strategy 1: ID-based — one row per artifact_id ──
df_act_id = df_activity.groupBy("artifact_id").agg(
    spark_max("last_modified_date").alias("act_date_by_id")
)

# ── Strategy 2: Name + workspace — fallback for ID mismatches ──
df_act_name = df_activity.groupBy(
    lower(trim(col("workspace_name"))).alias("act_ws_key"),
    lower(trim(col("artifact_name"))).alias("act_name_key"),
).agg(
    spark_max("last_modified_date").alias("act_date_by_name")
)

# ── Three-way join: inventory → activity (by ID) → activity (by name) → config ──
df_enriched = df_inv.alias("inv") \
    .join(
        df_act_id,
        col("inv.item_id") == col("artifact_id"),
        "left"
    ) \
    .join(
        df_act_name,
        (lower(trim(col("inv.workspace_name"))) == col("act_ws_key")) &
        (lower(trim(col("inv.item_name"))) == col("act_name_key")),
        "left"
    ) \
    .join(
        df_config.alias("cfg"),
        col("inv.item_type") == col("cfg.item_type"),
        "left"
    )

# ── COALESCE: pick the best date from all sources ──
df_report = df_enriched.select(
    col("inv.workspace_name"),
    col("inv.item_name"),
    col("inv.item_type"),
    col("inv.item_id"),
    col("act_date_by_id"),
    col("act_date_by_name"),
    col("inv.last_modified_date").alias("inventory_modified_date"),
    col("inv.created_date"),
    col("cfg.retention_days"),
).withColumn(
    "last_modified_date",
    coalesce(
        col("act_date_by_id"),            # best: activity events matched by ID
        col("act_date_by_name"),          # second: activity events matched by name
        col("inventory_modified_date"),   # third: Fabric/PBI Scanner API
        col("created_date"),              # last resort: creation date
    )
).withColumn(
    "date_source",
    when(col("act_date_by_id").isNotNull(),          "activity_id_match")
    .when(col("act_date_by_name").isNotNull(),        "activity_name_match")
    .when(col("inventory_modified_date").isNotNull(), "fabric_api")
    .when(col("created_date").isNotNull(),            "created_date")
    .otherwise("none")
).withColumn(
    "reference_date", col("last_modified_date")
).withColumn(
    "days_since_modified",
    datediff(current_date(), col("reference_date"))
).withColumn(
    "exceeds_retention",
    when(
        col("days_since_modified").isNotNull() & col("retention_days").isNotNull(),
        col("days_since_modified") > col("retention_days")
    ).otherwise(None)
).withColumn(
    "days_overdue",
    when(
        col("exceeds_retention") == True,
        col("days_since_modified") - col("retention_days")
    ).otherwise(lit(0))
).withColumn(
    "status",
    when(col("reference_date").isNull(), "⚪ No date — cannot assess")
    .when(col("exceeds_retention") == True, "🔴 EXCEEDS RETENTION")
    .otherwise("🟢 Within retention")
).drop("act_date_by_id", "act_date_by_name", "inventory_modified_date")

# ── Deduplicate: keep exactly one row per unique item ──
# Partition by (item_id, item_type, item_name) to preserve sub-items
# (LakehouseTable, LakehouseFile, etc.) which share the parent's item_id.
# Prefer rows that have a date, then pick the most recent date.
dedup_window = Window.partitionBy("item_id", "item_type", "item_name").orderBy(
    when(col("reference_date").isNotNull(), 0).otherwise(1),
    col("reference_date").desc_nulls_last()
)
df_report = df_report.withColumn("_rank", row_number().over(dedup_window)) \
                     .filter(col("_rank") == 1) \
                     .drop("_rank")

# Cache for reuse
df_report.cache()
total = df_report.count()

# ── Summary counts ──
overdue   = df_report.filter(col("exceeds_retention") == True).count()
within    = df_report.filter(col("exceeds_retention") == False).count()
no_date   = df_report.filter(col("reference_date").isNull()).count()

# ── Date source breakdown ──
source_rows = df_report.groupBy("date_source").count().collect()
source_counts = {r["date_source"]: r["count"] for r in source_rows}

print("═" * 65)
print("  RETENTION READINESS REPORT")
print("  ⚠️  READ-ONLY — No objects will be deleted or modified")
print("═" * 65)
print(f"  Total items:              {total}")
print(f"  🔴 Exceed retention:       {overdue}")
print(f"  🟢 Within retention:       {within}")
print(f"  ⚪ No date (can't assess): {no_date}")
print(f"  Retention period:          per type (from Notebook 01)")
print(f"─" * 65)
print(f"  📅 Date sources (how each item's date was resolved):")
print(f"     Activity events (ID match):   {source_counts.get('activity_id_match', 0)}")
print(f"     Activity events (name match): {source_counts.get('activity_name_match', 0)}")
print(f"     Fabric API:                   {source_counts.get('fabric_api', 0)}")
print(f"     Created date (fallback):      {source_counts.get('created_date', 0)}")
print(f"     No date available:            {source_counts.get('none', 0)}")
print("═" * 65)

# ── Items that EXCEED retention (uncomment to display) ──
# print(f"\n🔴 ITEMS EXCEEDING RETENTION ({overdue}):")
# print("   These items have not been modified within the retention window.\n")
# df_report.filter(col("exceeds_retention") == True) \
#     .select("workspace_name", "item_name", "item_type",
#             "last_modified_date", "date_source",
#             "days_since_modified", "retention_days", "days_overdue") \
#     .orderBy(col("days_overdue").desc()) \
#     .show(100, truncate=False)

# ── Items WITHIN retention (uncomment to display) ──
# print(f"🟢 ITEMS WITHIN RETENTION ({within}):")
# df_report.filter(col("exceeds_retention") == False) \
#     .select("workspace_name", "item_name", "item_type",
#             "last_modified_date", "date_source",
#             "days_since_modified", "retention_days") \
#     .orderBy("item_type", "workspace_name") \
#     .show(100, truncate=False)

# ── Items with no date (uncomment to display) ──
# if no_date > 0:
#     print(f"⚪ ITEMS WITH NO MODIFICATION DATE ({no_date}):")
#     print("   Cannot assess retention — no date available from any source.\n")
#     df_report.filter(col("reference_date").isNull()) \
#         .select("workspace_name", "item_name", "item_type") \
#         .orderBy("item_type", "workspace_name") \
#         .show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 6 — Summary by Type & Workspace

# CELL ********************

# ── Summary by Item Type ──
print("📊 RETENTION SUMMARY BY ITEM TYPE:\n")
df_report.groupBy("item_type").agg(
    count("*").alias("total"),
    spark_sum(when(col("exceeds_retention") == True, 1).otherwise(0)).alias("overdue"),
    spark_sum(when(col("exceeds_retention") == False, 1).otherwise(0)).alias("within"),
    spark_sum(when(col("reference_date").isNull(), 1).otherwise(0)).alias("no_date"),
    spark_max("days_since_modified").alias("max_days_inactive"),
    avg("days_since_modified").cast("int").alias("avg_days_inactive"),
).orderBy(col("overdue").desc()).show(30, truncate=False)

# ── Summary by Workspace ──
print("📊 RETENTION SUMMARY BY WORKSPACE:\n")
df_report.groupBy("workspace_name").agg(
    count("*").alias("total"),
    spark_sum(when(col("exceeds_retention") == True, 1).otherwise(0)).alias("overdue"),
    spark_sum(when(col("exceeds_retention") == False, 1).otherwise(0)).alias("within"),
    spark_sum(when(col("reference_date").isNull(), 1).otherwise(0)).alias("no_date"),
).orderBy(col("overdue").desc()).show(30, truncate=False)

# ── Config table reference ──
print("📋 CURRENT RETENTION CONFIG:")
spark.table(config_table).show(30, truncate=False)

print(f"\n{'═'*65}")
print(f"  ✅ Report complete — NO objects were deleted or modified")
print(f"  📌 To adjust retention: UPDATE {config_table} SET retention_days = <N> WHERE item_type = '<type>'")
print(f"  📌 Retention days are set in Notebook 01 Cell 4 (single source of truth)")
print(f"{'═'*65}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 7 — Save Readiness Table

# CELL ********************

# ── Save the readiness report as a Delta table ──
# This is the FINAL output — one row per item with its retention status.
# ⚠️ Still read-only: no objects are deleted or modified.

df_save = df_report.select(
    "workspace_name",
    "item_name",
    "item_type",
    "last_modified_date",
    "retention_days",
    "days_since_modified",
    "days_overdue",
    "status",
    "date_source",
    "created_date",
    current_date().alias("report_date"),
    "item_id",
)

df_save.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(readiness_table)

saved_count = df_save.count()
overdue_count = df_save.filter(col("status").contains("EXCEEDS")).count()

print(f"✅ Readiness table '{readiness_table}' saved with {saved_count} item(s)")
print(f"   🔴 {overdue_count} item(s) exceed retention")
print(f"   Report date: {datetime.utcnow().strftime('%Y-%m-%d')}")
print(f"\n   Query it anytime:")
print(f"   SELECT * FROM {readiness_table} ORDER BY days_overdue DESC")
print(f"   SELECT * FROM {readiness_table} WHERE status LIKE '%EXCEEDS%'")
print(f"   SELECT date_source, COUNT(*) FROM {readiness_table} GROUP BY date_source")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 8 - create a final readiness report Excel file for export

# MARKDOWN ********************

# ####
# Export to file and overwrite for Excel analysis.
# Then to download it:
# 
# Go to your RetentionConfig lakehouse in the Fabric UI
# Navigate to Files → exports
# Right-click retention_readiness_report.xlsx → Download


# CELL ********************

# ── Export retention_readiness to Excel ──
df_readiness = spark.table("retention_readiness")

# Convert to pandas, then write to Excel
pdf = df_readiness.orderBy("status", "days_overdue").toPandas()
output_path = "/lakehouse/default/Files/exports/retention_readiness_report.xlsx"

import os
os.makedirs(os.path.dirname(output_path), exist_ok=True)
pdf.to_excel(output_path, index=False, sheet_name="Retention Readiness")

print(f"✅ Exported {len(pdf)} rows to {output_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Stop Session

# CELL ********************

# ── Stop Spark session to release compute resources ──
print("🛑 Stopping Spark session...")
mssparkutils.session.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
