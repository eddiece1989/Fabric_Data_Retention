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

# # 02 — Activity Log Collector
# **Purpose:** Incrementally collect Fabric/Power BI activity events via the Admin ActivityEvents API and land them in a Delta table. Computes a `last_modified_summary` view per item.
# 
# **Why this notebook exists:**
# - The Fabric REST API (`/v1/workspaces/{id}/items`) returns **no timestamp fields** on items
# - The PBI Admin Scanner only covers Reports, Datasets, Dataflows, Dashboards, Datamarts
# - The ActivityEvents API has a **28-day retention window** — if you don't collect regularly, you lose history
# - This notebook runs incrementally: it knows which days it has already collected and only fetches new ones
# 
# **Output:**
# - Delta table `activity_events_raw` — every modification event, one row per event
# - Delta table `activity_last_modified` — computed summary: one row per item with its last modification date
# 
# **Schedule:** Run daily (or at least every 28 days) to maintain continuous coverage.
# 
# **References:**
# - [ActivityEvents API](https://learn.microsoft.com/en-us/rest/api/power-bi/admin/get-activity-events)
# - [Activity log guidance](https://learn.microsoft.com/en-us/fabric/enterprise/powerbi/service-admin-auditing)


# MARKDOWN ********************

# ####
# This is the truly incremental notebook. It appends raw events to activity_events_raw, preserving history beyond the 28-day API window. If you run it every week, it only fetches the new 7 days.
# Look at Cell 6 — it checks which days are already in activity_events_raw, then only fetches days it hasn't collected yet. Cell 8 writes with .mode("append"), so each run adds new days without touching old data. The activity_last_modified summary table (Cell 10) is then recomputed from all raw data with .mode("overwrite").


# MARKDOWN ********************

# #### Cell 2 — Parameters

# CELL ********************

# ── Parameters ──
config_lakehouse_path = "/lakehouse/default"        # Mount path to RetentionConfig lakehouse
raw_table_name        = "activity_events_raw"        # Delta table for raw events
summary_table_name    = "activity_last_modified"     # Delta table for per-item summary
lookback_days         = 28                           # API retention window (max 28 days, use 28)

# Set to True to re-fetch ALL days (drops existing raw table).
# After running once with True, set back to False for incremental collection.
force_refetch         = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 3 — Imports & Utilities

# CELL ********************

# ── Imports & Utilities ──
import json
import requests
import time
from datetime import datetime, timedelta, date
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DateType, TimestampType
)
from pyspark.sql.functions import (
    col, lit, max as spark_max, min as spark_min,
    count, countDistinct, to_date, current_date
)
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()


def safe_parse_datetime(dt_string):
    """Parse various datetime formats from Fabric/PBI API responses."""
    if not dt_string:
        return None
    try:
        cleaned = dt_string.replace("Z", "+00:00")
        return datetime.fromisoformat(cleaned).replace(tzinfo=None)
    except Exception:
        try:
            return datetime.strptime(dt_string[:19], "%Y-%m-%dT%H:%M:%S")
        except Exception:
            return None


print("✅ Imports loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 4 — Read-Only Activities Filter

# CELL ********************

# ── Define read-only activities to EXCLUDE ──
# Any event NOT in this set is treated as a potential modification.
# This ensures views, exports, and reads don't reset an item's "last modified" date.

READ_ONLY_ACTIVITIES = {
    # View / Read
    'ViewReport', 'ViewDashboard', 'ViewDataset', 'ViewDataflow',
    'ViewUsageMetrics', 'ViewUsageMetricsReport', 'ViewArtifact',
    # Get / Fetch metadata
    'GetReport', 'GetDashboard', 'GetDataset', 'GetDatasources',
    'GetRefreshHistory', 'GetRefreshSchedule', 'GetGroupUsers',
    'GetWorkspaces', 'GetWorkspace', 'GetSnapshots',
    'GetArtifactAccessRequestsResponseList',
    # Export / Download
    'ExportReport', 'ExportDataflow', 'ExportArtifact',
    'ExportActivityEvents', 'DownloadReport',
    # Share (read-only from item perspective)
    'ShareReport', 'ShareDashboard',
    # Render / Screenshot
    'PrintReport', 'GenerateScreenshot',
    # Token / Auth
    'GenerateCustomVisualAADAccessToken',
    # Analyze / External
    'AnalyzeInExcel', 'AnalyzedByExternalApplication',
    # Discovery
    'DiscoverSuggestedPeople', 'DiscoverSuggestedArtifacts',
}

print(f"📋 {len(READ_ONLY_ACTIVITIES)} read-only activity types will be excluded")
print(f"   Only MODIFICATION events are stored in the raw table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 5 — Workspace Lookup (Fabric + Admin Groups APIs)

# CELL ********************

# ── Build workspace lookup (ID → Name) from Fabric REST API + PBI Admin Groups ──
# The ActivityEvents API often returns WorkSpaceId but NOT WorkspaceName.
# We resolve names from two sources:
#   1. Fabric REST API — covers shared/org workspaces
#   2. PBI Admin Groups API — covers personal workspaces (type = 'PersonalGroup')

print("📡 Step 1: Listing org workspaces via Fabric REST API...\n")

fabric_token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com")
fabric_headers = {
    "Authorization": f"Bearer {fabric_token}",
    "Content-Type": "application/json"
}

workspace_lookup = {}  # workspace_id -> workspace_name
continuation_url = "https://api.fabric.microsoft.com/v1/workspaces"

while continuation_url:
    resp = requests.get(continuation_url, headers=fabric_headers)
    resp.raise_for_status()
    data = resp.json()
    for ws in data.get("value", []):
        workspace_lookup[ws["id"]] = ws["displayName"]
    continuation_url = data.get("continuationUri") or data.get("@odata.nextLink")

org_count = len(workspace_lookup)
print(f"   Found {org_count} org/shared workspace(s)")

# ── Step 2: Personal workspaces via PBI Admin Groups API ──
print(f"\n📡 Step 2: Listing personal workspaces via PBI Admin Groups API...")

pbi_token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")
pbi_headers = {
    "Authorization": f"Bearer {pbi_token}",
    "Content-Type": "application/json"
}

personal_count = 0
try:
    # Fetch personal workspaces (type = 'PersonalGroup')
    # The API pages with $top and $skip
    skip = 0
    page_size = 5000
    while True:
        groups_url = (
            f"https://api.powerbi.com/v1.0/myorg/admin/groups"
            f"?$top={page_size}&$skip={skip}"
            f"&$filter=type eq 'PersonalGroup'"
            f"&$expand=users"
        )
        resp = requests.get(groups_url, headers=pbi_headers)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 30))
            print(f"   ⏳ Rate-limited — waiting {retry_after}s...")
            time.sleep(retry_after)
            continue
        resp.raise_for_status()
        data = resp.json()
        groups = data.get("value", [])
        if not groups:
            break
        for grp in groups:
            ws_id = grp.get("id", "")
            if ws_id and ws_id not in workspace_lookup:
                # Find the owner from the users array
                users = grp.get("users", [])
                owner = ""
                for u in users:
                    if u.get("groupUserAccessRight") == "Admin":
                        owner = u.get("emailAddress") or u.get("displayName") or u.get("identifier") or ""
                        break
                if not owner and users:
                    owner = users[0].get("emailAddress") or users[0].get("displayName") or ""

                # >>> FOR DEMO: hide owner ID. Uncomment the next line for production: <<<
                # ws_name = f"My workspace ({owner})" if owner else "My workspace (unknown)"
                ws_name = "My workspace"

                workspace_lookup[ws_id] = ws_name
                personal_count += 1
        skip += page_size
        # If fewer results than page_size, we've reached the end
        if len(groups) < page_size:
            break
except Exception as e:
    print(f"   ⚠️ Could not fetch personal workspaces: {e}")
    print(f"   Continuing with Fabric API workspaces only")

print(f"   Found {personal_count} personal workspace(s)")

# ── Summary ──
print(f"\n✅ Workspace lookup built: {len(workspace_lookup)} total")
print(f"   Org/shared:  {org_count}")
print(f"   Personal:    {personal_count}")
print()
for ws_id, ws_name in sorted(workspace_lookup.items(), key=lambda x: x[1]):
    print(f"   • {ws_name}  ({ws_id})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 6 — Incremental Check (Days Already Collected)

# CELL ********************

# ── Determine which days need to be collected ──
# Check what's already in the raw table to avoid re-fetching

now = datetime.utcnow()
earliest_available = (now - timedelta(days=lookback_days)).date()

already_collected = set()

# ── Force re-fetch: drop existing raw table so we start clean ──
if force_refetch:
    print("🔄 force_refetch = True — dropping existing raw table for clean re-collection")
    try:
        spark.sql(f"DROP TABLE IF EXISTS {raw_table_name}")
        print(f"   Dropped '{raw_table_name}'")
    except Exception as e:
        print(f"   Table didn't exist or couldn't drop: {e}")
    already_collected = set()
else:
    try:
        existing_df = spark.table(raw_table_name)
        collected_dates = existing_df.select("event_date") \
            .distinct() \
            .collect()
        already_collected = {row["event_date"] for row in collected_dates}
        print(f"📊 Raw table '{raw_table_name}' exists with {existing_df.count()} rows")
        print(f"   Days already collected: {len(already_collected)}")
    except Exception:
        print(f"📊 Raw table '{raw_table_name}' does not exist yet — will create it")

# Build list of days to fetch (only days within the API retention window that we haven't collected)
days_to_fetch = []
check_day = earliest_available
while check_day < now.date():
    if check_day not in already_collected:
        days_to_fetch.append(check_day)
    check_day += timedelta(days=1)

print(f"\n📅 API retention window: {earliest_available} to {now.date()}")
print(f"   Days available:          {lookback_days}")
print(f"   Days already collected:  {len(already_collected)}")
print(f"   Days to fetch:           {len(days_to_fetch)}")

if not days_to_fetch:
    print("\n✅ All available days already collected — nothing to fetch!")
else:
    print(f"\n   Fetching: {days_to_fetch[0]} → {days_to_fetch[-1]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 7 — Fetch Activity Events from API

# CELL ********************

# ── Fetch activity events from the API ──
# One-day window per call, continuation tokens for paging, 28-day retention
# Workspace names resolved: API field → Fabric/Admin lookup → UserId fallback
#
# FIELD EXTRACTION STRATEGY:
#   ArtifactId: ArtifactId → artifactId → parse GUID from ObjectId URL → ItemId
#   ArtifactName: ArtifactName → artifactName → ObjectDisplayName → DisplayName → ItemName
#   This ensures Fabric-native items (notebooks, pipelines, lakehouses) are captured
#   even when the API uses different field names than classic PBI items.

import re

# Regex to extract a GUID from an ObjectId URL like:
#   https://app.fabric.microsoft.com/groups/{ws_id}/items/{item_id}
#   https://app.powerbi.com/groups/{ws_id}/reports/{item_id}
GUID_PATTERN = re.compile(r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}')


def extract_artifact_id(event):
    """Extract artifact ID from event, trying multiple field names and ObjectId URL parsing."""
    # Direct field names
    art_id = event.get("ArtifactId") or event.get("artifactId") or ""
    if art_id:
        return art_id

    # Try ItemId (used by some Fabric activities)
    art_id = event.get("ItemId") or event.get("itemId") or ""
    if art_id:
        return art_id

    # Try DatasetId / ReportId / DashboardId (PBI-specific)
    art_id = (event.get("DatasetId") or event.get("ReportId")
              or event.get("DashboardId") or event.get("DataflowId") or "")
    if art_id:
        return art_id

    # Try parsing GUID from ObjectId URL
    object_id = event.get("ObjectId") or event.get("objectId") or ""
    if object_id:
        guids = GUID_PATTERN.findall(object_id)
        if len(guids) >= 2:
            # URL format: .../groups/{ws_id}/items/{item_id} — take the LAST GUID
            return guids[-1]
        elif len(guids) == 1:
            return guids[0]

    return ""


def extract_artifact_name(event):
    """Extract artifact name from event, trying multiple field names."""
    return (event.get("ArtifactName") or event.get("artifactName")
            or event.get("ObjectDisplayName") or event.get("DisplayName")
            or event.get("displayName") or event.get("ItemName")
            or event.get("itemName") or event.get("ObjectName")
            or event.get("objectName") or "")


if not days_to_fetch:
    print("⏭️  No new days to fetch — skipping API calls")
    new_events = []
else:
    pbi_token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")
    pbi_headers = {
        "Authorization": f"Bearer {pbi_token}",
        "Content-Type": "application/json"
    }

    new_events = []
    total_raw_events = 0
    skipped_readonly = 0
    skipped_no_id = 0       # events dropped because no artifact_id could be extracted
    recovered_by_fallback = 0  # events saved by fallback field extraction
    fetch_errors = []


    for day_idx, fetch_date in enumerate(days_to_fetch):
        day_start = datetime.combine(fetch_date, datetime.min.time())
        day_end   = datetime.combine(fetch_date, datetime.max.time()).replace(microsecond=0)

        s_str = day_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        e_str = day_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        events_url = (
            f"https://api.powerbi.com/v1.0/myorg/admin/activityevents"
            f"?startDateTime='{s_str}'&endDateTime='{e_str}'"
        )

        try:
            page_url = events_url
            day_events = 0
            day_skipped = 0

            while page_url:
                resp = requests.get(page_url, headers=pbi_headers)

                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 30))
                    print(f"   ⏳ Rate-limited — waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue

                if resp.status_code != 200:
                    if day_idx == 0:
                        print(f"   ⚠️ HTTP {resp.status_code} for {fetch_date}: {resp.text[:300]}")
                    fetch_errors.append(f"HTTP {resp.status_code} on {fetch_date}")
                    break

                data = resp.json()

                for event in data.get("activityEventEntities", []):
                    activity = event.get("Activity") or event.get("activity") or ""
                    ev_ts    = event.get("CreationTime") or event.get("creationTime") or ""

                    if not ev_ts:
                        continue

                    # ── Enhanced artifact_id extraction with fallbacks ──
                    direct_id = event.get("ArtifactId") or event.get("artifactId") or ""
                    artifact_id = extract_artifact_id(event)

                    if not artifact_id:
                        skipped_no_id += 1
                        continue

                    if not direct_id and artifact_id:
                        recovered_by_fallback += 1

                    total_raw_events += 1

                    # Skip read-only events
                    if activity in READ_ONLY_ACTIVITIES:
                        skipped_readonly += 1
                        day_skipped += 1
                        continue

                    ev_time = safe_parse_datetime(ev_ts)
                    if not ev_time:
                        continue

                    day_events += 1

                    # Resolve workspace name (3-tier fallback):
                    #   1. API response field (sometimes populated)
                    #   2. workspace_lookup from Fabric + Admin Groups APIs
                    #   3. "My workspace" as last resort (with or without user_id)
                    ws_id   = (event.get("WorkSpaceId") or event.get("workSpaceId")
                               or event.get("WorkspaceId") or "")
                    user_id = (event.get("UserId") or event.get("userId") or "")
                    ws_name = (event.get("WorkspaceName") or event.get("workspaceName") or "")
                    if not ws_name and ws_id:
                        ws_name = workspace_lookup.get(ws_id, "")
                    if not ws_name:
                        # >>> FOR DEMO: hide user ID. Uncomment next line for production: <<<
                        # ws_name = f"My workspace ({user_id})" if user_id else "My workspace"
                        ws_name = "My workspace"

                    artifact_name = extract_artifact_name(event)

                    new_events.append({
                        "event_date":     fetch_date,
                        "event_timestamp": ev_time,
                        "activity":       activity,
                        "artifact_id":    artifact_id,
                        "artifact_name":  artifact_name,
                        "artifact_type":  (event.get("ItemType") or event.get("itemType")
                                           or event.get("ArtifactType") or event.get("artifactType") or ""),
                        "workspace_id":   ws_id,
                        "workspace_name": ws_name,
                        "user_id":        user_id,
                        "collection_date": now.date(),
                    })

                page_url = data.get("continuationUri")

        except Exception as e:
            fetch_errors.append(f"Error on {fetch_date}: {e}")

        # Progress update every 5 days
        if (day_idx + 1) % 5 == 0 or day_idx == len(days_to_fetch) - 1:
            print(f"   📅 Fetched {day_idx + 1}/{len(days_to_fetch)} days "
                  f"({len(new_events)} modification events so far)")

    print(f"\n{'═'*50}")
    print(f"✅ API fetch complete:")
    print(f"   Days fetched:             {len(days_to_fetch)}")
    print(f"   Total raw events:         {total_raw_events}")
    print(f"   Read-only (excluded):     {skipped_readonly}")
    print(f"   Modification events kept: {len(new_events)}")
    print(f"   Recovered by fallback:    {recovered_by_fallback}")
    print(f"   Dropped (no artifact_id): {skipped_no_id}")

    # Check workspace name resolution
    named = sum(1 for e in new_events if e["workspace_name"])
    unnamed = sum(1 for e in new_events if not e["workspace_name"])
    print(f"   Workspace names resolved: {named}")
    if unnamed:
        print(f"   ⚠️ Missing workspace name: {unnamed}")

    if fetch_errors:
        print(f"\n   ⚠️ Errors ({len(fetch_errors)}):")
        for e in fetch_errors[:5]:
            print(f"      • {e}")

    # ════════════════════════════════════════════════════════════
    #  DIAGNOSTIC: Show dropped events (no artifact_id found)
    # ════════════════════════════════════════════════════════════

    if skipped_no_id > 0:
        print(f"\n⚠️ {skipped_no_id} events skipped — no artifact_id found")
    else:
        print(f"\n✅ No events were dropped — all had artifact_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 8 — Write Raw Delta Table (Append)

# CELL ********************

# ── Write new events to the raw Delta table (append) ──

raw_schema = StructType([
    StructField("event_date",       DateType(),      True),
    StructField("event_timestamp",  TimestampType(), True),
    StructField("activity",         StringType(),    True),
    StructField("artifact_id",      StringType(),    True),
    StructField("artifact_name",    StringType(),    True),
    StructField("artifact_type",    StringType(),    True),
    StructField("workspace_id",     StringType(),    True),
    StructField("workspace_name",   StringType(),    True),
    StructField("user_id",          StringType(),    True),
    StructField("collection_date",  DateType(),      True),
])

if new_events:
    df_new = spark.createDataFrame(new_events, schema=raw_schema)

    df_new.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(raw_table_name)

    print(f"✅ Appended {len(new_events)} new modification events to '{raw_table_name}'")
else:
    print(f"ℹ️  No new events to append")

# Show totals in the raw table
df_raw = spark.table(raw_table_name)
total_rows = df_raw.count()
date_range = df_raw.select(
    spark_min("event_date").alias("earliest"),
    spark_max("event_date").alias("latest"),
    countDistinct("event_date").alias("days_covered"),
    countDistinct("artifact_id").alias("unique_items"),
).collect()[0]

print(f"\n📊 Raw table totals:")
print(f"   Total rows:      {total_rows}")
print(f"   Date range:      {date_range['earliest']} → {date_range['latest']}")
print(f"   Days covered:    {date_range['days_covered']}")
print(f"   Unique items:    {date_range['unique_items']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 9 — Backfill Workspace Names

# CELL ********************

# ── Backfill workspace names for existing rows ──
# Updates rows that have workspace_id but no workspace_name.
# Two passes: (1) lookup from Fabric + Admin APIs, (2) "My workspace" fallback

df_raw = spark.table(raw_table_name)
missing_count = df_raw.filter(
    (col("workspace_id") != "") &
    ((col("workspace_name").isNull()) | (col("workspace_name") == ""))
).count()

if missing_count > 0:
    print(f"🔄 Backfilling {missing_count} row(s) with missing workspace_name...\n")

    # ── Pass 1: MERGE using workspace_lookup (Fabric + Admin Groups) ──
    ws_rows = [{"workspace_id": k, "ws_name_lookup": v} for k, v in workspace_lookup.items()]
    if ws_rows:
        df_ws = spark.createDataFrame(ws_rows)
        dt = DeltaTable.forName(spark, raw_table_name)
        dt.alias("raw").merge(
            df_ws.alias("ws"),
            "raw.workspace_id = ws.workspace_id AND (raw.workspace_name IS NULL OR raw.workspace_name = '')"
        ).whenMatchedUpdate(set={
            "workspace_name": col("ws.ws_name_lookup")
        }).execute()
        print("   Pass 1: workspace lookup MERGE complete")

    # ── Pass 2: Fallback — label remaining blanks as "My workspace" ──
    still_missing = spark.table(raw_table_name).filter(
        (col("workspace_id") != "") &
        ((col("workspace_name").isNull()) | (col("workspace_name") == ""))
    ).count()

    if still_missing > 0:
        print(f"   Pass 2: {still_missing} rows still missing — applying fallback...")
        from pyspark.sql.functions import concat, when

        dt = DeltaTable.forName(spark, raw_table_name)

        # >>> FOR DEMO: hide user ID. Uncomment next block and comment out the one after for production: <<<
        # dt.update(
        #     condition="(workspace_name IS NULL OR workspace_name = '') AND user_id IS NOT NULL AND user_id != ''",
        #     set={
        #         "workspace_name": concat(lit("My workspace ("), col("user_id"), lit(")"))
        #     }
        # )

        dt.update(
            condition="(workspace_name IS NULL OR workspace_name = '')",
            set={
                "workspace_name": lit("My workspace")
            }
        )
        print("   Pass 2: fallback UPDATE complete")

    # ── Verify ──
    final_missing = spark.table(raw_table_name).filter(
        (col("workspace_id") != "") &
        ((col("workspace_name").isNull()) | (col("workspace_name") == ""))
    ).count()

    print(f"\n✅ Backfill complete:")
    print(f"   Rows fixed:             {missing_count - final_missing}")
    if final_missing:
        print(f"   ⚠️ Still missing names:  {final_missing}  (no workspace_id or user_id)")
else:
    print(f"✅ All rows already have workspace_name — no backfill needed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 10 — Compute Per-Item Summary Table

# CELL ********************

# ── Compute per-item last_modified summary ──
# This aggregates ALL collected history (not just today's fetch) to produce
# one row per artifact with its earliest and most recent modification date.

df_raw = spark.table(raw_table_name)

df_summary = df_raw.groupBy(
    "artifact_id", "artifact_name", "artifact_type",
    "workspace_id", "workspace_name"
).agg(
    spark_min("event_timestamp").alias("first_modified_date"),
    spark_max("event_timestamp").alias("last_modified_date"),
    count("*").alias("modification_count"),
    spark_max("event_date").alias("latest_event_date"),
)

# Cast timestamps to dates for the summary table
df_summary = df_summary.withColumn("first_modified_date", to_date("first_modified_date")) \
                        .withColumn("last_modified_date", to_date("last_modified_date"))

# Overwrite the summary table each run (it's a full recompute from raw)
df_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(summary_table_name)

summary_count = df_summary.count()
print(f"✅ Summary table '{summary_table_name}' written with {summary_count} item(s)")
print(f"   Each row = one artifact with its last modification date\n")

df_summary.orderBy("last_modified_date", ascending=False).show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 11 — Summary Report

# CELL ********************

# ── Summary Report ──
from pyspark.sql.functions import avg, sum as spark_sum

df_summary = spark.table(summary_table_name)
total_items = df_summary.count()

print("═"*60)
print("  ACTIVITY LOG COLLECTION REPORT")
print("═"*60)
print(f"  Run Date:           {now.strftime('%Y-%m-%d %H:%M')} UTC")
print(f"  New events fetched: {len(new_events) if 'new_events' in dir() else 0}")
print(f"  Items tracked:      {total_items}")
print("═"*60)

# Coverage by item type
print(f"\n📊 Items by Type:")
df_summary.groupBy("artifact_type").agg(
    count("*").alias("items"),
    spark_max("last_modified_date").alias("most_recent_modification"),
    avg("modification_count").cast("int").alias("avg_modifications"),
).orderBy(col("items").desc()).show(30, truncate=False)

# Coverage by workspace
print(f"📊 Items by Workspace:")
df_summary.groupBy("workspace_name").agg(
    count("*").alias("items"),
    spark_max("last_modified_date").alias("most_recent_modification"),
).orderBy("workspace_name").show(30, truncate=False)

# Raw table health
df_raw = spark.table(raw_table_name)
raw_stats = df_raw.select(
    spark_min("event_date").alias("earliest"),
    spark_max("event_date").alias("latest"),
    countDistinct("event_date").alias("days"),
    count("*").alias("total_events"),
).collect()[0]

print(f"📦 Raw Table Health:")
print(f"   Earliest event:   {raw_stats['earliest']}")
print(f"   Latest event:     {raw_stats['latest']}")
print(f"   Days collected:   {raw_stats['days']}")
print(f"   Total events:     {raw_stats['total_events']}")
print(f"\n   💡 Run this notebook daily to maintain continuous coverage")
print(f"   💡 The API only retains 28 days — missed days are lost forever")

# Activity distribution (which event types are most common?)
print(f"\n📊 Top 15 Modification Activity Types:")
df_raw.groupBy("activity").agg(
    count("*").alias("event_count"),
    countDistinct("artifact_id").alias("unique_items"),
).orderBy(col("event_count").desc()).show(15, truncate=False)

print(f"\n✅ Collection complete. Query these tables:")
print(f"   SELECT * FROM {raw_table_name}           -- all modification events")
print(f"   SELECT * FROM {summary_table_name}   -- per-item last modified date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 12 — Ad-hoc Queries

# CELL ********************

# ── Ad-hoc Queries ──

# Use this query to drop the table getting appended for the last 28 days when you want to start over
#spark.sql("DROP TABLE IF EXISTS activity_events_raw")
#print("✅ Dropped activity_events_raw")

# Total rows per collection_date — proves the append
# View activities_event_raw delta table

# spark.sql("""
#     SELECT collection_date, COUNT(*) as row_count
#     FROM activity_events_raw
#     GROUP BY collection_date
#     ORDER BY collection_date
# """).show()

# See some of the NEW rows specifically
# spark.sql("""
#     SELECT event_date, collection_date, activity, artifact_name, workspace_name
#     FROM activity_events_raw
#     WHERE collection_date = (SELECT MAX(collection_date) FROM activity_events_raw)
#     ORDER BY event_date DESC
# """).show(50, truncate=False)





# Show the full summary table
# df_inv = spark.sql(f"""
#    SELECT workspace_name, artifact_name, artifact_type,
#           last_modified_date, modification_count
#    FROM {summary_table_name}
#    ORDER BY last_modified_date DESC
# """)
# df_inv.show(100, truncate=False)

# Uncomment any of the queries below as needed:

# -- Items modified in the last 7 days --
# spark.sql(f"""
#     SELECT workspace_name, artifact_name, artifact_type,
#            last_modified_date, modification_count
#     FROM {summary_table_name}
#     WHERE last_modified_date >= current_date() - INTERVAL 7 DAYS
#     ORDER BY last_modified_date DESC
# """).show(100, truncate=False)

# -- Items NOT modified in the last 14 days (stale?) --
# spark.sql(f"""
#     SELECT workspace_name, artifact_name, artifact_type,
#            last_modified_date, modification_count
#     FROM {summary_table_name}
#     WHERE last_modified_date < current_date() - INTERVAL 14 DAYS
#     ORDER BY last_modified_date ASC
# """).show(100, truncate=False)

# -- Raw events for a specific item --
# spark.sql(f"""
#     SELECT event_date, event_timestamp, activity, user_id
#     FROM {raw_table_name}
#     WHERE artifact_name = 'YOUR_ITEM_NAME'
#     ORDER BY event_timestamp DESC
# """).show(100, truncate=False)

# -- Daily event volume (useful to verify collection is working) --
# spark.sql(f"""
#     SELECT event_date,
#            COUNT(*) AS events,
#            COUNT(DISTINCT artifact_id) AS unique_items
#     FROM {raw_table_name}
#     GROUP BY event_date
#     ORDER BY event_date DESC
# """).show(30, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
