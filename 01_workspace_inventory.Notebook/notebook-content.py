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

# # 01 — Workspace Inventory & Retention Readiness
# **Purpose:** Scan all accessible workspaces, catalog every object, calculate retention age, and flag items overdue for deletion.  
# **Output:** Delta table `workspace_inventory` in the Data_Retention_Reporting_demo workspace,RetentionConfig lakehouse.  
# **Usage:** Can be run independently or as part of a pipeline. This is a read-only visibility report.

# MARKDOWN ********************

# ####
#  Notebook workspace_inventory completes a full scan every time. It calls the Fabric REST API + PBI Scanner + Activity Events API to catalog all items. It overwrites the workspace_inventory table completely on each run. You should re-run this notebook periodically too because it discovers new/removed items.

# MARKDOWN ********************

# 
# #### Cell 2 — Parameters
# ##### Assigns the attached default lakehouse and the destination Delta table to a parameter

# CELL ********************

# ── Parameters ──
config_lakehouse_path = "/lakehouse/default"  # Mount path to RetentionConfig lakehouse
inventory_table_name = "workspace_inventory"   # Delta table name for results

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
from datetime import datetime, timedelta, date
from email.utils import parsedate_to_datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, BooleanType, DateType
)
from pyspark.sql.functions import col, lit, datediff, when

spark = SparkSession.builder.getOrCreate()


def get_fabric_headers():
    token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def safe_parse_datetime(dt_string):
    """Parse various datetime formats from Fabric REST API responses."""
    if not dt_string:
        return None
    # Try ISO 8601 first (most common from Fabric/PBI APIs)
    try:
        cleaned = dt_string.replace("Z", "+00:00")
        return datetime.fromisoformat(cleaned).replace(tzinfo=None)
    except Exception:
        pass
    # Try ISO 8601 without timezone
    try:
        return datetime.strptime(dt_string[:19], "%Y-%m-%dT%H:%M:%S")
    except Exception:
        pass
    # Try RFC 1123 (used by OneLake DFS API: "Mon, 17 Mar 2026 12:00:00 GMT")
    try:
        return parsedate_to_datetime(dt_string).replace(tzinfo=None)
    except Exception:
        pass
    return None


print("✅ Utilities loaded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 4 — Load Retention Config

# CELL ********************

# ── Retention period configuration ──
# This is the SINGLE SOURCE OF TRUTH for retention days.
# Change this value for demo vs production. Customize per type below.
default_retention_days = 10

# You can customize retention per object type here
RETENTION_DAYS_BY_TYPE = {
    "Lakehouse":       default_retention_days,
    "Warehouse":       default_retention_days,
    "Notebook":        default_retention_days,
    "Pipeline":        default_retention_days,
    "Report":          default_retention_days,
    "SemanticModel":   default_retention_days,
    "DataPipeline":    default_retention_days,
    "Dataflow":        default_retention_days,
    "Environment":     default_retention_days,
    "KQLDatabase":     default_retention_days,
    "Eventstream":     default_retention_days,
    "MLModel":         default_retention_days,
    "MLExperiment":    default_retention_days,
    "SparkJobDefinition": default_retention_days,
    "Workspace":       default_retention_days,
    # Sub-item types (discovered inside Lakehouses / Warehouses)
    "LakehouseTable":  default_retention_days,
    "LakehouseFile":   default_retention_days,
    "WarehouseTable":  default_retention_days,
    "WarehouseView":   default_retention_days,
}
print(f"   Default retention period: {default_retention_days} day(s)")
print(f"   Configured object types: {len(RETENTION_DAYS_BY_TYPE)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 5 — Scan All Workspaces

# CELL ********************

# ── Scan all accessible workspaces ──
print("📡 Listing all accessible workspaces...\n")
headers = get_fabric_headers()

all_workspaces = []
continuation_url = "https://api.fabric.microsoft.com/v1/workspaces"

while continuation_url:
    resp = requests.get(continuation_url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    all_workspaces.extend(data.get("value", []))
    continuation_url = data.get("continuationUri") or data.get("@odata.nextLink")

print(f"Found {len(all_workspaces)} total workspace(s):\n")
for ws in all_workspaces:
    print(f"   • {ws['displayName']}  (type: {ws.get('type','N/A')})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 6 — PBI Admin Scanner (Item Dates)

# CELL ********************

# ── Step 1: Initialize & PBI Admin Scanner ──
import time

now = datetime.utcnow()
inventory_rows = []
errors = []

# Read-only activity types to EXCLUDE from modification tracking
# Any event NOT in this set is treated as a potential modification
READ_ONLY_ACTIVITIES = {
    'ViewReport', 'ViewDashboard', 'ViewDataset', 'ViewDataflow',
    'ViewUsageMetrics', 'ViewUsageMetricsReport', 'ViewArtifact',
    'GetReport', 'GetDashboard', 'GetDataset', 'GetDatasources',
    'GetRefreshHistory', 'GetRefreshSchedule', 'GetGroupUsers',
    'GetWorkspaces', 'GetWorkspace', 'GetSnapshots',
    'GetArtifactAccessRequestsResponseList',
    'ExportReport', 'ExportDataflow', 'ExportArtifact',
    'ExportActivityEvents', 'DownloadReport',
    'ShareReport', 'ShareDashboard',
    'PrintReport', 'GenerateScreenshot',
    'GenerateCustomVisualAADAccessToken',
    'AnalyzeInExcel', 'AnalyzedByExternalApplication',
    'DiscoverSuggestedPeople', 'DiscoverSuggestedArtifacts',
}

print(f"📋 Excluding {len(READ_ONLY_ACTIVITIES)} read-only activity types\n")
print("📡 Step 1: Running PBI Admin Scanner to fetch item dates...\n")

pbi_token = mssparkutils.credentials.getToken("https://analysis.windows.net/powerbi/api")
pbi_headers = {
    "Authorization": f"Bearer {pbi_token}",
    "Content-Type": "application/json"
}

ws_ids = [ws["id"] for ws in all_workspaces]
date_lookup = {}  # item_id -> {"created": datetime, "modified": datetime, "source": str}

for batch_start in range(0, len(ws_ids), 100):
    batch_ids = ws_ids[batch_start:batch_start + 100]
    print(f"   Scanning batch {batch_start//100 + 1} ({len(batch_ids)} workspace(s))...")

    try:
        scan_resp = requests.post(
            "https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo"
            "?lineage=True&datasourceDetails=True",
            headers=pbi_headers,
            json={"workspaces": batch_ids}
        )

        if scan_resp.status_code != 202:
            print(f"   ⚠️ Scanner returned {scan_resp.status_code}: {scan_resp.text[:200]}")
            continue

        scan_id = scan_resp.json().get("id")
        print(f"   Scan initiated (id: {scan_id}), polling...")

        scan_succeeded = False
        for attempt in range(15):
            time.sleep(2)
            status_resp = requests.get(
                f"https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanStatus/{scan_id}",
                headers=pbi_headers
            )
            scan_status = status_resp.json().get("status")
            if scan_status == "Succeeded":
                scan_succeeded = True
                break
            elif scan_status in ("Failed", "Error"):
                print(f"   ❌ Scan failed: {status_resp.text[:200]}")
                break

        if not scan_succeeded:
            if scan_status not in ("Failed", "Error"):
                print(f"   ⚠️ Scan timed out (last status: {scan_status})")
            continue

        result_resp = requests.get(
            f"https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/{scan_id}",
            headers=pbi_headers
        )
        if result_resp.status_code != 200:
            print(f"   ⚠️ Could not fetch scan results: {result_resp.status_code}")
            continue

        result_data = result_resp.json()

        for ws_info in result_data.get("workspaces", []):
            for report in ws_info.get("reports", []):
                rid = report.get("id")
                if rid:
                    date_lookup[rid] = {
                        "created":  safe_parse_datetime(report.get("createdDateTime")),
                        "modified": safe_parse_datetime(report.get("modifiedDateTime")),
                        "source":   "pbi_scanner",
                    }
            for ds in ws_info.get("datasets", []):
                did = ds.get("id")
                if did:
                    date_lookup[did] = {
                        "created":  safe_parse_datetime(ds.get("createdDate")),
                        "modified": safe_parse_datetime(ds.get("modifiedDateTime") or ds.get("modifiedDate")),
                        "source":   "pbi_scanner",
                    }
            for df_item in ws_info.get("dataflows", []):
                dfid = df_item.get("objectId") or df_item.get("id")
                if dfid:
                    date_lookup[dfid] = {
                        "created":  safe_parse_datetime(df_item.get("createdDateTime") or df_item.get("configuredBy")),
                        "modified": safe_parse_datetime(df_item.get("modifiedDateTime") or df_item.get("modifiedBy")),
                        "source":   "pbi_scanner",
                    }
            for dash in ws_info.get("dashboards", []):
                dashid = dash.get("id")
                if dashid:
                    date_lookup[dashid] = {
                        "created":  safe_parse_datetime(dash.get("createdDateTime")),
                        "modified": safe_parse_datetime(dash.get("modifiedDateTime")),
                        "source":   "pbi_scanner",
                    }
            for dm in ws_info.get("datamarts", []):
                dmid = dm.get("id")
                if dmid:
                    date_lookup[dmid] = {
                        "created":  safe_parse_datetime(dm.get("createdDateTime") or dm.get("configuredBy")),
                        "modified": safe_parse_datetime(dm.get("modifiedDateTime") or dm.get("modifiedDate")),
                        "source":   "pbi_scanner",
                    }

    except Exception as e:
        err_msg = f"Scanner error for batch {batch_start//100 + 1}: {e}"
        print(f"   ❌ {err_msg}")
        errors.append(err_msg)

print(f"\n   ✅ Scanner complete — found dates for {len(date_lookup)} item(s)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 7 — Activity Events API (Modification Events)

# CELL ********************

# ── Step 1B: Activity Events API — Modification events only ──
# Enhanced: tries multiple field names for artifact ID to capture Fabric-native items
# (notebooks, pipelines, lakehouses) that may use ItemId or ObjectId instead of ArtifactId.

import re

GUID_PATTERN = re.compile(r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}')


def extract_artifact_id_05(event):
    """Extract artifact ID trying multiple field names + ObjectId URL parsing."""
    art_id = event.get("ArtifactId") or event.get("artifactId") or ""
    if art_id:
        return art_id
    art_id = event.get("ItemId") or event.get("itemId") or ""
    if art_id:
        return art_id
    art_id = (event.get("DatasetId") or event.get("ReportId")
              or event.get("DashboardId") or event.get("DataflowId") or "")
    if art_id:
        return art_id
    object_id = event.get("ObjectId") or event.get("objectId") or ""
    if object_id:
        guids = GUID_PATTERN.findall(object_id)
        if len(guids) >= 2:
            return guids[-1]
        elif len(guids) == 1:
            return guids[0]
    return ""


def extract_artifact_name_05(event):
    """Extract artifact name trying multiple field names."""
    return (event.get("ArtifactName") or event.get("artifactName")
            or event.get("ObjectDisplayName") or event.get("DisplayName")
            or event.get("displayName") or event.get("ItemName")
            or event.get("itemName") or event.get("ObjectName")
            or event.get("objectName") or "")


print("\n📡 Step 1B: Querying Activity Events API (last 29 days)...")
print("   Only modification events count toward last_modified_date.\n")

ACTIVITY_LOOKBACK_DAYS = 29
activity_date_lookup = {}  # artifact_id -> {"first_activity", "last_activity", "name", "ws_id"}

end_dt   = datetime.utcnow()
start_dt = end_dt - timedelta(days=ACTIVITY_LOOKBACK_DAYS)
current_day = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)

days_processed   = 0
total_events     = 0
skipped_readonly = 0
skipped_no_id    = 0
recovered_by_fallback = 0

while current_day < end_dt:
    day_start = current_day
    day_end   = current_day.replace(hour=23, minute=59, second=59)
    if day_end > end_dt:
        day_end = end_dt

    s_str = day_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    e_str = day_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    events_url = (
        f"https://api.powerbi.com/v1.0/myorg/admin/activityevents"
        f"?startDateTime='{s_str}'&endDateTime='{e_str}'"
    )

    try:
        page_url = events_url
        while page_url:
            ev_resp = requests.get(page_url, headers=pbi_headers)

            if ev_resp.status_code == 429:
                retry_after = int(ev_resp.headers.get("Retry-After", 30))
                print(f"   ⏳ Rate-limited — waiting {retry_after}s...")
                time.sleep(retry_after)
                continue

            if ev_resp.status_code != 200:
                if days_processed == 0:
                    print(f"   ⚠️ HTTP {ev_resp.status_code}: {ev_resp.text[:500]}")
                break

            ev_data = ev_resp.json()

            for event in ev_data.get("activityEventEntities", []):
                ev_ts    = event.get("CreationTime") or event.get("creationTime")
                ev_ws_id = (event.get("WorkSpaceId") or event.get("workSpaceId")
                            or event.get("WorkspaceId") or "")
                activity = event.get("Activity") or event.get("activity") or ""

                if not ev_ts:
                    continue

                # ── Enhanced artifact_id extraction with fallbacks ──
                direct_id = event.get("ArtifactId") or event.get("artifactId") or ""
                art_id = extract_artifact_id_05(event)
                art_name = extract_artifact_name_05(event)

                if not art_id:
                    skipped_no_id += 1
                    continue

                if not direct_id and art_id:
                    recovered_by_fallback += 1

                ev_time = safe_parse_datetime(ev_ts)
                if not ev_time:
                    continue

                total_events += 1

                if activity in READ_ONLY_ACTIVITIES:
                    skipped_readonly += 1
                    continue

                if art_id not in activity_date_lookup:
                    activity_date_lookup[art_id] = {
                        "first_activity": ev_time,
                        "last_activity":  ev_time,
                        "name":  art_name,
                        "ws_id": ev_ws_id,
                    }
                else:
                    rec = activity_date_lookup[art_id]
                    if ev_time < rec["first_activity"]:
                        rec["first_activity"] = ev_time
                    if ev_time > rec["last_activity"]:
                        rec["last_activity"] = ev_time
                    if art_name and not rec.get("name"):
                        rec["name"] = art_name
                    if ev_ws_id and not rec.get("ws_id"):
                        rec["ws_id"] = ev_ws_id

            page_url = ev_data.get("continuationUri")

    except Exception as e:
        errors.append(f"Activity Events error for {day_start.date()}: {e}")

    current_day += timedelta(days=1)
    days_processed += 1
    if days_processed % 5 == 0:
        print(f"   Processed {days_processed}/{ACTIVITY_LOOKBACK_DAYS} days "
              f"({len(activity_date_lookup)} unique items, {total_events} events)...")

print(f"\n   ✅ Activity Events complete:")
print(f"      Days scanned:          {days_processed}")
print(f"      Total audit events:    {total_events}")
print(f"      Read-only (excluded):  {skipped_readonly}")
print(f"      Modification events:   {total_events - skipped_readonly}")
print(f"      Unique modified items: {len(activity_date_lookup)}")
print(f"      Recovered by fallback: {recovered_by_fallback}")
print(f"      Dropped (no ID found): {skipped_no_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 8 — Merge Activity Dates into Lookup

# CELL ********************

# ── Merge activity dates into date_lookup + build name-based fallback ──
new_from_activity = 0
enriched_modified = 0

for art_id, act in activity_date_lookup.items():
    if art_id not in date_lookup:
        date_lookup[art_id] = {
            "created":  act["first_activity"],
            "modified": act["last_activity"],
            "source":   "activity_events",
        }
        new_from_activity += 1
    else:
        existing = date_lookup[art_id]
        if act["last_activity"] and (
            not existing.get("modified")
            or act["last_activity"] > existing["modified"]
        ):
            existing["modified"] = act["last_activity"]
            enriched_modified += 1

# Name-based fallback: (workspace_id, item_name) -> dates
# Handles cases where ArtifactId in audit logs ≠ Fabric v1 item id
activity_name_lookup = {}
for art_id, act in activity_date_lookup.items():
    act_name = act.get("name", "").strip()
    act_ws   = act.get("ws_id", "").strip()
    if act_name and act_ws:
        key = (act_ws.lower(), act_name.lower())
        if key not in activity_name_lookup:
            activity_name_lookup[key] = {
                "created":  act["first_activity"],
                "modified": act["last_activity"],
            }
        else:
            existing = activity_name_lookup[key]
            if act["first_activity"] < existing["created"]:
                existing["created"] = act["first_activity"]
            if act["last_activity"] > existing["modified"]:
                existing["modified"] = act["last_activity"]

print(f"📊 Merge results:")
print(f"   NEW items dated:          {new_from_activity}  (not in PBI scanner)")
print(f"   Enriched modified date:   {enriched_modified}  (updated from activity log)")
print(f"   Name-based fallbacks:     {len(activity_name_lookup)} (ws_id + name pairs)")
print(f"   Total date_lookup:        {len(date_lookup)} items")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 9 — List Items per Workspace & Merge Dates

# CELL ********************

# ── Step 2: List items per workspace & merge dates ──
print("\n📋 Step 2: Listing all items per workspace & merging dates...\n")

# Reset lists so re-running this cell won't duplicate rows
inventory_rows = []
errors = []

for ws in all_workspaces:
    ws_name = ws["displayName"]
    ws_id = ws["id"]
    ws_type = ws.get("type", "Unknown")

    print(f"{'─'*50}")
    print(f"📂 Workspace: {ws_name}")

    # ── Add the workspace itself as an inventory row ──
    ws_created_dt  = None
    ws_modified_dt = None
    ws_date_source = "none"

    if ws_id in date_lookup:
        ws_created_dt  = date_lookup[ws_id].get("created")
        ws_modified_dt = date_lookup[ws_id].get("modified")
        ws_date_source = date_lookup[ws_id].get("source", "id_match")
    else:
        ws_name_key = (ws_id.lower(), ws_name.lower())
        if ws_name_key in activity_name_lookup:
            ws_created_dt  = activity_name_lookup[ws_name_key].get("created")
            ws_modified_dt = activity_name_lookup[ws_name_key].get("modified")
            ws_date_source = "activity_name_match"

    ws_retention_days = RETENTION_DAYS_BY_TYPE.get("Workspace", default_retention_days)
    ws_reference_dt   = ws_modified_dt or ws_created_dt
    ws_age_days       = (now - ws_reference_dt).days if ws_reference_dt else None
    ws_deletion_due   = (ws_reference_dt + timedelta(days=ws_retention_days)) if ws_reference_dt else None
    ws_is_overdue     = (now > ws_deletion_due) if ws_deletion_due else None

    inventory_rows.append({
        "workspace_name":        ws_name,
        "workspace_id":          ws_id,
        "workspace_type":        ws_type,
        "item_name":             ws_name,
        "item_id":               ws_id,
        "item_type":             "Workspace",
        "description":           ws.get("description", ""),
        "created_date":          ws_created_dt.date() if ws_created_dt else None,
        "last_modified_date":    ws_modified_dt.date() if ws_modified_dt else None,
        "date_source":           ws_date_source,
        "age_days":              ws_age_days,
        "retention_period_days": ws_retention_days,
        "deletion_due_date":     ws_deletion_due.date() if ws_deletion_due else None,
        "is_overdue":            ws_is_overdue,
        "scan_date":             now.date(),
    })

    items_url = f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/items"
    all_items = []

    try:
        page_url = items_url
        while page_url:
            items_resp = requests.get(page_url, headers=headers)
            items_resp.raise_for_status()
            items_data = items_resp.json()
            all_items.extend(items_data.get("value", []))
            page_url = items_data.get("continuationUri") or items_data.get("@odata.nextLink")
    except Exception as e:
        err_msg = f"Error listing items in {ws_name}: {e}"
        print(f"   ❌ {err_msg}")
        errors.append(err_msg)
        continue

    dates_found = 0
    for item in all_items:
        item_id   = item.get("id", "")
        item_name = item.get("displayName", "Unknown")
        item_type = item.get("type", "Unknown")
        item_desc = item.get("description", "")

        created_dt  = None
        modified_dt = None
        date_source = "none"

        # Try ID-based lookup first (PBI Scanner / Activity Events)
        if item_id in date_lookup:
            created_dt  = date_lookup[item_id].get("created")
            modified_dt = date_lookup[item_id].get("modified")
            date_source = date_lookup[item_id].get("source", "id_match")
        else:
            # Fallback 1: match by (workspace_id, item_name) from activity events
            name_key = (ws_id.lower(), item_name.lower())
            if name_key in activity_name_lookup:
                created_dt  = activity_name_lookup[name_key].get("created")
                modified_dt = activity_name_lookup[name_key].get("modified")
                date_source = "activity_name_match"


        if created_dt or modified_dt:
            dates_found += 1

        retention_days = RETENTION_DAYS_BY_TYPE.get(item_type, default_retention_days)
        reference_dt   = modified_dt or created_dt
        age_days       = (now - reference_dt).days if reference_dt else None
        deletion_due   = (reference_dt + timedelta(days=retention_days)) if reference_dt else None
        is_overdue     = (now > deletion_due) if deletion_due else None

        # Convert datetimes → date for DateType columns
        inventory_rows.append({
            "workspace_name":        ws_name,
            "workspace_id":          ws_id,
            "workspace_type":        ws_type,
            "item_name":             item_name,
            "item_id":               item_id,
            "item_type":             item_type,
            "description":           item_desc or "",
            "created_date":          created_dt.date() if created_dt else None,
            "last_modified_date":    modified_dt.date() if modified_dt else None,
            "date_source":           date_source,
            "age_days":              age_days,
            "retention_period_days": retention_days,
            "deletion_due_date":     deletion_due.date() if deletion_due else None,
            "is_overdue":            is_overdue,
            "scan_date":             now.date(),
        })

    print(f"   ✅ {len(all_items)} objects ({dates_found} with dates)")

# ── Summary & Diagnostics ──
print(f"\n{'═'*50}")
print(f"Total objects cataloged: {len(inventory_rows)}")
dates_total = sum(1 for r in inventory_rows if r["created_date"] or r["last_modified_date"])
print(f"Objects with dates: {dates_total}/{len(inventory_rows)}")
no_dates = len(inventory_rows) - dates_total
if no_dates > 0:
    print(f"⚠️  {no_dates} item(s) still without dates (date_source='none')")

# Date Source Breakdown
from collections import Counter
source_counts = Counter(r["date_source"] for r in inventory_rows)
print(f"\n📊 Date Source Breakdown:")
for src, cnt in source_counts.most_common():
    print(f"   {src}: {cnt} item(s)")


if errors:
    print(f"\nErrors encountered: {len(errors)}")
    for e in errors:
        print(f"   • {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 9b - Lakehouse and Warehouse Sub-Item Discovery

# CELL ********************

# ── Step 2b: Discover sub-items inside Lakehouses & Warehouses ──
print("\n📋 Step 2b: Discovering tables/files inside Lakehouses & Warehouses...\n")

headers = get_fabric_headers()

# OneLake DFS requires a token scoped to storage.azure.com, not api.fabric.microsoft.com
onelake_token = mssparkutils.credentials.getToken("https://storage.azure.com/")
onelake_headers = {"Authorization": f"Bearer {onelake_token}"}

sub_item_count = 0
sub_item_dates = 0

# ── Collect Lakehouses and Warehouses from the existing inventory ──
lakehouses = [r for r in inventory_rows if r["item_type"] == "Lakehouse"]
warehouses = [r for r in inventory_rows if r["item_type"] == "Warehouse"]

print(f"   Found {len(lakehouses)} Lakehouse(s) and {len(warehouses)} Warehouse(s) to scan\n")

MAX_SCAN_DEPTH = 10  # Safety limit to prevent infinite recursion

# ═══════════════════════════════════════════════════════
# LAKEHOUSE TABLES — via Fabric REST API + DFS fallback
# ═══════════════════════════════════════════════════════
for lh in lakehouses:
    ws_id   = lh["workspace_id"]
    ws_name = lh["workspace_name"]
    lh_id   = lh["item_id"]
    lh_name = lh["item_name"]
    ws_type = lh["workspace_type"]

    print(f"{'─'*50}")
    print(f"📦 Lakehouse: {lh_name}  (workspace: {ws_name})")

    # ── Tables — Method 1: Fabric Tables API ──
    tables_url = f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/lakehouses/{lh_id}/tables"
    lh_tables = []
    api_status = "unknown"
    try:
        page_url = tables_url
        while page_url:
            resp = requests.get(page_url, headers=headers)
            api_status = str(resp.status_code)
            if resp.status_code in (400, 404):
                break
            resp.raise_for_status()
            resp_data = resp.json()
            # Handle both possible response keys ("data" or "value")
            batch = resp_data.get("data") or resp_data.get("value") or []
            if isinstance(batch, list):
                lh_tables.extend(batch)
            page_url = resp_data.get("continuationUri") or resp_data.get("@odata.nextLink")
    except Exception as e:
        api_status = f"error: {e}"

    print(f"   ℹ️ Tables API: status={api_status}, entries={len(lh_tables)}")

    table_dates = 0
    real_table_count = 0

    # ── Step A: Process tables returned by the API ──
    for tbl in lh_tables:
        try:
            tbl_name = tbl.get("name") or tbl.get("tableName") or tbl.get("displayName") or ""
        except (AttributeError, TypeError):
            continue
        if not tbl_name or tbl_name in ("Files", "Tables", "TableMaintenance"):
            continue

        tbl_type = tbl.get("type", "Unknown")
        tbl_format = tbl.get("format", "Unknown")

        created_dt  = None
        modified_dt = None
        date_source = "none"

        raw_ts = tbl.get("lastUpdatedTimestamp") or tbl.get("lastModifiedTimestamp") or ""
        if raw_ts:
            modified_dt = safe_parse_datetime(raw_ts)
            if modified_dt:
                date_source = "lakehouse_tables_api"

        if modified_dt:
            table_dates  += 1
            sub_item_dates += 1

        retention_days = RETENTION_DAYS_BY_TYPE.get("LakehouseTable", default_retention_days)
        reference_dt   = modified_dt or created_dt
        age_days       = (now - reference_dt).days if reference_dt else None
        deletion_due   = (reference_dt + timedelta(days=retention_days)) if reference_dt else None
        is_overdue     = (now > deletion_due) if deletion_due else None

        inventory_rows.append({
            "workspace_name":        ws_name,
            "workspace_id":          ws_id,
            "workspace_type":        ws_type,
            "item_name":             f"{lh_name}/{tbl_name}",
            "item_id":               lh_id,
            "item_type":             "LakehouseTable",
            "description":           f"Table in {lh_name} ({tbl_type}, {tbl_format})",
            "created_date":          None,
            "last_modified_date":    modified_dt.date() if modified_dt else None,
            "date_source":           date_source,
            "age_days":              age_days,
            "retention_period_days": retention_days,
            "deletion_due_date":     deletion_due.date() if deletion_due else None,
            "is_overdue":            is_overdue,
            "scan_date":             now.date(),
        })
        sub_item_count += 1
        real_table_count += 1

    # ── Step B: DFS fallback — if API found no real tables, scan OneLake directly ──
    # Uses recursive=true to handle schemas-enabled lakehouses where tables
    # are nested under schema dirs (e.g., Tables/dbo/tablename)
    if real_table_count == 0:
        onelake_base_url = f"https://onelake.dfs.fabric.microsoft.com/{ws_id}/{lh_id}"
        try:
            # Paginated DFS listing to capture ALL entries under Tables/
            dfs_paths = []
            dfs_continuation = None
            dfs_status = "unknown"
            while True:
                tbl_list_url = (
                    f"{onelake_base_url}"
                    f"?resource=filesystem&directory=Tables&recursive=true"
                )
                if dfs_continuation:
                    tbl_list_url += f"&continuation={dfs_continuation}"
                tbl_resp = requests.get(tbl_list_url, headers=onelake_headers)
                dfs_status = str(tbl_resp.status_code)
                if tbl_resp.status_code != 200:
                    break
                page_data = tbl_resp.json()
                dfs_paths.extend(page_data.get("paths", []))
                dfs_continuation = tbl_resp.headers.get("x-ms-continuation")
                if not dfs_continuation:
                    break

            print(f"   ℹ️ DFS Tables scan: status={dfs_status}")

            if tbl_resp.status_code == 200:
                print(f"   ℹ️ DFS Tables entries: {len(dfs_paths)}")

                # Show first few raw DFS paths for diagnostics
                for dbg_i, dbg_e in enumerate(dfs_paths[:3]):
                    print(f"      [path {dbg_i}] {dbg_e.get('name','')}  (isDir={dbg_e.get('isDirectory','?')})")
                if len(dfs_paths) > 3:
                    print(f"      ... ({len(dfs_paths) - 3} more)")

                # Structural dirs that are Fabric lakehouse internals, not schemas
                STRUCTURAL_DIRS = {"Tables", "Files", "TableMaintenance"}

                # Two-pass approach to distinguish schema dirs from real tables.
                # Handles schemas-enabled lakehouses where DFS paths have extra
                # structural prefixes (e.g. Tables/Tables/dbo/mytable/...).
                schema_dirs = set()
                schema_table_names = {}  # table_name -> first entry with metadata

                for entry in dfs_paths:
                    entry_name = entry.get("name", "")
                    rel = entry_name[len("Tables/"):] if entry_name.startswith("Tables/") else entry_name

                    # Skip Files/ entries (shouldn't be in Tables listing)
                    if rel.startswith("Files/") or rel == "Files":
                        continue
                    # Strip extra Tables/ structural prefix
                    was_double_nested = False
                    if rel.startswith("Tables/"):
                        rel = rel[len("Tables/"):]
                        was_double_nested = True
                    elif rel == "Tables":
                        continue

                    parts = [p for p in rel.split("/") if p]

                    # Any dir directly under Tables/Tables/ is a schema dir
                    # (catches empty schemas like dbo with no sub-tables)
                    if was_double_nested and len(parts) >= 1 and not parts[0].startswith("_"):
                        schema_dirs.add(parts[0])

                    if len(parts) >= 3 and not parts[1].startswith("_"):
                        schema_dirs.add(parts[0])
                        tbl_name = parts[1]
                        if tbl_name not in schema_table_names:
                            schema_table_names[tbl_name] = entry

                # Pass 2: Collect directory entries for tables.
                # - Depth-1 dirs NOT in schema_dirs → standalone tables
                # - Depth-2 dirs under schema_dirs → schema-based tables
                # Also update schema_table_names with dir entries (better metadata)
                seen_tables = {}  # tbl_name -> entry

                for entry in dfs_paths:
                    entry_name = entry.get("name", "")
                    is_dir = entry.get("isDirectory", "false")
                    if isinstance(is_dir, str):
                        is_dir = is_dir.lower() == "true"
                    if not is_dir:
                        continue

                    rel = entry_name[len("Tables/"):] if entry_name.startswith("Tables/") else entry_name

                    # Skip Files/ entries and strip extra Tables/ prefix
                    if rel.startswith("Files/") or rel == "Files":
                        continue
                    if rel.startswith("Tables/"):
                        rel = rel[len("Tables/"):]
                    elif rel == "Tables":
                        continue

                    parts = [p for p in rel.split("/") if p]

                    if any(p.startswith("_") for p in parts):
                        continue

                    if len(parts) == 1:
                        name = parts[0]
                        if name in schema_dirs:
                            continue  # schema folder, not a table
                        if not name or name in ("Files", "Tables", "TableMaintenance"):
                            continue
                        seen_tables[name] = entry
                    elif len(parts) == 2 and parts[0] in schema_dirs:
                        tbl_name = parts[1]
                        if not tbl_name or tbl_name in ("Files", "Tables", "TableMaintenance"):
                            continue
                        # Prefer dir entry over file entry for lastModified
                        schema_table_names[tbl_name] = entry
                        seen_tables[tbl_name] = entry

                # Add schema-based tables that had no dir entry (found via file paths only)
                for tbl_name, entry in schema_table_names.items():
                    if tbl_name not in seen_tables:
                        if tbl_name and tbl_name not in ("Files", "Tables", "TableMaintenance"):
                            seen_tables[tbl_name] = entry

                if schema_dirs:
                    print(f"   ℹ️ Schema dirs detected (skipped): {sorted(schema_dirs)}")
                print(f"   ℹ️ Real tables found via DFS: {list(seen_tables.keys())}")

                for tbl_name, entry in seen_tables.items():
                    raw_modified = entry.get("lastModified", "")
                    modified_dt = safe_parse_datetime(raw_modified) if raw_modified else None
                    created_dt = None
                    date_source = "none"
                    if modified_dt:
                        date_source = "onelake_dfs_tables"
                        table_dates += 1
                        sub_item_dates += 1

                    retention_days = RETENTION_DAYS_BY_TYPE.get("LakehouseTable", default_retention_days)
                    reference_dt = modified_dt or created_dt
                    age_days = (now - reference_dt).days if reference_dt else None
                    deletion_due = (reference_dt + timedelta(days=retention_days)) if reference_dt else None
                    is_overdue = (now > deletion_due) if deletion_due else None

                    inventory_rows.append({
                        "workspace_name":        ws_name,
                        "workspace_id":          ws_id,
                        "workspace_type":        ws_type,
                        "item_name":             f"{lh_name}/{tbl_name}",
                        "item_id":               lh_id,
                        "item_type":             "LakehouseTable",
                        "description":           f"Table in {lh_name} (DFS discovered)",
                        "created_date":          None,
                        "last_modified_date":    modified_dt.date() if modified_dt else None,
                        "date_source":           date_source,
                        "age_days":              age_days,
                        "retention_period_days": retention_days,
                        "deletion_due_date":     deletion_due.date() if deletion_due else None,
                        "is_overdue":            is_overdue,
                        "scan_date":             now.date(),
                    })
                    sub_item_count += 1
                    real_table_count += 1
            elif tbl_resp.status_code in (401, 403):
                print(f"   ⚠️ No access to OneLake Tables folder ({tbl_resp.status_code})")
            elif tbl_resp.status_code == 404:
                print(f"   ℹ️ No Tables directory found")
            else:
                print(f"   ⚠️ DFS table scan returned {tbl_resp.status_code}")
        except Exception as e:
            err_msg = f"Error scanning DFS tables in {lh_name}: {e}"
            print(f"   ⚠️ {err_msg}")
            errors.append(err_msg)

    print(f"   📊 Tables: {real_table_count} found ({table_dates} with dates)"
          + (f"  [API: {len(lh_tables)} raw]" if lh_tables else "  [via DFS]" if real_table_count > 0 else ""))

    # ── Files — via OneLake DFS API ──
    # Single recursive=true call to get ALL files under /Files/ — no manual recursion needed
    onelake_base = f"https://onelake.dfs.fabric.microsoft.com/{ws_id}/{lh_id}"
    file_count = 0
    file_dates = 0

    try:
        continuation = None
        while True:
            list_url = f"{onelake_base}?resource=filesystem&directory=Files&recursive=true"
            if continuation:
                list_url += f"&continuation={continuation}"

            resp = requests.get(list_url, headers=onelake_headers)
            if resp.status_code == 404:
                break
            if resp.status_code in (401, 403):
                print(f"   ⚠️ No access to OneLake files in {lh_name} ({resp.status_code})")
                break
            resp.raise_for_status()
            data = resp.json()

            for entry in data.get("paths", []):
                entry_name = entry.get("name", "")
                is_directory = entry.get("isDirectory", "false")

                if isinstance(is_directory, str):
                    is_directory = is_directory.lower() == "true"

                # Only process files (not directories), and only under Files/
                if is_directory:
                    continue
                if not entry_name.startswith("Files/"):
                    continue
                # Skip delta internal files stored under Files/Tables/
                if entry_name.startswith("Files/Tables/"):
                    continue

                created_dt  = None
                modified_dt = None
                date_source = "none"

                raw_modified = entry.get("lastModified", "")
                if raw_modified:
                    modified_dt = safe_parse_datetime(raw_modified)
                    if modified_dt:
                        date_source = "onelake_dfs"
                        file_dates += 1
                        sub_item_dates += 1

                display_name = f"{lh_name}/{entry_name}"
                display_name = display_name.replace("/Files/Files/", "/Files/", 1)

                retention_days = RETENTION_DAYS_BY_TYPE.get("LakehouseFile", default_retention_days)
                reference_dt   = modified_dt or created_dt
                age_days       = (now - reference_dt).days if reference_dt else None
                deletion_due   = (reference_dt + timedelta(days=retention_days)) if reference_dt else None
                is_overdue     = (now > deletion_due) if deletion_due else None

                inventory_rows.append({
                    "workspace_name":        ws_name,
                    "workspace_id":          ws_id,
                    "workspace_type":        ws_type,
                    "item_name":             display_name,
                    "item_id":               lh_id,
                    "item_type":             "LakehouseFile",
                    "description":           f"File in {lh_name}",
                    "created_date":          created_dt.date() if created_dt else None,
                    "last_modified_date":    modified_dt.date() if modified_dt else None,
                    "date_source":           date_source,
                    "age_days":              age_days,
                    "retention_period_days": retention_days,
                    "deletion_due_date":     deletion_due.date() if deletion_due else None,
                    "is_overdue":            is_overdue,
                    "scan_date":             now.date(),
                })
                file_count += 1
                sub_item_count += 1

            # Check for pagination continuation token
            continuation = resp.headers.get("x-ms-continuation")
            if not continuation:
                break

    except Exception as e:
        err_msg = f"Error scanning files in {lh_name}: {e}"
        print(f"   ⚠️ {err_msg}")
        errors.append(err_msg)

    print(f"   📁 Files:  {file_count} found ({file_dates} with dates)")

# ═══════════════════════════════════════════════════════
# WAREHOUSE TABLES & VIEWS — via Fabric SQL endpoint
# Uses INFORMATION_SCHEMA + sys metadata for dates
# ═══════════════════════════════════════════════════════
for wh in warehouses:
    ws_id   = wh["workspace_id"]
    ws_name = wh["workspace_name"]
    wh_id   = wh["item_id"]
    wh_name = wh["item_name"]
    ws_type = wh["workspace_type"]
    print(f"{'─'*50}")
    print(f"🏢 Warehouse: {wh_name}  (workspace: {ws_name})")

    wh_tables_found = 0
    wh_views_found  = 0
    wh_dates_found  = 0

    try:
        # Query the warehouse's SQL endpoint for tables and views with modification dates
        wh_sql = f"""
            SELECT
                t.name           AS object_name,
                t.type_desc      AS object_type,
                s.name           AS schema_name,
                t.create_date    AS created,
                t.modify_date    AS modified
            FROM [{wh_name}].sys.objects t
            JOIN [{wh_name}].sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.type IN ('U', 'V')
            ORDER BY t.type_desc, t.name
        """
        wh_df = spark.sql(wh_sql)
        wh_rows = wh_df.collect()

        for row in wh_rows:
            obj_name    = row["object_name"]
            obj_type_db = row["object_type"]   # USER_TABLE or VIEW
            schema_name = row["schema_name"]
            created_dt  = row["created"] if row["created"] else None
            modified_dt = row["modified"] if row["modified"] else None
            date_source = "none"

            # Convert to datetime if they're date objects
            if created_dt and not isinstance(created_dt, datetime):
                created_dt = datetime.combine(created_dt, datetime.min.time())
            if modified_dt and not isinstance(modified_dt, datetime):
                modified_dt = datetime.combine(modified_dt, datetime.min.time())

            if created_dt or modified_dt:
                date_source = "warehouse_sys_metadata"
                wh_dates_found += 1
                sub_item_dates += 1

            if obj_type_db == "USER_TABLE":
                item_type = "WarehouseTable"
                wh_tables_found += 1
            else:
                item_type = "WarehouseView"
                wh_views_found += 1

            display_name = f"{wh_name}/{schema_name}.{obj_name}"
            retention_days = RETENTION_DAYS_BY_TYPE.get(item_type, default_retention_days)
            reference_dt   = modified_dt or created_dt
            age_days       = (now - reference_dt).days if reference_dt else None
            deletion_due   = (reference_dt + timedelta(days=retention_days)) if reference_dt else None
            is_overdue     = (now > deletion_due) if deletion_due else None

            inventory_rows.append({
                "workspace_name":        ws_name,
                "workspace_id":          ws_id,
                "workspace_type":        ws_type,
                "item_name":             display_name,
                "item_id":               wh_id,
                "item_type":             item_type,
                "description":           f"{obj_type_db} in {wh_name}.{schema_name}",
                "created_date":          created_dt.date() if created_dt else None,
                "last_modified_date":    modified_dt.date() if modified_dt else None,
                "date_source":           date_source,
                "age_days":              age_days,
                "retention_period_days": retention_days,
                "deletion_due_date":     deletion_due.date() if deletion_due else None,
                "is_overdue":            is_overdue,
                "scan_date":             now.date(),
            })
            sub_item_count += 1

        print(f"   📊 Tables: {wh_tables_found}  |  Views: {wh_views_found}  |  With dates: {wh_dates_found}")

    except Exception as e:
        err_msg = f"Error querying warehouse {wh_name}: {e}"
        print(f"   ❌ {err_msg}")
        errors.append(err_msg)

# ── Sub-Item Summary ──
print(f"\n{'═'*50}")
print(f"📦 Sub-item discovery complete:")
print(f"   Total sub-items found:   {sub_item_count}")
print(f"   Sub-items with dates:    {sub_item_dates}/{sub_item_count}")
print(f"   Updated inventory total: {len(inventory_rows)} rows")

# Count by sub-item type
lt_count = sum(1 for r in inventory_rows if r["item_type"] == "LakehouseTable")
lf_count = sum(1 for r in inventory_rows if r["item_type"] == "LakehouseFile")
wt_count = sum(1 for r in inventory_rows if r["item_type"] == "WarehouseTable")
wv_count = sum(1 for r in inventory_rows if r["item_type"] == "WarehouseView")
print(f"   LakehouseTable: {lt_count}  |  LakehouseFile: {lf_count}")
print(f"   WarehouseTable: {wt_count}  |  WarehouseView: {wv_count}")

# Updated Date Source Breakdown (including sub-items)
from collections import Counter
source_counts = Counter(r["date_source"] for r in inventory_rows)

print(f"\n📊 Updated Date Source Breakdown (with sub-items):")
for src, cnt in source_counts.most_common():
    print(f"   {src}: {cnt} item(s)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 10 — Build Spark DataFrame

# CELL ********************

# ── Build Spark DataFrame ──
schema = StructType([
    StructField("workspace_name",        StringType(),    True),
    StructField("workspace_id",          StringType(),    True),
    StructField("workspace_type",        StringType(),    True),
    StructField("item_name",             StringType(),    True),
    StructField("item_id",              StringType(),    True),
    StructField("item_type",             StringType(),    True),
    StructField("description",           StringType(),    True),
    StructField("created_date",          DateType(),      True),
    StructField("last_modified_date",    DateType(),      True),
    StructField("date_source",           StringType(),    True),
    StructField("age_days",              IntegerType(),   True),
    StructField("retention_period_days", IntegerType(),   True),
    StructField("deletion_due_date",     DateType(),      True),
    StructField("is_overdue",            BooleanType(),   True),
    StructField("scan_date",             DateType(),      True),
])

print(f"Schema ready — {len(inventory_rows)} inventory rows, {len(schema.fields)} columns")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 11 — Write to Delta Table

# CELL ********************

# ── Deduplicate before writing — prefer rows WITH dates ──
best_rows = {}  # key -> row (keeps the one with the best date info)
for row in inventory_rows:
    key = (row["item_id"], row["item_type"], row["item_name"])
    if key not in best_rows:
        best_rows[key] = row
    else:
        # Keep the row that has a last_modified_date (or created_date) over one that doesn't
        existing = best_rows[key]
        if not existing["last_modified_date"] and row["last_modified_date"]:
            best_rows[key] = row
        elif not existing["last_modified_date"] and not existing["created_date"] and row["created_date"]:
            best_rows[key] = row

deduped = list(best_rows.values())
if len(deduped) < len(inventory_rows):
    print(f"⚠️ Removed {len(inventory_rows) - len(deduped)} duplicate rows (kept rows with dates)")
inventory_rows = deduped

# ── Write to Delta table ──
if inventory_rows:
    df = spark.createDataFrame(inventory_rows, schema=schema)
else:
    df = spark.createDataFrame([], schema=schema)

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(inventory_table_name)

print(f"✅ Delta table written: {inventory_table_name}")
print(f"   Rows: {df.count()}")

dates_api  = df.filter(col("last_modified_date").isNotNull()).count()
dates_none = df.filter(col("last_modified_date").isNull()).count()
print(f"   With last_modified_date:    {dates_api}")
print(f"   Without last_modified_date: {dates_none}")
print(f"\n   Date source breakdown:")
df.groupBy("date_source").count().orderBy("count", ascending=False).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 12 — Summary Report

# CELL ********************

# ── Display Summary Report ──
from pyspark.sql.functions import count, sum as spark_sum, avg

print("\n" + "═"*70)
print("  WORKSPACE INVENTORY & RETENTION READINESS REPORT")
print("═"*70)
print(f"  Scan Date:  {now.strftime('%Y-%m-%d')} UTC")
print(f"  Workspaces: {df.select('workspace_name').distinct().count()}")
print(f"  Objects:    {df.count()}")
print("═"*70 + "\n")

total_items  = df.count()
has_modified = df.filter(col("last_modified_date").isNotNull()).count()
no_modified  = df.filter(col("last_modified_date").isNull()).count()

print(f"📅 RETENTION DATE READINESS:")
print(f"   ┌────────────────────────────────────────────────────────────┐")
print(f"   │ last_modified_date populated:  {has_modified:>3}/{total_items}  ← USABLE for retention│")
print(f"   │ last_modified_date NULL:       {no_modified:>3}/{total_items}  ← cannot assess yet   │")
print(f"   └────────────────────────────────────────────────────────────┘")

print(f"\n   Date Source Breakdown:")
print(f"   ─────────────────────────────────────────────────────────────")
source_summary = df.groupBy("date_source").agg(
    count("*").alias("items"),
    spark_sum(col("last_modified_date").isNotNull().cast("int")).alias("has_date"),
).orderBy(col("items").desc())
source_summary.show(truncate=False)

print(f"   📌 pbi_scanner = true modification date from Power BI Admin API")
print(f"   📌 activity_events = last MODIFICATION event (views excluded)")
print(f"   📌 activity_name_match = matched by workspace+name (ID mismatch)")
print(f"   📌 lakehouse_tables_api = table date from Lakehouse Tables API")
print(f"   📌 onelake_dfs = file date from OneLake DFS API")
print(f"   📌 warehouse_sys_metadata = table/view date from sys.objects")
print(f"   📌 none = no modification activity found in last 29 days\n")

if no_modified > 0:
    print(f"   ⚠️  {no_modified} item(s) have NO last_modified_date:")
    print(f"      → Retention policies CANNOT evaluate these items yet.")
    print(f"      → Run this scan regularly to detect future modifications.\n")

# By Workspace
print("📊 Objects per Workspace:")
ws_summary = df.groupBy("workspace_name").agg(
    count("*").alias("total_objects"),
    spark_sum(col("last_modified_date").isNotNull().cast("int")).alias("has_modified_date"),
    spark_sum(col("is_overdue").cast("int")).alias("overdue_count")
).orderBy("workspace_name")
ws_summary.show(50, truncate=False)

# By Object Type
print("📊 Objects per Type:")
type_summary = df.groupBy("item_type").agg(
    count("*").alias("total"),
    spark_sum(col("last_modified_date").isNotNull().cast("int")).alias("has_modified_date"),
    spark_sum(col("is_overdue").cast("int")).alias("overdue"),
    avg("age_days").cast("int").alias("avg_age_days")
).orderBy(col("total").desc())
type_summary.show(30, truncate=False)

# Overdue Items
overdue_df    = df.filter(col("is_overdue") == True)
overdue_count = overdue_df.count()
print(f"🚨 Overdue Items: {overdue_count}")
if overdue_count > 0:
    overdue_df.select(
        "workspace_name", "item_name", "item_type",
        "age_days", "retention_period_days", "deletion_due_date", "date_source"
    ).orderBy("age_days", ascending=False).show(100, truncate=False)

# Items with no modification date
no_date  = df.filter(col("last_modified_date").isNull())
nd_count = no_date.count()
if nd_count > 0:
    print(f"\n⚠️  {nd_count} item(s) have no last_modified_date (retention not assessable):")
    no_date.select(
        "workspace_name", "item_name", "item_type", "date_source"
    ).show(50, truncate=False)

print("\n✅ Inventory scan complete. Query the Delta table for full details:")
print(f"   SELECT * FROM {inventory_table_name}")
print(f"\n   -- Retention-ready items only --")
print(f"   SELECT * FROM {inventory_table_name} WHERE last_modified_date IS NOT NULL")
print(f"\n   -- Items needing attention (no modification date) --")
print(f"   SELECT * FROM {inventory_table_name} WHERE date_source = 'none'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Cell 13 — Ad-hoc Queries

# CELL ********************

# ── Query the Delta Table ──
df_inv = spark.sql("SELECT * FROM workspace_inventory ORDER BY workspace_name, item_type, item_name")
df_inv.show(100, truncate=False)

# Uncomment any of the queries below as needed:

# -- Only items with last_modified_date populated --
# spark.sql("""
#     SELECT workspace_name, item_name, item_type, created_date, last_modified_date,
#            date_source, age_days, retention_period_days, deletion_due_date, is_overdue
#     FROM workspace_inventory
#     WHERE last_modified_date IS NOT NULL
#     ORDER BY last_modified_date DESC
# """).show(100, truncate=False)

# -- Overdue items only --
# spark.sql("""
#     SELECT workspace_name, item_name, item_type, age_days, retention_period_days,
#            deletion_due_date, date_source
#     FROM workspace_inventory
#     WHERE is_overdue = true
#     ORDER BY age_days DESC
# """).show(100, truncate=False)

# -- Items with no date (age unknown) --
# spark.sql("""
#     SELECT workspace_name, item_name, item_type, date_source
#     FROM workspace_inventory
#     WHERE age_days IS NULL
#     ORDER BY workspace_name, item_type
# """).show(100, truncate=False)

# -- Summary by workspace --
# spark.sql("""
#     SELECT workspace_name,
#            COUNT(*) AS total_items,
#            SUM(CASE WHEN is_overdue THEN 1 ELSE 0 END) AS overdue,
#            SUM(CASE WHEN age_days IS NULL THEN 1 ELSE 0 END) AS no_date,
#            ROUND(AVG(age_days), 1) AS avg_age_days
#     FROM workspace_inventory
#     GROUP BY workspace_name
#     ORDER BY workspace_name
# """).show(50, truncate=False)

# -- Summary by item type --
# spark.sql("""
#     SELECT item_type,
#            COUNT(*) AS total,
#            SUM(CASE WHEN is_overdue THEN 1 ELSE 0 END) AS overdue,
#            SUM(CASE WHEN age_days IS NULL THEN 1 ELSE 0 END) AS no_date,
#            MIN(age_days) AS min_age, MAX(age_days) AS max_age
#     FROM workspace_inventory
#     GROUP BY item_type
#     ORDER BY total DESC
# """).show(30, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
