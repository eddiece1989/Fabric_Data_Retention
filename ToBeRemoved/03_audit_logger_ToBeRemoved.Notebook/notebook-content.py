# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 03_audit_logger
# Consolidates individual audit records from a pipeline run into
# the central audit Delta table. Called at the end of the pipeline
# after all ForEach iterations complete.

# CELL ********************

config_lakehouse_path = "/lakehouse/default"
run_id = "manual-test-001"
pipeline_status = "Succeeded"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Inlined config_loader utilities ──
import json
import fnmatch
import re
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any


def load_json_config(lakehouse_path: str, filename: str) -> dict:
    file_path = f"{lakehouse_path}/Files/config/{filename}"
    if file_path.startswith("abfss://"):
        content = mssparkutils.fs.head(file_path, 1000000)
    else:
        with open(file_path, "r") as f:
            content = f.read()
    return json.loads(content)

def load_retention_policies(lakehouse_path: str) -> dict:
    return load_json_config(lakehouse_path, "retention_policies.json")

def load_exceptions(lakehouse_path: str) -> dict:
    return load_json_config(lakehouse_path, "exceptions.json")

def get_enabled_policies(config, business_unit=None, schedule=None):
    results = []
    for bu in config.get("business_units", []):
        bu_name = bu["business_unit"]
        if business_unit and bu_name.lower() != business_unit.lower():
            continue
        for policy in bu.get("policies", []):
            if not policy.get("enabled", True):
                continue
            if schedule and policy.get("schedule", "").lower() != schedule.lower():
                continue
            results.append({"business_unit": bu_name, "owner": bu.get("owner"), **policy})
    return results

def is_item_excluded(item_identifier, item_type, policy_id, exceptions, current_date=None):
    if current_date is None:
        current_date = datetime.utcnow()
    for exc in exceptions.get("exceptions", []):
        if exc["policy_id"] != "*" and exc["policy_id"] != policy_id:
            continue
        if exc["item_type"] != item_type:
            continue
        if not exc.get("permanent", False) and exc.get("expiry_date"):
            expiry = datetime.strptime(exc["expiry_date"], "%Y-%m-%d")
            if current_date > expiry:
                continue
        if fnmatch.fnmatch(item_identifier, exc["item_identifier"]):
            return True, exc.get("reason", "No reason provided")
    return False, None

def calculate_cutoff_date(policy, current_date=None):
    if current_date is None:
        current_date = datetime.utcnow()
    retention_days = policy["retention"]["period_days"]
    grace_days = policy["retention"].get("grace_period_days", 0)
    return current_date - timedelta(days=retention_days + grace_days)

def matches_pattern(name, pattern):
    patterns = [p.strip() for p in pattern.split("|")]
    return any(fnmatch.fnmatch(name, p) for p in patterns)

def matches_file_extension(filename, extensions):
    if not extensions:
        return True
    return any(filename.lower().endswith(ext.lower()) for ext in extensions)

def build_action_item(policy, item_name, item_path, item_type, last_modified, size_bytes=None, metadata=None):
    return {
        "policy_id": policy["policy_id"], "policy_name": policy["policy_name"],
        "business_unit": policy["business_unit"], "action": policy["action"],
        "item_name": item_name, "item_path": item_path, "item_type": item_type,
        "last_modified": last_modified.isoformat(), "size_bytes": size_bytes,
        "source_config": policy["source_config"],
        "archive_config": policy.get("archive_config"), "metadata": metadata or {}
    }

def get_fabric_headers():
    token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def get_workspace_id(workspace_name):
    headers = get_fabric_headers()
    response = requests.get("https://api.fabric.microsoft.com/v1/workspaces", headers=headers)
    response.raise_for_status()
    for ws in response.json().get("value", []):
        if ws["displayName"].lower() == workspace_name.lower():
            return ws["id"]
    raise ValueError(f"Workspace '{workspace_name}' not found")

def get_lakehouse_id(workspace_id, lakehouse_name):
    headers = get_fabric_headers()
    response = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses", headers=headers)
    response.raise_for_status()
    for lh in response.json().get("value", []):
        if lh["displayName"].lower() == lakehouse_name.lower():
            return lh["id"]
    raise ValueError(f"Lakehouse '{lakehouse_name}' not found in workspace {workspace_id}")

# ── Local path helpers (mssparkutils.fs needs abfss://, local paths use Python I/O) ──
import os

def _is_local(path):
    return not path.startswith("abfss://")

def fs_write(path, content, overwrite=True):
    if _is_local(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(content)
    else:
        mssparkutils.fs.put(path, content, overwrite)

def fs_read(path, max_bytes=1000000):
    if _is_local(path):
        with open(path, "r") as f:
            return f.read()
    else:
        return mssparkutils.fs.head(path, max_bytes)

def fs_ls(path):
    if _is_local(path):
        import types
        results = []
        if not os.path.exists(path):
            return results
        for name in os.listdir(path):
            full = os.path.join(path, name)
            info = types.SimpleNamespace()
            info.name = name
            info.path = full
            info.isDir = os.path.isdir(full)
            info.size = os.path.getsize(full) if not info.isDir else 0
            info.modifyTime = int(os.path.getmtime(full) * 1000)
            results.append(info)
        return results
    else:
        return mssparkutils.fs.ls(path)

def fs_rm(path, recurse=False):
    if _is_local(path):
        import shutil
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)
    else:
        mssparkutils.fs.rm(path, recurse)

print("✅ Config loader utilities loaded successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    TimestampType, BooleanType
)
from pyspark.sql.functions import lit, current_timestamp, col

spark = SparkSession.builder.getOrCreate()

# Load global settings for audit table location
policies_config = load_retention_policies(config_lakehouse_path)
global_settings = policies_config.get("global_settings", {})

audit_lakehouse = global_settings.get("audit_lakehouse", "RetentionAudit")
audit_table_name = global_settings.get("audit_table", "retention_audit_log")

print(f"{'='*60}")
print(f"AUDIT LOGGER")
print(f"{'='*60}")
print(f"Run ID:          {run_id}")
print(f"Pipeline status: {pipeline_status}")
print(f"Audit table:     {audit_lakehouse}.{audit_table_name}")
print(f"{'='*60}\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

AUDIT_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("policy_id", StringType(), False),
    StructField("business_unit", StringType(), True),
    StructField("item_name", StringType(), False),
    StructField("item_path", StringType(), True),
    StructField("item_type", StringType(), True),
    StructField("requested_action", StringType(), True),
    StructField("actual_action", StringType(), True),
    StructField("status", StringType(), False),
    StructField("details", StringType(), True),
    StructField("dry_run", BooleanType(), True),
    StructField("last_modified", StringType(), True),
    StructField("size_bytes", LongType(), True),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ensure_audit_table_exists():
    """Create the audit Delta table if it doesn't already exist."""
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {audit_table_name} (
                run_id STRING NOT NULL,
                timestamp STRING NOT NULL,
                policy_id STRING NOT NULL,
                business_unit STRING,
                item_name STRING NOT NULL,
                item_path STRING,
                item_type STRING,
                requested_action STRING,
                actual_action STRING,
                status STRING NOT NULL,
                details STRING,
                dry_run BOOLEAN,
                last_modified STRING,
                size_bytes BIGINT,
                pipeline_status STRING,
                ingested_at TIMESTAMP
            )
            USING DELTA
            COMMENT 'Retention pipeline audit log — tracks all retention actions taken'
            TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
        """)
        print(f"✅ Audit table '{audit_table_name}' is ready")
    except Exception as e:
        print(f"⚠️ Error ensuring audit table: {str(e)}")
        raise

ensure_audit_table_exists()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

audit_records_path = f"{config_lakehouse_path}/Files/pipeline_runs/{run_id}/audit/"

print(f"\n🔄 Loading audit records from: {audit_records_path}")

try:
    # List all audit JSON files for this run
    audit_files = fs_ls(audit_records_path)
    json_files = [f for f in audit_files if f.name.endswith(".json")]
    
    print(f"   Found {len(json_files)} audit records")
    
    if len(json_files) == 0:
        print("   ℹ️ No items were processed in this run")
        # Create a summary record for the empty run
        empty_record = {
            "run_id": run_id,
            "timestamp": datetime.utcnow().isoformat(),
            "policy_id": "N/A",
            "business_unit": "N/A",
            "item_name": "NO_ITEMS_FOUND",
            "item_path": "",
            "item_type": "",
            "requested_action": "none",
            "actual_action": "none",
            "status": "no_candidates",
            "details": "Discovery found no items matching retention policies",
            "dry_run": True,
            "last_modified": "",
            "size_bytes": None
        }
        records = [empty_record]
    else:
        # Read all audit JSON files
        records = []
        for f in json_files:
            content = fs_read(f.path)
            record = json.loads(content)
            records.append(record)
    
    print(f"   Parsed {len(records)} records")

except Exception as e:
    print(f"   ❌ Error loading audit records: {str(e)}")
    # Create an error record
    records = [{
        "run_id": run_id,
        "timestamp": datetime.utcnow().isoformat(),
        "policy_id": "SYSTEM",
        "business_unit": "SYSTEM",
        "item_name": "AUDIT_COLLECTION_ERROR",
        "item_path": "",
        "item_type": "",
        "requested_action": "none",
        "actual_action": "none",
        "status": "error",
        "details": f"Failed to collect audit records: {str(e)}",
        "dry_run": True,
        "last_modified": "",
        "size_bytes": None
    }]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"\n📝 Writing {len(records)} records to audit table...")

# Convert to DataFrame
audit_df = spark.createDataFrame(records, schema=AUDIT_SCHEMA)

# Add pipeline-level metadata
audit_df = audit_df \
    .withColumn("pipeline_status", lit(pipeline_status)) \
    .withColumn("ingested_at", current_timestamp())

# Append to the audit table
audit_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(audit_table_name)

print(f"✅ {len(records)} records written to {audit_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"\n{'='*60}")
print(f"RUN SUMMARY — {run_id}")
print(f"{'='*60}")

# Count by status
status_counts = {}
action_counts = {}
bu_counts = {}

for r in records:
    status = r.get("status", "unknown")
    status_counts[status] = status_counts.get(status, 0) + 1
    
    action = r.get("actual_action", "unknown")
    action_counts[action] = action_counts.get(action, 0) + 1
    
    bu = r.get("business_unit", "unknown")
    bu_counts[bu] = bu_counts.get(bu, 0) + 1

print(f"\nBy Status:")
for status, count in sorted(status_counts.items()):
    icon = {"success": "✅", "dry_run": "🔍", "error": "❌", "skipped": "⏭️"}.get(status, "❓")
    print(f"   {icon} {status}: {count}")

print(f"\nBy Action:")
for action, count in sorted(action_counts.items()):
    print(f"   {action}: {count}")

print(f"\nBy Business Unit:")
for bu, count in sorted(bu_counts.items()):
    print(f"   {bu}: {count}")

# Total size of items processed
total_size = sum(r.get("size_bytes", 0) or 0 for r in records)
if total_size > 0:
    size_mb = total_size / (1024 * 1024)
    print(f"\nTotal size processed: {size_mb:.2f} MB")

print(f"\n{'='*60}")
print(f"Pipeline status: {pipeline_status}")
print(f"Completed at: {datetime.utcnow().isoformat()}")
print(f"{'='*60}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean up the per-run JSON files after consolidation
try:
    run_path = f"{config_lakehouse_path}/Files/pipeline_runs/{run_id}"
    # Keep the discovery_results.json for debugging, clean up audit JSONs
    fs_rm(audit_records_path, recurse=True)
    print(f"\n🧹 Cleaned up temp audit files from {audit_records_path}")
except Exception as e:
    print(f"   ⚠️ Cleanup warning: {str(e)}")


# Return summary
summary = {
    "run_id": run_id,
    "total_records": len(records),
    "status_counts": status_counts,
    "pipeline_status": pipeline_status
}
mssparkutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
