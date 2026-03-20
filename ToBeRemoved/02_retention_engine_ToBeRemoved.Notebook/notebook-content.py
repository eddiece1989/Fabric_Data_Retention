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

# # 02_retention_engine
# Executes retention actions (delete, archive, log-only) on items
# identified by the discovery notebook. Called by the pipeline
# ForEach activity — receives a single item per invocation.

# CELL ********************

action_item_json = "{}"  # Overridden by pipeline
config_lakehouse_path = "/lakehouse/default"
dry_run = "true"  # String from pipeline parameter
run_id = "manual-test-001"

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
import requests
import shutil
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

spark = SparkSession.builder.getOrCreate()

# Parse the action item
action_item = json.loads(action_item_json)
is_dry_run = dry_run.lower() == "true"

policy_id = action_item.get("policy_id", "UNKNOWN")
item_name = action_item.get("item_name", "UNKNOWN")
item_path = action_item.get("item_path", "")
item_type = action_item.get("item_type", "")
action = action_item.get("action", "")
source_config = action_item.get("source_config", {})
archive_config = action_item.get("archive_config")
metadata = action_item.get("metadata", {})

print(f"{'='*60}")
print(f"RETENTION ENGINE")
print(f"{'='*60}")
print(f"Run ID:        {run_id}")
print(f"Policy:        {policy_id}")
print(f"Item:          {item_name}")
print(f"Type:          {item_type}")
print(f"Action:        {action}")
print(f"Dry-run:       {is_dry_run}")
print(f"{'='*60}\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def action_delete_lakehouse_file(item: dict, dry_run: bool) -> dict:
    """Delete a file from a Lakehouse Files area."""
    file_path = item["item_path"]
    result = {
        "status": "success",
        "action_taken": "delete",
        "details": f"File: {file_path}"
    }
    
    if dry_run:
        print(f"   🔍 DRY RUN — Would delete file: {file_path}")
        result["status"] = "dry_run"
        result["action_taken"] = "delete (dry-run)"
    else:
        try:
            fs_rm(file_path)
            print(f"   🗑️ DELETED file: {file_path}")
        except Exception as e:
            print(f"   ❌ Failed to delete file: {file_path} — {str(e)}")
            result["status"] = "error"
            result["details"] = str(e)
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def action_archive_lakehouse_file(item: dict, dry_run: bool) -> dict:
    """Archive a file: copy to archive destination, then delete original."""
    file_path = item["item_path"]
    archive_cfg = item.get("archive_config", {})
    result = {
        "status": "success",
        "action_taken": "archive",
        "details": ""
    }
    
    if not archive_cfg:
        result["status"] = "error"
        result["details"] = "No archive_config specified for archive action"
        return result
    
    # Build archive destination path
    destination = archive_cfg.get("destination", "adls")
    
    if destination == "adls":
        adls_account = archive_cfg["adls_account"]
        container = archive_cfg["container"]
        preserve_structure = archive_cfg.get("preserve_folder_structure", True)
        
        if preserve_structure:
            # Keep the relative path structure in the archive
            relative_path = file_path.split("/Files/", 1)[-1] if "/Files/" in file_path else item["item_name"]
        else:
            relative_path = item["item_name"]
        
        archive_path = f"abfss://{container}@{adls_account}.dfs.core.windows.net/archive/{relative_path}"
        result["details"] = f"Archived to: {archive_path}"
        
        if dry_run:
            print(f"   🔍 DRY RUN — Would archive file:")
            print(f"      Source:      {file_path}")
            print(f"      Destination: {archive_path}")
            result["status"] = "dry_run"
            result["action_taken"] = "archive (dry-run)"
        else:
            try:
                # Copy to archive
                mssparkutils.fs.cp(file_path, archive_path, recurse=False)
                print(f"   📦 ARCHIVED: {file_path} → {archive_path}")
                
                # Delete original
                fs_rm(file_path)
                print(f"   🗑️ DELETED original: {file_path}")
            except Exception as e:
                print(f"   ❌ Archive failed: {str(e)}")
                result["status"] = "error"
                result["details"] = str(e)
    else:
        result["status"] = "error"
        result["details"] = f"Unsupported archive destination: {destination}"
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def action_delete_delta_table(item: dict, dry_run: bool) -> dict:
    """Drop a Delta table from a Lakehouse."""
    table_name = item["item_name"]
    source = item.get("source_config", {})
    workspace = source.get("workspace", "")
    lakehouse = source.get("lakehouse", "")
    
    result = {
        "status": "success",
        "action_taken": "delete",
        "details": f"Table: {workspace}/{lakehouse}/{table_name}"
    }
    
    if dry_run:
        print(f"   🔍 DRY RUN — Would drop table: {table_name}")
        result["status"] = "dry_run"
        result["action_taken"] = "delete (dry-run)"
    else:
        try:
            # Use Fabric REST API to delete the table
            workspace_id = get_workspace_id(workspace)
            lakehouse_id = get_lakehouse_id(workspace_id, lakehouse)
            headers = get_fabric_headers()
            
            url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables/{table_name}"
            response = requests.delete(url, headers=headers)
            
            if response.status_code in [200, 204]:
                print(f"   🗑️ DROPPED table: {table_name}")
            else:
                # Fallback: try Spark SQL
                spark.sql(f"DROP TABLE IF EXISTS `{table_name}`")
                print(f"   🗑️ DROPPED table via SQL: {table_name}")
                
        except Exception as e:
            print(f"   ❌ Failed to drop table: {table_name} — {str(e)}")
            result["status"] = "error"
            result["details"] = str(e)
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def action_delete_warehouse_table(item: dict, dry_run: bool) -> dict:
    """Drop a table from a Fabric Warehouse."""
    table_name = item["item_name"]  # schema.table_name format
    warehouse_id = item.get("metadata", {}).get("warehouse_id", "")
    
    result = {
        "status": "success",
        "action_taken": "delete",
        "details": f"Warehouse table: {table_name}"
    }
    
    if dry_run:
        print(f"   🔍 DRY RUN — Would drop warehouse table: {table_name}")
        result["status"] = "dry_run"
        result["action_taken"] = "delete (dry-run)"
    else:
        try:
            # Execute DROP TABLE via the warehouse SQL endpoint
            drop_query = f"DROP TABLE IF EXISTS {table_name}"
            
            spark.read \
                .format("com.microsoft.sqlserver.jdbc.spark") \
                .option("url", f"jdbc:sqlserver://{warehouse_id}-onelake.sql.fabric.microsoft.com:1433") \
                .option("query", drop_query) \
                .option("authentication", "ActiveDirectoryServicePrincipal") \
                .load()
            
            print(f"   🗑️ DROPPED warehouse table: {table_name}")
            
        except Exception as e:
            print(f"   ❌ Failed to drop warehouse table: {table_name} — {str(e)}")
            result["status"] = "error"
            result["details"] = str(e)
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def action_delete_notebook(item: dict, dry_run: bool) -> dict:
    """Delete a Fabric notebook via REST API."""
    nb_name = item["item_name"]
    nb_id = item.get("metadata", {}).get("notebook_id", "")
    ws_id = item.get("metadata", {}).get("workspace_id", "")
    
    result = {
        "status": "success",
        "action_taken": "delete",
        "details": f"Notebook: {nb_name} (ID: {nb_id})"
    }
    
    if dry_run:
        print(f"   🔍 DRY RUN — Would delete notebook: {nb_name}")
        result["status"] = "dry_run"
        result["action_taken"] = "delete (dry-run)"
    else:
        try:
            headers = get_fabric_headers()
            url = f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/notebooks/{nb_id}"
            
            response = requests.delete(url, headers=headers)
            
            if response.status_code in [200, 204]:
                print(f"   🗑️ DELETED notebook: {nb_name}")
            else:
                print(f"   ❌ API returned {response.status_code}: {response.text}")
                result["status"] = "error"
                result["details"] = f"HTTP {response.status_code}: {response.text}"
                
        except Exception as e:
            print(f"   ❌ Failed to delete notebook: {nb_name} — {str(e)}")
            result["status"] = "error"
            result["details"] = str(e)
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Map (item_type, action) to handler functions
ACTION_HANDLERS = {
    ("lakehouse_files", "delete"):    action_delete_lakehouse_file,
    ("lakehouse_files", "archive"):   action_archive_lakehouse_file,
    ("delta_table", "delete"):        action_delete_delta_table,
    ("delta_table", "archive"):       action_archive_lakehouse_file,  # Archive reuses file logic
    ("warehouse_table", "delete"):    action_delete_warehouse_table,
    ("notebook", "delete"):           action_delete_notebook,
}


def execute_action(item: dict, dry_run: bool) -> dict:
    """
    Execute the appropriate retention action for an item.
    Returns a result dict with status, action_taken, and details.
    """
    item_type = item.get("item_type", "")
    action = item.get("action", "")
    
    # Handle error items from discovery
    if action == "error":
        return {
            "status": "skipped",
            "action_taken": "none",
            "details": f"Discovery error: {item.get('error', 'unknown')}"
        }
    
    handler_key = (item_type, action)
    handler = ACTION_HANDLERS.get(handler_key)
    
    if not handler:
        msg = f"No handler for ({item_type}, {action})"
        print(f"   ⚠️ {msg}")
        return {
            "status": "skipped",
            "action_taken": "none",
            "details": msg
        }
    
    return handler(item, dry_run)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = execute_action(action_item, is_dry_run)

print(f"\n{'='*60}")
print(f"RESULT")
print(f"{'='*60}")
print(f"Status:       {result['status']}")
print(f"Action taken: {result['action_taken']}")
print(f"Details:      {result['details']}")

# Build audit record
audit_record = {
    "run_id": run_id,
    "timestamp": datetime.utcnow().isoformat(),
    "policy_id": policy_id,
    "business_unit": action_item.get("business_unit", ""),
    "item_name": item_name,
    "item_path": item_path,
    "item_type": item_type,
    "requested_action": action,
    "actual_action": result["action_taken"],
    "status": result["status"],
    "details": result["details"],
    "dry_run": is_dry_run,
    "last_modified": action_item.get("last_modified", ""),
    "size_bytes": action_item.get("size_bytes")
}

# Output audit record for the audit logger
audit_json = json.dumps(audit_record, default=str)

# Write individual audit record
audit_path = f"{config_lakehouse_path}/Files/pipeline_runs/{run_id}/audit/{policy_id}_{item_name}.json"
try:
    fs_write(audit_path, audit_json)
except Exception as e:
    print(f"   ⚠️ Could not write audit file: {str(e)}")

# Return result to pipeline
mssparkutils.notebook.exit(audit_json)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
