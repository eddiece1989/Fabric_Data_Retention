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

# # 01_discovery
# Discovers items matching retention policies and returns a list
# of candidates for retention action. Called by the dynamic pipeline.

# CELL ********************

# Pipeline parameters (these are overridden by the pipeline at runtime)
config_lakehouse_path = "/lakehouse/default"  # Default for interactive testing
business_unit = ""  # Empty = all business units
schedule = ""       # Empty = all schedules
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
    raise ValueError(f"Lakehouse '{lakehouse_name}' not found in workspace {workspace_id}")# ── Local path helpers (mssparkutils.fs needs abfss://, local paths use Python I/O) ──
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
from datetime import datetime
from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()

# Load configs
print(f"🔄 Loading retention policies from: {config_lakehouse_path}")
policies_config = load_retention_policies(config_lakehouse_path)
exceptions_config = load_exceptions(config_lakehouse_path)

global_settings = policies_config.get("global_settings", {})
dry_run = global_settings.get("dry_run", True)

print(f"   Dry-run mode: {dry_run}")
print(f"   Business unit filter: {business_unit or 'ALL'}")
print(f"   Schedule filter: {schedule or 'ALL'}")

# ── Auto-Discovery: scan workspaces and generate dynamic policies ──
auto_disc = policies_config.get("auto_discovery", {})
if auto_disc.get("enabled", False):
    print(f"\n{'='*60}")
    print(f"AUTO-DISCOVERY MODE")
    print(f"{'='*60}")
    ws_patterns = auto_disc.get("workspace_patterns", [])
    scan_targets = auto_disc.get("scan_targets", [])
    exclude_lakehouses = [n.lower() for n in auto_disc.get("exclude_lakehouses", [])]
    exclude_paths = auto_disc.get("exclude_paths", [])
    default_ret = auto_disc.get("default_retention", {"period_days": 1, "based_on": "last_modified", "grace_period_days": 0})
    default_action = auto_disc.get("default_action", "delete")
    default_sched = auto_disc.get("default_schedule", "daily")
    file_exts = auto_disc.get("file_extensions", [".csv", ".parquet", ".json"])
    table_pat = auto_disc.get("table_pattern", "*")
    ad_owner = auto_disc.get("owner", "retention-admin@statefarm.com")

    # 1. List all accessible workspaces
    headers = get_fabric_headers()
    ws_response = requests.get("https://api.fabric.microsoft.com/v1/workspaces", headers=headers)
    ws_response.raise_for_status()
    all_workspaces = ws_response.json().get("value", [])

    # 2. Filter workspaces by patterns (exact or fnmatch)
    matched_workspaces = []
    for ws in all_workspaces:
        ws_name = ws["displayName"]
        for pat in ws_patterns:
            if fnmatch.fnmatch(ws_name, pat) or ws_name == pat:
                matched_workspaces.append(ws)
                break

    print(f"   Workspace patterns: {ws_patterns}")
    print(f"   Matched workspaces: {[w['displayName'] for w in matched_workspaces]}")

    auto_policy_counter = 0
    auto_bu_name = "Auto_Discovery"
    auto_policies = []

    for ws in matched_workspaces:
        ws_name = ws["displayName"]
        ws_id = ws["id"]
        print(f"\n   📡 Scanning workspace: {ws_name} ({ws_id})")

        # 3. List all lakehouses in this workspace
        if "lakehouse_files" in scan_targets or "delta_tables" in scan_targets:
            try:
                lh_resp = requests.get(
                    f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/lakehouses",
                    headers=headers
                )
                lh_resp.raise_for_status()
                lakehouses = lh_resp.json().get("value", [])
                print(f"      Found {len(lakehouses)} lakehouse(s): {[l['displayName'] for l in lakehouses]}")

                for lh in lakehouses:
                    lh_name = lh["displayName"]
                    if lh_name.lower() in exclude_lakehouses:
                        print(f"      ⏭️ Skipping excluded lakehouse: {lh_name}")
                        continue

                    # Generate a lakehouse_files policy for each lakehouse
                    if "lakehouse_files" in scan_targets:
                        auto_policy_counter += 1
                        pol_id = f"AD-F-{auto_policy_counter:04d}"
                        auto_policies.append({
                            "policy_id": pol_id,
                            "policy_name": f"Auto: {ws_name}/{lh_name} Files",
                            "description": f"Auto-discovered file scan for {lh_name} in {ws_name}",
                            "enabled": True,
                            "source_type": "lakehouse_files",
                            "source_config": {
                                "workspace": ws_name,
                                "lakehouse": lh_name,
                                "path": "Files",
                                "file_extensions": file_exts
                            },
                            "retention": dict(default_ret),
                            "action": default_action,
                            "archive_config": None,
                            "schedule": default_sched,
                            "business_unit": auto_bu_name,
                            "owner": ad_owner,
                            "_auto_exclude_paths": exclude_paths
                        })
                        print(f"      ✅ Generated policy {pol_id} — {ws_name}/{lh_name} files")

                    # Generate a delta_tables policy for each lakehouse
                    if "delta_tables" in scan_targets:
                        auto_policy_counter += 1
                        pol_id = f"AD-T-{auto_policy_counter:04d}"
                        auto_policies.append({
                            "policy_id": pol_id,
                            "policy_name": f"Auto: {ws_name}/{lh_name} Tables",
                            "description": f"Auto-discovered table scan for {lh_name} in {ws_name}",
                            "enabled": True,
                            "source_type": "delta_table",
                            "source_config": {
                                "workspace": ws_name,
                                "lakehouse": lh_name,
                                "table_pattern": table_pat,
                                "schema": None
                            },
                            "retention": dict(default_ret),
                            "action": default_action,
                            "archive_config": None,
                            "schedule": default_sched,
                            "business_unit": auto_bu_name,
                            "owner": ad_owner
                        })
                        print(f"      ✅ Generated policy {pol_id} — {ws_name}/{lh_name} tables")

            except Exception as e:
                print(f"      ❌ Error listing lakehouses in {ws_name}: {e}")

        # 4. List all warehouses in this workspace
        if "warehouse_tables" in scan_targets:
            try:
                wh_resp = requests.get(
                    f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/warehouses",
                    headers=headers
                )
                wh_resp.raise_for_status()
                warehouses_list = wh_resp.json().get("value", [])
                print(f"      Found {len(warehouses_list)} warehouse(s): {[w['displayName'] for w in warehouses_list]}")

                for wh in warehouses_list:
                    wh_name = wh["displayName"]
                    auto_policy_counter += 1
                    pol_id = f"AD-W-{auto_policy_counter:04d}"
                    auto_policies.append({
                        "policy_id": pol_id,
                        "policy_name": f"Auto: {ws_name}/{wh_name} Warehouse Tables",
                        "description": f"Auto-discovered warehouse scan for {wh_name} in {ws_name}",
                        "enabled": True,
                        "source_type": "warehouse_table",
                        "source_config": {
                            "workspace": ws_name,
                            "warehouse": wh_name,
                            "table_pattern": table_pat,
                            "schema": None
                        },
                        "retention": dict(default_ret),
                        "action": default_action,
                        "archive_config": None,
                        "schedule": default_sched,
                        "business_unit": auto_bu_name,
                        "owner": ad_owner
                    })
                    print(f"      ✅ Generated policy {pol_id} — {ws_name}/{wh_name} warehouse")

            except Exception as e:
                print(f"      ❌ Error listing warehouses in {ws_name}: {e}")

    print(f"\n   🏁 Auto-discovery generated {len(auto_policies)} dynamic policies")
else:
    auto_policies = []

# Get explicit policies from config
bu_filter = business_unit if business_unit else None
sched_filter = schedule if schedule else None
explicit_policies = get_enabled_policies(policies_config, bu_filter, sched_filter)

# Merge: auto-discovered policies + explicit policies (deduplication: explicit wins)
# Explicit policies take priority — skip auto policies that target the same workspace/lakehouse/path combo
explicit_keys = set()
for p in explicit_policies:
    sc = p.get("source_config", {})
    key = f"{sc.get('workspace','')}/{sc.get('lakehouse', sc.get('warehouse',''))}/{sc.get('path', sc.get('table_pattern',''))}"
    explicit_keys.add(key.lower())

deduplicated_auto = []
for p in auto_policies:
    sc = p.get("source_config", {})
    key = f"{sc.get('workspace','')}/{sc.get('lakehouse', sc.get('warehouse',''))}/{sc.get('path', sc.get('table_pattern',''))}"
    if key.lower() not in explicit_keys:
        deduplicated_auto.append(p)
    else:
        print(f"   ⏭️ Skipping auto policy {p['policy_id']} — explicit policy already covers {key}")

active_policies = explicit_policies + deduplicated_auto

print(f"\n   Explicit policies: {len(explicit_policies)}")
print(f"   Auto-discovered policies: {len(deduplicated_auto)}")
print(f"   Total active policies to evaluate: {len(active_policies)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def discover_lakehouse_files(policy: dict, cutoff_date: datetime) -> list:
    """
    Discover Lakehouse files that exceed retention period.
    Uses mssparkutils.fs to list files and check modification dates.
    """
    candidates = []
    source = policy["source_config"]
    workspace = source["workspace"]
    lakehouse = source["lakehouse"]
    path_pattern = source["path"]
    extensions = source.get("file_extensions", [])
    auto_exclude_paths = policy.get("_auto_exclude_paths", [])
    
    try:
        workspace_id = get_workspace_id(workspace)
        lakehouse_id = get_lakehouse_id(workspace_id, lakehouse)
        
        # Build the abfss path for the lakehouse
        # In Fabric: abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}
        base_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}"
        
        # Extract the directory portion from the pattern (before any wildcards)
        search_dir = path_pattern.split("*")[0].rstrip("/")
        full_search_path = f"{base_path}/{search_dir}"
        
        print(f"   📂 Scanning: {full_search_path}")
        
        # Recursively list files
        files = fs_ls(full_search_path)
        all_files = _recursive_list(full_search_path, files)
        
        for file_info in all_files:
            # Skip directories
            if file_info.isDir:
                continue
            
            # Skip auto-excluded paths (e.g., Files/config)
            if auto_exclude_paths:
                file_rel = file_info.path.replace("\\", "/")
                skip = False
                for exc_path in auto_exclude_paths:
                    if f"/{exc_path}/" in file_rel or file_rel.endswith(f"/{exc_path}"):
                        skip = True
                        break
                if skip:
                    continue
            
            # Check file extension filter
            if not matches_file_extension(file_info.name, extensions):
                continue
            
            # Check modification date
            mod_time = datetime.fromtimestamp(file_info.modifyTime / 1000)
            if mod_time < cutoff_date:
                # Check exceptions
                item_id = f"{workspace}/{lakehouse}/{file_info.path}"
                excluded, reason = is_item_excluded(
                    item_id, "lakehouse_files", 
                    policy["policy_id"], exceptions_config
                )
                
                if excluded:
                    print(f"   ⏭️ EXCLUDED: {file_info.name} — {reason}")
                    continue
                
                candidates.append(build_action_item(
                    policy=policy,
                    item_name=file_info.name,
                    item_path=file_info.path,
                    item_type="lakehouse_files",
                    last_modified=mod_time,
                    size_bytes=file_info.size
                ))
        
        print(f"   Found {len(candidates)} file candidates for policy {policy['policy_id']}")
        
    except Exception as e:
        print(f"   ❌ Error discovering files for {policy['policy_id']}: {str(e)}")
        candidates.append({
            "policy_id": policy["policy_id"],
            "error": str(e),
            "item_type": "lakehouse_files",
            "action": "error"
        })
    
    return candidates


def _recursive_list(base_path: str, items: list, max_depth: int = 10) -> list:
    """Recursively list all files under a path."""
    all_files = []
    if max_depth <= 0:
        return all_files
    
    for item in items:
        if item.isDir:
            try:
                sub_items = fs_ls(item.path)
                all_files.extend(_recursive_list(item.path, sub_items, max_depth - 1))
            except Exception:
                pass  # Skip inaccessible directories
        else:
            all_files.append(item)
    
    return all_files

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def discover_delta_tables(policy: dict, cutoff_date: datetime) -> list:
    """
    Discover Delta tables that exceed retention period.
    Uses Fabric REST API and Spark catalog to inspect tables.
    """
    candidates = []
    source = policy["source_config"]
    workspace = source["workspace"]
    lakehouse = source["lakehouse"]
    table_pattern = source.get("table_pattern", "*")
    
    try:
        workspace_id = get_workspace_id(workspace)
        lakehouse_id = get_lakehouse_id(workspace_id, lakehouse)
        
        # Use Fabric REST API to list tables
        headers = get_fabric_headers()
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables"
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        tables = response.json().get("data", [])
        
        print(f"   📊 Found {len(tables)} tables in {lakehouse}")
        
        for table in tables:
            table_name = table.get("name", "")
            
            # Check name pattern
            if not matches_pattern(table_name, table_pattern):
                continue
            
            # Get table properties to determine last modified
            # Use the table's lastUpdatedTimestamp from the API if available
            last_modified_str = table.get("properties", {}).get("lastUpdatedTimestamp")
            if last_modified_str:
                mod_time = datetime.fromisoformat(last_modified_str.replace("Z", "+00:00")).replace(tzinfo=None)
            else:
                # Fallback: query Delta log for last modification
                try:
                    detail_df = spark.sql(f"DESCRIBE DETAIL `{table_name}`")
                    detail = detail_df.collect()[0]
                    mod_time = datetime.fromtimestamp(detail["lastModified"] / 1000)
                except Exception:
                    print(f"   ⚠️ Cannot determine last modified for {table_name}, skipping")
                    continue
            
            if mod_time < cutoff_date:
                # Check exceptions
                item_id = f"{workspace}/{lakehouse}/{table_name}"
                excluded, reason = is_item_excluded(
                    item_id, "delta_table",
                    policy["policy_id"], exceptions_config
                )
                
                if excluded:
                    print(f"   ⏭️ EXCLUDED: {table_name} — {reason}")
                    continue
                
                candidates.append(build_action_item(
                    policy=policy,
                    item_name=table_name,
                    item_path=f"{workspace}/{lakehouse}/{table_name}",
                    item_type="delta_table",
                    last_modified=mod_time,
                    metadata={"table_location": table.get("location", "")}
                ))
        
        print(f"   Found {len(candidates)} table candidates for policy {policy['policy_id']}")
        
    except Exception as e:
        print(f"   ❌ Error discovering tables for {policy['policy_id']}: {str(e)}")
        candidates.append({
            "policy_id": policy["policy_id"],
            "error": str(e),
            "item_type": "delta_table",
            "action": "error"
        })
    
    return candidates

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def discover_warehouse_tables(policy: dict, cutoff_date: datetime) -> list:
    """
    Discover Warehouse tables that exceed retention period.
    Uses Fabric REST API to enumerate tables in a Warehouse item.
    """
    candidates = []
    source = policy["source_config"]
    workspace = source["workspace"]
    warehouse = source["warehouse"]
    table_pattern = source.get("table_pattern", "*")
    schema = source.get("schema")
    
    try:
        workspace_id = get_workspace_id(workspace)
        headers = get_fabric_headers()
        
        # Get warehouse ID
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/warehouses"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        warehouses = response.json().get("value", [])
        warehouse_id = None
        for wh in warehouses:
            if wh["displayName"].lower() == warehouse.lower():
                warehouse_id = wh["id"]
                break
        
        if not warehouse_id:
            raise ValueError(f"Warehouse '{warehouse}' not found")
        
        # Query warehouse tables using SQL endpoint
        # Build the schema-qualified query
        schema_filter = f"AND TABLE_SCHEMA = '{schema}'" if schema else ""
        query = f"""
            SELECT TABLE_SCHEMA, TABLE_NAME, 
                   STATS_DATE(OBJECT_ID(CONCAT(TABLE_SCHEMA, '.', TABLE_NAME)), 1) as last_updated
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE' {schema_filter}
        """
        
        # Execute via the warehouse SQL connection
        # In Fabric, you can connect to the warehouse SQL endpoint
        warehouse_df = spark.read \
            .format("com.microsoft.sqlserver.jdbc.spark") \
            .option("url", f"jdbc:sqlserver://{warehouse_id}-onelake.sql.fabric.microsoft.com:1433") \
            .option("query", query) \
            .option("authentication", "ActiveDirectoryServicePrincipal") \
            .load()
        
        tables = warehouse_df.collect()
        print(f"   🏭 Found {len(tables)} tables in warehouse {warehouse}")
        
        for row in tables:
            table_name = row["TABLE_NAME"]
            table_schema = row["TABLE_SCHEMA"]
            
            if not matches_pattern(table_name, table_pattern):
                continue
            
            mod_time = row["last_updated"]
            if mod_time and mod_time < cutoff_date:
                item_id = f"{workspace}/{warehouse}/{table_schema}.{table_name}"
                excluded, reason = is_item_excluded(
                    item_id, "warehouse_table",
                    policy["policy_id"], exceptions_config
                )
                
                if excluded:
                    print(f"   ⏭️ EXCLUDED: {table_schema}.{table_name} — {reason}")
                    continue
                
                candidates.append(build_action_item(
                    policy=policy,
                    item_name=f"{table_schema}.{table_name}",
                    item_path=item_id,
                    item_type="warehouse_table",
                    last_modified=mod_time,
                    metadata={"warehouse_id": warehouse_id}
                ))
        
        print(f"   Found {len(candidates)} warehouse table candidates for {policy['policy_id']}")
        
    except Exception as e:
        print(f"   ❌ Error discovering warehouse tables for {policy['policy_id']}: {str(e)}")
        candidates.append({
            "policy_id": policy["policy_id"],
            "error": str(e),
            "item_type": "warehouse_table",
            "action": "error"
        })
    
    return candidates

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def discover_notebooks(policy: dict, cutoff_date: datetime) -> list:
    """
    Discover Fabric notebooks that exceed retention period.
    Uses Fabric REST API to list and inspect notebooks.
    """
    candidates = []
    source = policy["source_config"]
    workspace = source["workspace"]
    name_pattern = source.get("name_pattern", "*")
    
    try:
        workspace_id = get_workspace_id(workspace)
        headers = get_fabric_headers()
        
        # List all notebooks in the workspace
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        notebooks = response.json().get("value", [])
        print(f"   📓 Found {len(notebooks)} notebooks in {workspace}")
        
        for nb in notebooks:
            nb_name = nb.get("displayName", "")
            
            if not matches_pattern(nb_name, name_pattern):
                continue
            
            # Get notebook details for last modified date
            nb_id = nb["id"]
            detail_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks/{nb_id}"
            detail_response = requests.get(detail_url, headers=headers)
            
            if detail_response.status_code == 200:
                nb_detail = detail_response.json()
                last_modified_str = nb_detail.get("lastModifiedDateTime") or nb_detail.get("modifiedDateTime")
                
                if last_modified_str:
                    mod_time = datetime.fromisoformat(last_modified_str.replace("Z", "+00:00")).replace(tzinfo=None)
                else:
                    print(f"   ⚠️ No mod date for notebook {nb_name}, skipping")
                    continue
                
                if mod_time < cutoff_date:
                    item_id = f"{workspace}/{nb_name}"
                    excluded, reason = is_item_excluded(
                        item_id, "notebook",
                        policy["policy_id"], exceptions_config
                    )
                    
                    if excluded:
                        print(f"   ⏭️ EXCLUDED: {nb_name} — {reason}")
                        continue
                    
                    candidates.append(build_action_item(
                        policy=policy,
                        item_name=nb_name,
                        item_path=item_id,
                        item_type="notebook",
                        last_modified=mod_time,
                        metadata={
                            "notebook_id": nb_id,
                            "workspace_id": workspace_id
                        }
                    ))
        
        print(f"   Found {len(candidates)} notebook candidates for {policy['policy_id']}")
        
    except Exception as e:
        print(f"   ❌ Error discovering notebooks for {policy['policy_id']}: {str(e)}")
        candidates.append({
            "policy_id": policy["policy_id"],
            "error": str(e),
            "item_type": "notebook",
            "action": "error"
        })
    
    return candidates

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dispatcher: maps source_type to discovery function
DISCOVERY_HANDLERS = {
    "lakehouse_files": discover_lakehouse_files,
    "delta_table": discover_delta_tables,
    "warehouse_table": discover_warehouse_tables,
    "notebook": discover_notebooks,
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"\n{'='*60}")
print(f"RETENTION DISCOVERY RUN")
print(f"Run ID: {run_id}")
print(f"Timestamp: {datetime.utcnow().isoformat()}")
print(f"Policies to evaluate: {len(active_policies)}")
print(f"{'='*60}\n")

all_candidates = []

for policy in active_policies:
    policy_id = policy["policy_id"]
    source_type = policy["source_type"]
    
    print(f"\n🔍 Evaluating policy: {policy_id} — {policy['policy_name']}")
    print(f"   Business unit: {policy['business_unit']}")
    print(f"   Source type: {source_type}")
    print(f"   Action: {policy['action']}")
    
    # Calculate cutoff date
    cutoff = calculate_cutoff_date(policy)
    print(f"   Cutoff date: {cutoff.strftime('%Y-%m-%d')}")
    
    # Get the discovery handler
    handler = DISCOVERY_HANDLERS.get(source_type)
    if not handler:
        print(f"   ⚠️ No discovery handler for source type: {source_type}")
        continue
    
    # Run discovery
    candidates = handler(policy, cutoff)
    all_candidates.extend(candidates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"\n{'='*60}")
print(f"DISCOVERY SUMMARY")
print(f"{'='*60}")
print(f"Total candidates found: {len(all_candidates)}")

# Break down by action type
action_counts = {}
for c in all_candidates:
    action = c.get("action", "unknown")
    action_counts[action] = action_counts.get(action, 0) + 1

for action, count in action_counts.items():
    print(f"   {action}: {count}")

# Break down by business unit
bu_counts = {}
for c in all_candidates:
    bu = c.get("business_unit", "unknown")
    bu_counts[bu] = bu_counts.get(bu, 0) + 1

for bu, count in bu_counts.items():
    print(f"   {bu}: {count}")

# Save candidates as JSON for the pipeline ForEach activity
output_json = json.dumps(all_candidates, default=str)

# Write to a temp location for the pipeline to pick up
output_path = f"{config_lakehouse_path}/Files/pipeline_runs/{run_id}/discovery_results.json"
fs_write(output_path, output_json)
print(f"\n📄 Results written to: {output_path}")

# Also set as notebook output for pipeline activity
mssparkutils.notebook.exit(output_json)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
