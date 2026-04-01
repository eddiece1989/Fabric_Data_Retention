# Fabric Data Retention Reporting — Workflow

## Workflow Diagram

```mermaid
flowchart LR
    subgraph AUTH["AUTHENTICATION"]
        direction TB
        AU1["Fabric Credential\n(user identity)"]
        AU2["Service Principal\n(SP + MSAL)"]
    end

    subgraph APIS["DATA SOURCES (APIs)"]
        direction TB
        A1["Fabric REST API\n/v1/workspaces\n/v1/workspaces/{id}/items"]
        A1A["Fabric Admin API\n/v1/admin/workspaces\n/v1/admin/items"]
        A2["PBI Admin Scanner\n/admin/workspaces/getInfo"]
        A3["Activity Events API\n/admin/activityevents\n(28-day window)"]
        A4["Lakehouse Tables API\n/v1/workspaces/{id}/lakehouses/{id}/tables"]
        A5["OneLake DFS API\nhttps://onelake.dfs.fabric.microsoft.com"]
        A6["Warehouse SQL Metadata\nSpark SQL: sys.objects"]
    end

    subgraph NB01["NOTEBOOK 01 — Workspace Inventory"]
        B1["Scan all workspaces"]
        B2["Catalog all items\n+ sub-items"]
        B3["Collect modification\ndates from 5 APIs"]
        B4["Apply retention\nperiod per type"]
    end

    subgraph NB02["NOTEBOOK 02 — Activity Log Collector"]
        C1["Check which days\nalready collected"]
        C2["Fetch new activity\nevents (incremental)"]
        C3["Filter out 30+\nread-only events"]
        C4["Compute per-item\nlast modified date"]
    end

    subgraph LAKE["DELTA LAKEHOUSE (RetentionConfig)"]
        direction TB
        T1[("workspace_inventory\n(overwrite)")]
        T2[("activity_events_raw\n(append)")]
        T3[("activity_last_modified\n(overwrite)")]
        T4[("retention_config\n(overwrite)")]
        T5[("retention_readiness\n(overwrite)")]
    end

    subgraph NB03["NOTEBOOK 03 — Retention Config & Readiness"]
        D1["Discover item types\nfrom inventory"]
        D2["Build retention\nconfig table"]
        D3["Join inventory +\nactivity + config"]
        D4["Score each item:\n🔴 Exceeds / 🟢 Within / ⚪ No date"]
    end

    subgraph OUT["OUTPUT"]
        R1["Delta table:\nretention_readiness"]
        R2["Excel export:\nretention_readiness_report.xlsx"]
    end

    AU1 --> A1
    AU2 --> A1A
    A1 --> B1
    A1A --> B1
    A2 --> B3
    A3 --> B3
    A3 --> C2
    A4 --> B2
    A5 --> B2
    A6 --> B2

    B1 --> B2 --> B3 --> B4
    B4 --> T1

    C1 --> C2 --> C3 --> C4
    C4 --> T2
    C4 --> T3

    T1 --> D1
    T3 --> D3
    D1 --> D2 --> D3 --> D4
    D4 --> T4
    D4 --> T5

    T5 --> R1
    T5 --> R2

    style AUTH fill:#34495E,color:#fff
    style APIS fill:#4A90D9,color:#fff
    style NB01 fill:#2ECC71,color:#fff
    style NB02 fill:#F39C12,color:#fff
    style LAKE fill:#8E44AD,color:#fff
    style NB03 fill:#E74C3C,color:#fff
    style OUT fill:#1ABC9C,color:#fff
```

---

## Pipeline Execution

```mermaid
flowchart LR
    P["retention_reporting_pipeline"] --> N1["Notebook 01\nWorkspace Inventory"]
    N1 --> N2["Notebook 02\nActivity Log Collector"]
    N2 --> N3["Notebook 03\nRetention Config\n& Readiness"]

    style P fill:#34495E,color:#fff
    style N1 fill:#2ECC71,color:#fff
    style N2 fill:#F39C12,color:#fff
    style N3 fill:#E74C3C,color:#fff
```

The pipeline runs all three notebooks in sequence: **01 → 02 → 03**. If any notebook fails, the pipeline stops and does not run subsequent notebooks on stale data.

---

## Step-by-Step Explanation

### Data Sources — 6 APIs (Blue)

| # | API | Scope | What It Provides |
|---|-----|-------|-----------------|
| 1 | Fabric REST API | `/v1/workspaces`, `/v1/workspaces/{id}/items` | Lists all workspaces and items (no dates) |
| 2 | PBI Admin Scanner | `/admin/workspaces/getInfo` | createdDateTime, modifiedDateTime for Reports, Datasets, Dataflows, Dashboards, Datamarts |
| 3 | Activity Events API | `/admin/activityevents` | User activity events for the last 28 days (edits, creates, deletes) |
| 4 | Lakehouse Tables API | `/v1/workspaces/{id}/lakehouses/{id}/tables` | Table names, types, formats, lastUpdatedTimestamp |
| 5 | OneLake DFS API | `https://onelake.dfs.fabric.microsoft.com` | File paths with lastModified dates (recursive scan of Files/ and Tables/ directories) |
| 6 | Warehouse SQL Metadata | Spark SQL on `sys.objects` | create_date and modify_date for tables and views |

### Notebook 01 — Workspace Inventory (Green)
- Scans **all accessible workspaces** via the Fabric REST API
- Catalogs every item: name, type, ID, workspace
- Collects modification dates from the **PBI Admin Scanner** (Reports, Datasets) and **Activity Events API** (Notebooks, Pipelines, Lakehouses)
- Discovers **sub-items** inside Lakehouses (tables via Lakehouse Tables API, files via OneLake DFS) and Warehouses (tables/views via sys.objects)
- Applies **retention period** per item type from the `RETENTION_DAYS_BY_TYPE` dictionary in Cell 4 (single source of truth)
- **Overwrites** the `workspace_inventory` Delta table on each run

### Notebook 02 — Activity Log Collector (Orange)
- Fetches activity events from the **Activity Events API** (28-day retention window)
- Runs **incrementally** — checks which days are already collected, only fetches new days
- **Filters out 30+ read-only event types** (views, exports, downloads) so viewing an item doesn't reset its retention clock
- Appends raw events to `activity_events_raw` (preserves history beyond the 28-day API window)
- Computes `activity_last_modified` — one row per item with its most recent modification date

### Notebook 03 — Retention Config & Readiness Report (Red)
- Discovers all item types from the inventory and builds the `retention_config` table
- **Three-way join**: inventory (what exists) + activity (when last modified) + config (how long to keep)
- **COALESCE priority** for dates: activity_id_match → activity_name_match → fabric_api → created_date
- Scores each item:
  - 🔴 **EXCEEDS RETENTION** — not modified within the retention window
  - 🟢 **Within retention** — recently modified
  - ⚪ **No date** — cannot assess (no modification data available)
- Saves `retention_readiness` Delta table and exports to Excel

### Delta Lakehouse — RetentionConfig (Purple)

| Table | Write Mode | Source | Description |
|-------|-----------|--------|-------------|
| `workspace_inventory` | Overwrite | NB01 | Full catalog of all workspace items with dates and retention period |
| `activity_events_raw` | Append | NB02 | Raw activity events (incremental, preserves history) |
| `activity_last_modified` | Overwrite | NB02 | One row per artifact with last modification date |
| `retention_config` | Overwrite | NB03 | Retention period rules per item type |
| `retention_readiness` | Overwrite | NB03 | Final scored readiness report |

### Output (Teal)
- **Delta table** `retention_readiness` — queryable from Spark SQL or Power BI
- **Excel export** `Files/exports/retention_readiness_report.xlsx` — downloadable from the Lakehouse file browser

---

## Authentication

All API calls use `mssparkutils.credentials.getToken()` with the identity of whoever runs the notebook:

| Token Scope | Used For |
|-------------|----------|
| `https://api.fabric.microsoft.com` | Fabric REST API, Lakehouse Tables API |
| `https://analysis.windows.net/powerbi/api` | PBI Admin Scanner, Activity Events API |
| `https://storage.azure.com/` | OneLake DFS API |

For production/scheduled runs, switch to a **service principal** or **managed identity** so notebooks can run unattended.
