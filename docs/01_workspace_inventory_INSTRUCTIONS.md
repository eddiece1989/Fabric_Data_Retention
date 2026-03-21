# Notebook 01 — Workspace Inventory

## What Does This Notebook Do?

This notebook creates a **complete inventory of every item across all your Microsoft Fabric workspaces**. Think of it as taking a snapshot of everything your organization has — every report, dataset, notebook, pipeline, lakehouse, and more — along with when each item was last created or modified.

## Why Is This Important?

Before you can decide what's old and should be cleaned up, you need to know **what you have** and **when it was last touched**. This notebook answers those questions by scanning every workspace your account can see.

## How Does It Work? (Step by Step)

### Step 1 — Connect and List Workspaces
The notebook connects to Microsoft Fabric using your account credentials and retrieves a list of **all workspaces** you have access to. This includes shared team workspaces as well as personal workspaces.

### Step 2 — Get Modification Dates (Two Sources)
Getting accurate "last modified" dates in Fabric is tricky because **no single source has all the dates**. This notebook pulls from two different sources:

- **PBI Admin Scanner** — This is a Microsoft API that returns creation and modification dates for classic Power BI items like Reports, Datasets, Dashboards, and Dataflows. It works well for these item types but does **not** cover newer Fabric items.

- **Activity Events API** — This is a second Microsoft API that records user actions (like editing a notebook or updating a pipeline). The notebook looks at these activity records to determine when Fabric-native items (Notebooks, Pipelines, Lakehouses, etc.) were last modified. It only counts **real changes** — simply viewing or opening an item does not count as a modification.

### Step 3 — Set the Retention Period
Before building the inventory, the notebook sets the **retention period** in Cell 4. This is the **single source of truth** for how many days an item can go without being modified before it's considered overdue. The default is **10 days** for demo purposes. You can customize retention per item type (e.g., 90 days for Reports, 180 days for SemanticModels) using the `RETENTION_DAYS_BY_TYPE` dictionary in that same cell.

Notebook 03 reads this value from the inventory table — it does **not** have its own retention setting.

### Step 4 — Build the Inventory Table
The notebook combines everything into a single table called `workspace_inventory`. For **each item**, the table records:

| Column | What It Means |
|--------|--------------|
| **workspace_name** | Which workspace the item lives in |
| **item_name** | The name of the item (e.g., "Sales Report Q4") |
| **item_type** | What kind of item it is (Report, Notebook, Pipeline, LakehouseTable, LakehouseFile, etc.) |
| **item_id** | A unique identifier for the item |
| **created_date** | When the item was first created |
| **last_modified_date** | The most recent date someone made a change to it |
| **date_source** | Where the date came from (PBI Scanner, Activity Events, OneLake DFS, etc.) |
| **age_days** | How many days since the item was last modified (or created) |
| **retention_period_days** | The retention period assigned to this item type |
| **deletion_due_date** | The date when this item would be considered overdue |
| **is_overdue** | Whether the item has exceeded its retention period |

### Step 5 — Discover Sub-Items Inside Lakehouses & Warehouses
The notebook goes one level deeper by scanning **inside** each Lakehouse and Warehouse to discover:
- **Lakehouse Tables** — via the Fabric Tables API, with a fallback to OneLake DFS for schemas-enabled lakehouses
- **Lakehouse Files** — via the OneLake DFS API (recursive scan of the Files/ directory)
- **Warehouse Tables & Views** — via SQL `sys.objects` metadata

Each sub-item gets its own row in the inventory with its own modification date and retention calculation.

### Step 6 — Show a Summary
At the end, the notebook displays a summary showing:
- How many items were found in total (including sub-items)
- How many items were found per workspace
- How many items had dates from each source
- How many items had **no modification date at all** (meaning no one has changed them in the last 28+ days)
- Sub-item counts by type (LakehouseTable, LakehouseFile, WarehouseTable, WarehouseView)

## What Does It NOT Do?

- It does **not delete** anything
- It does **not move or archive** anything
- It does **not change** any settings
- It is completely **read-only** — it just looks and reports

## How Long Does It Take?

Typically **2–3 minutes**, depending on how many workspaces and items your organization has.

## Output

A Delta table called **`workspace_inventory`** saved in the RetentionConfig lakehouse. This table is used by Notebook 03 to build the retention readiness report. It contains the retention period for each item (set in Cell 4 of this notebook), which serves as the **single source of truth** — no other notebook or config file controls retention days.
