# Notebook 03 — Retention Config & Readiness Report

## What Does This Notebook Do?

This is the **final notebook** in the reporting pipeline. It takes everything collected by Notebooks 01 and 02, applies a retention policy (how many days old is "too old"), and produces a **readiness report** showing which items exceed their retention period and which ones are still within bounds.

Think of it as the final answer to the question: **"What do we have that's old enough to be flagged for review?"**

## Why Is This Important?

Having an inventory (Notebook 01) and activity history (Notebook 02) is useful, but the real value comes from comparing that data against a **retention policy** — a rule that says "if an item hasn't been touched in X days, flag it." This notebook does that comparison and gives you a clear, color-coded report.

## How Does It Work? (Step by Step)

### Step 1 — Read the Retention Period from Notebook 01
This notebook does **not** set its own retention period. Instead, it reads the `retention_period_days` column from the `workspace_inventory` table (produced by Notebook 01). The retention days are configured in **Notebook 01 Cell 4**, which is the **single source of truth** for the entire project. This means you only need to change one place when adjusting retention — and Notebook 03 will automatically pick up the new value on its next run.

### Step 2 — Discover All Item Types
The notebook reads the `workspace_inventory` table (created by Notebook 01) and finds all the distinct item types — things like Report, Notebook, Pipeline, SemanticModel, Lakehouse, etc. It counts how many of each type exist.

### Step 3 — Build the Retention Config Table
For each item type discovered in Step 2, the notebook creates a row in a new table called `retention_config`. The retention days for each type are inherited from the inventory table (which got them from Notebook 01 Cell 4). Each row contains:

| Column | What It Means |
|--------|--------------|
| **item_type** | The type of item (Report, Notebook, etc.) |
| **retention_days** | How many days before it's considered overdue |
| **action** | What to do if overdue — set to "flag_only" (report it, never delete) |
| **description** | A human-readable description of the rule |
| **updated_date** | When the config was last generated |

**Important:** This table is **auto-generated** from the inventory data. It does not read from any external config file. The retention period for each item type comes from Notebook 01 Cell 4, carried through the `workspace_inventory` table.

### Step 4 — Build the Readiness Report
This is the main output. The notebook joins three data sources together:

1. **Workspace Inventory** (from Notebook 01) — what items exist
2. **Activity History** (from Notebook 02) — when each item was last modified
3. **Retention Config** (from Step 3 above) — how many days is "too old"

For each item, it picks the **best available date** using this priority order:
1. Activity events matched by item ID (most reliable)
2. Activity events matched by item name + workspace (fallback)
3. Fabric/PBI Scanner API date (from the inventory)
4. Creation date (last resort)

Then it calculates how many days have passed since that date and compares it to the retention period.

### Step 5 — Color-Coded Results
Every item gets a status:

| Status | What It Means |
|--------|--------------|
| **🔴 EXCEEDS RETENTION** | This item has not been modified within the retention window. It's "overdue." |
| **🟢 Within retention** | This item was recently modified. It's fine. |
| **⚪ No date — cannot assess** | No modification date could be found from any source. This item can't be evaluated. |

The report also shows:
- **days_since_modified** — how many days since the last change
- **days_overdue** — how many days past the retention limit
- **date_source** — where the date came from, so you can see the transparency of how each item was evaluated

### Step 6 — Summaries by Type and Workspace
The notebook shows summary breakdowns:
- **By item type** — e.g., "12 Reports are overdue, 3 Notebooks are within retention"
- **By workspace** — e.g., "Data_Retention_Reporting_demo has 8 overdue items, Demo_Insurance has 2"

### Step 7 — Save the Results
The final readiness report is saved as a Delta table called `retention_readiness`. This table can be used to:
- Build **Power BI dashboards** showing retention status across the organization
- Set up **automated alerts** when items cross the retention threshold
- Feed into a **governance workflow** for review and approval

## What Does It NOT Do?

- It does **not delete** anything
- It does **not move or archive** anything
- It does **not change** any items or settings
- The action column always says **"flag_only"** — it only reports, never acts
- It is completely **read-only**

## How Long Does It Take?

Typically **under 30 seconds**, since it's just reading and joining existing tables.

## Output

Two Delta tables saved in the RetentionConfig lakehouse:
- **`retention_config`** — One row per item type with the retention period and action. Auto-generated each run.
- **`retention_readiness`** — The final report. One row per item showing its status, dates, and whether it exceeds retention. This is the table you'd build dashboards on.
