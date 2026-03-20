# Notebook 02 — Activity Log Collector

## What Does This Notebook Do?

This notebook **collects a history of user activity** from Microsoft Fabric and Power BI. Every time someone edits a report, updates a dataset, modifies a notebook, or makes any real change — that action is recorded by Microsoft as an "activity event." This notebook gathers all those events and stores them so you have a reliable record of **when each item was last changed**.

## Why Is This Important?

Microsoft only keeps activity records for **28 days**. After that, the data is gone forever. If you don't collect these events regularly, you lose the ability to know when items were last modified. This notebook solves that problem by:

1. **Saving the events** into a permanent table so they're never lost
2. **Running incrementally** — it knows which days it already collected and only fetches the new ones, so it never duplicates data

Think of it like a security camera that only keeps 28 days of footage. If you don't save the recordings, the old footage is taped over and gone. This notebook saves those recordings.

## How Does It Work? (Step by Step)

### Step 1 — Filter Out "View-Only" Activity
Not all activity counts as a modification. If someone just **opens** a report to look at it, that shouldn't reset the clock on how old that report is. This notebook maintains a list of **30+ read-only actions** (like viewing, exporting, downloading, sharing) that are excluded. Only **real changes** (editing, creating, updating, deleting) are kept.

### Step 2 — Build a Workspace Directory
The notebook creates a lookup of all workspace names and IDs by calling two Microsoft APIs:
- **Fabric REST API** — for shared/organizational workspaces
- **PBI Admin Groups API** — for personal workspaces ("My workspace")

This is needed because the activity events often include a workspace ID but not the workspace name, so the notebook resolves the names itself.

### Step 3 — Check What's Already Been Collected
Before fetching anything, the notebook checks its existing data to see which days it has already collected. It only fetches the **missing days**. For example, if you ran it yesterday, it will only fetch today's events instead of re-fetching the full 28 days.

### Step 4 — Fetch Activity Events from Microsoft
The notebook calls the Microsoft Activity Events API one day at a time, going back up to 28 days. For each event, it extracts:

| Field | What It Means |
|-------|--------------|
| **activity** | What the user did (e.g., "EditReport", "CreateNotebook") |
| **artifact_id** | The unique ID of the item that was changed |
| **artifact_name** | The name of the item |
| **artifact_type** | What kind of item it is (Report, Notebook, etc.) |
| **workspace_name** | Which workspace the item lives in |
| **event_timestamp** | Exactly when the change happened |

The notebook uses smart extraction logic — different item types store their IDs in different fields, so it tries multiple approaches to capture as many items as possible.

### Step 5 — Save the Raw Events
All the collected modification events are **appended** to a permanent Delta table called `activity_events_raw`. Each new run adds to the existing data, building up a longer and longer history over time.

### Step 6 — Backfill Missing Workspace Names
Sometimes older events are missing their workspace name. This step goes back and fills in those blanks using the workspace directory built in Step 2.

### Step 7 — Compute a Summary Table
From all the raw events (not just today's batch, but all history), the notebook computes a **one-row-per-item summary** called `activity_last_modified`. This table shows:
- The **first** time each item was modified
- The **last** time each item was modified
- How many **total modifications** each item has had

### Step 8 — Show a Report
At the end, the notebook displays:
- How many events were fetched
- How many unique items are being tracked
- A breakdown by item type and workspace
- Which activity types are most common
- The health of the raw data table (date range, total events, days covered)

## What Does It NOT Do?

- It does **not delete** anything
- It does **not move or archive** anything
- It does **not change** any settings or items
- It is completely **read-only** — it only collects and stores activity history

## How Long Does It Take?

- **First run** (fetching all 28 days): **3–5 minutes**
- **Subsequent runs** (incremental, 1 day): **under 1 minute**

## How Often Should It Run?

**Daily** is ideal. At minimum, it should run at least once every 28 days. If it goes longer than 28 days without running, you will have a gap in your activity history that can never be recovered.

## Output

Two Delta tables saved in the RetentionConfig lakehouse:
- **`activity_events_raw`** — Every modification event collected, one row per event. Grows over time.
- **`activity_last_modified`** — One row per item showing its most recent modification date. Recomputed each run.

The `activity_last_modified` table is used by Notebook 03 to determine how recently each item was changed.
