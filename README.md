# Fabric Data Retention Reporting

A Microsoft Fabric workaround solution that scans your entire tenant, catalogs every workspace and item, tracks modification dates and last modified dates, and flags items that exceed their retention period — giving you a clear, actionable view of what needs attention. The user running the notebooks must have the **Fabric Administrator** role (assigned in Microsoft Entra ID). This is a **tenant-level** role.

<p style="color:orange;">MICROSOFT DISCLAIMER - Information provided in this file is provided "as is" without Warranty Representation or Condition of any kind, either express or implied, including but not limited to conditions or other terms of merchantability and/or fitness for a particular purpose. The user assumes the entire risk as to the accuracy and use of the information produced by this script.</p>


## What It Does

Three PySpark notebooks work together to build a complete retention picture:

| Notebook | Purpose | Output |
|----------|---------|--------|
| **01 — Workspace Inventory** | Scans all workspaces, catalogs every item (reports, semantic models, notebooks, lakehouses, warehouses, pipelines, etc.), and collects modification dates from multiple APIs | creates `workspace_inventory` Delta table |
| **02 — Activity Log Collector** | Incrementally collects user activity events from the Activity Events API (28-day window), filters out read-only actions, and computes per-item last modified dates | creates `activity_events_raw` and `activity_last_modified` Delta tables |
| **03 — Retention Config & Readiness** | Joins inventory + activity + config data, scores each item's retention status, and exports a readiness report | creates `retention_config`, `retention_readiness` Delta tables + Excel export |

Each item is scored:
- 🔴 **EXCEEDS RETENTION** — item is past its retention period
- 🟢 **WITHIN RETENTION** — item is within its retention period
- ⚪ **NO DATE** — no modification date could be determined

All data is stored in a Delta Lakehouse and can be queried, visualized, or exported at any time.

# //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


## Sharing with Clients
Will the client see clean separated cells? Yes, for both options.

### Prerequisites (Both Options)

Before running the notebooks, the client's environment needs:

| Requirement | Details |
|-------------|---------|
| **Fabric Workspace** | A workspace where the notebooks will run |
| **Fabric Capacity** | Any Fabric capacity (F2 or higher) |
| **Lakehouse** | Create a Lakehouse in the workspace (e.g., `YourLakehouseName`) — this is where all Delta tables and exports are stored |
| **Fabric Administrator** | The user running the notebooks must have the **Fabric Administrator** role (assigned in Microsoft Entra ID, formerly "Power BI Service Administrator"). This is a **tenant-level** role — not the same as being a Workspace Admin. A Workspace Admin can only see their own workspace; a Fabric Administrator can see every workspace across the entire tenant. Ask your **Microsoft 365 Global Admin** or **Entra ID admin** to assign this role. **One Fabric Administrator is needed per tenant.** If the organization has multiple tenants, each tenant requires its own Fabric Administrator to run the scan. |
| **Notebook Configuration** | After import, attach each notebook to the Lakehouse. Review and adjust retention periods in Notebook 01 Cell 4 (`default_retention_days` and `RETENTION_DAYS_BY_TYPE`) |
| **Run Order** | Run notebooks in order: **01 → 02 → 03**. Notebook 01 builds the inventory, 02 collects activity data, and 03 joins everything to produce the readiness report Delta table |

---

### Option A — Standalone Import (No GitHub Required)

The simplest approach — the client receives exported notebook files and imports them into their own Fabric workspace. No GitHub account needed.

**What you (the provider) do:**

1. Open your Fabric workspace
2. Right-click each notebook → **Export**. This downloads a `.ipynb` file
3. Export all three notebooks:
   - `01_workspace_inventory.ipynb`
   - `02_activity_log_collector.ipynb`
   - `03_retention_config.ipynb`
4. Share the 3 `.ipynb` files with the client (via email, SharePoint or OneDrive for business shared folder, Teams, etc.)

**What the client does:**

1. Open their Fabric workspace in the browser
2. Click **Import → Notebook → Upload** and select the 3 `.ipynb` files
3. Open each notebook and **attach it to their Lakehouse** using the Lakehouse selector in the notebook toolbar
4. Open Notebook 01 Cell 4 and review the retention periods — adjust `default_retention_days` and `RETENTION_DAYS_BY_TYPE` to match their organization's policy
5. Run the notebooks in order: **01 → 02 → 03**
6. After Notebook 03 completes, the readiness report is available as a Delta table and an Excel file in the Lakehouse `Files/exports/` folder

**Pros:** Simple, no GitHub setup, client has full ownership and can modify freely
**Cons:** Client does not receive future updates — you must re-export and re-share when changes are made

---

### Option B — Temporary Git Sync (Automatic Import via GitHub)

The client connects their Fabric workspace to your GitHub repository main branch. The notebooks sync automatically. The client then disconnects Git Integration and has standalone copies.

**What you (the provider) do:**

1. Go to your GitHub repository → **Settings → Collaborators**
2. Invite the client's GitHub account with **Read** access (this prevents them from pushing changes to your repo)
3. Confirm the invitation

**What the client does:**

1. Accept the GitHub collaborator invitation
2. Open their Fabric workspace in the browser
3. Go to **Workspace Settings → Git Integration**
4. Connect to the GitHub repository:
   - **Repository:** `Fabric_Data_Retention`
   - **Branch:** `main`
   - **Git folder:** `/` (root)
5. Click **Connect and sync** — all three notebooks appear in the workspace automatically
6. Open each notebook and **attach it to their Lakehouse**
7. Open Notebook 01 Cell 4 and adjust retention periods if needed
8. Run the notebooks in order: **01 → 02 → 03**
9. Once verified, go back to **Workspace Settings → Git Integration → Disconnect** to make the notebooks standalone
10. The client now has independent copies they can freely modify

**Pros:** Faster setup, no manual file download/upload, notebooks arrive with all metadata intact
**Cons:** Requires a GitHub account for the client also, requires Git Integration setup (then disconnect)