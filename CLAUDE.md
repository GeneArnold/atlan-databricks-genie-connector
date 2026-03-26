# CLAUDE.md

## Project Overview

This is a monorepo for the Databricks Genie + Atlan integration. It has two components:

- **`genie-assets/`** — Python CLI scripts that extract Genie Spaces from Databricks and create them as assets in Atlan with custom metadata. Uses PyAtlan SDK + API keys. Run manually or on a schedule.
- **`genie-tab/`** — Flask web app deployed to Render that provides an embedded Genie chat tab inside Atlan. Uses `@atlanhq/atlan-auth` SDK for OAuth, Atlan REST API with Bearer tokens, and Databricks Genie Conversational API.

**This is demo tooling, not a product.** The code will be handed to the Atlan product team who will rewrite it as an official connector. Prioritize a working demo over production quality. Don't break existing data.

## Architecture

```
Databricks                    Atlan                         Render
┌──────────────┐    extract   ┌──────────────┐    iframe    ┌──────────────┐
│ Genie Spaces │ ──────────→  │ Genie Assets │ ──────────→  │  Genie Tab   │
│ (API)        │  genie-assets│ (CustomEntity│  genie-tab   │  (Flask app) │
└──────────────┘              └──────────────┘              └──────┬───────┘
       ↑                                                          │
       └──────────────── Genie Chat API ──────────────────────────┘
```

## Key Technical Details

### PyAtlan v4.2.5 Quirks (genie-assets)
- `AtlanConnectorType.CREATE_CUSTOM()` does NOT exist in v4 — removed from all scripts
- `CustomEntity.creator()` fails for custom connector types (`databricks-genie` not in enum) — build entities manually by setting `name`, `qualified_name`, `connection_qualified_name`, `connector_name`
- Custom metadata: use `CustomMetadataDict("Set Name")` (single string), set fields via dict syntax, attach with `entity.set_custom_metadata(cm)` — NOT `CustomMetadataDict({"Set Name": {...}})`
- Use `sub_type` not `asset_user_defined_type` for the CustomEntity user-defined type field
- `client.typedef.get_by_name()` requires the internal hashed name, not display name — use REST API `/api/meta/types/typedefs` to find internal name by display name first

### Atlan Custom Metadata (businessAttributes)
- Atlan uses **internal hashed keys** for both custom metadata set names AND field names in the REST API
- On databricks.atlan.com: "Genie Spaces Details" → `Qth5U0CzSYzHMg7stojl6o`
- 12 fields: spaceId, warehouseId, tableCount, tables, hasInstructions, category, createdBy, totalQueries, uniqueUsers, avgResponseTime, workspaceUrl, sampleQuestions
- workspaceUrl stores the Databricks workspace origin (e.g., `https://dbc-xxx.cloud.databricks.com`) so genie-tab can route API calls to the correct workspace
- sampleQuestions stores a JSON-encoded array of sample questions from the Genie Space API
- The genie-tab code searches all businessAttributes values for hex strings matching Databricks space ID format (20+ char hex)

### OAuth (genie-tab)
- Uses `@atlanhq/atlan-auth` SDK loaded from CDN: `https://unpkg.com/@atlanhq/atlan-auth@latest/dist/atlan-auth.umd.min.js`
- Raw postMessage listener added BEFORE SDK init to capture asset GUID (SDK strips `page` context in embedded mode)
- OAuth token forwarded as `Authorization: Bearer` header on all API calls to Flask backend
- PyAtlan does NOT work with OAuth tokens — must use Atlan REST API directly
- Standalone mode (direct browser access) triggers OAuth redirect; embedded mode (iframe in Atlan) gets token via postMessage

### Databricks Genie API
- Currently uses a Personal Access Token (PAT) — needs to move to OAuth M2M (service principal)
- Endpoints: `/api/2.0/genie/spaces/{id}/start-conversation`, `.../conversations/{id}/messages`
- Polls for response completion

## Deployment

### genie-tab (Render)
- **GitHub repo:** `https://github.com/GeneArnold/atlan-databricks-genie-connector`
- **Render service:** `databricks-atlan-com-genie-tab` at `https://databricks-atlan-com-genie-tab.onrender.com`
- **Render Root Directory:** `genie-tab`
- **Start command:** `gunicorn app:app`
- **Python version:** 3.11.10 (specified in `.python-version` — newer versions break pydantic)
- **Env vars on Render:** `DATABRICKS_WORKSPACES` (JSON array of `{url, token}`), `ATLAN_INSTANCE_URL`. Legacy single-workspace vars `DATABRICKS_WORKSPACE_URL` + `DATABRICKS_TOKEN` still supported as fallback.

### genie-assets (local/scheduled)
- Run locally with Python 3.11.10 + PyAtlan 4.2.5
- Uses PyAtlan SDK with API key authentication
- Env vars in `.env`: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `ATLAN_BASE_URL`, `ATLAN_API_KEY`

## Workspaces & Instances

| System | URL | Notes |
|--------|-----|-------|
| **Atlan (target)** | `https://databricks.atlan.com` | Demo instance. Connection QN: `default/databricks-genie/1774136962` |
| **Atlan (old)** | `https://partner-sandbox.atlan.com` | Previous test instance |
| **Databricks (Genie Spaces)** | `https://dbc-8d941db8-48cd.cloud.databricks.com` | Where the 8 Genie Spaces live |
| **Databricks (demo data)** | `https://dbc-ae31ce1d-325d.cloud.databricks.com` | atlan-ai-env, demo tables being crawled |

The `.env` uses the old Databricks workspace for extraction (that's where the Genie Spaces are) and `databricks.atlan.com` as the Atlan target.

## Commands

```bash
# genie-tab local dev
cd genie-tab
pip install -r requirements.txt
python app.py

# genie-assets — run scripts in order for first-time setup
cd genie-assets
pip install -r requirements.txt
python 01_create_genie_connection.py   # idempotent — finds or creates connection
python 02_setup_genie_metadata.py      # idempotent — checks existing metadata
python 03_extract_and_sync_genie_spaces.py  # extracts from Databricks, syncs to Atlan
python 04_create_lineage.py            # creates lineage (needs source tables in Atlan)
python 04_create_lineage.py "Wide World"   # target a single space
```

## Reference Projects (DO NOT MODIFY)

- `/Users/gene.arnold/WorkSpace/atlan-ext-details/` — Working example of Atlan Auth SDK + REST API pattern
- `/Users/gene.arnold/WorkSpace/genie_space_connector/` — Original genie-assets source (being replaced by this repo)
- `/Users/gene.arnold/WorkSpace/atlan-iframe/atlan-sdr-databricks/` — Original genie-tab source (being replaced by this repo)

## What's Next

See `TODO.md` at the root for the full roadmap.
