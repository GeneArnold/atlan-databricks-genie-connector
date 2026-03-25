# CLAUDE.md

## Project Overview

This is a monorepo for the Databricks Genie + Atlan integration. It has two components:

- **`genie-assets/`** — Python CLI scripts that extract Genie Spaces from Databricks and create them as assets in Atlan with custom metadata. Uses PyAtlan SDK + API keys. Run manually or on a schedule.
- **`genie-tab/`** — Flask web app deployed to Render that provides an embedded Genie chat tab inside Atlan. Uses `@atlanhq/atlan-auth` SDK for OAuth, Atlan REST API with Bearer tokens, and Databricks Genie Conversational API.

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

### Atlan Custom Metadata (businessAttributes)
- Atlan uses **internal hashed keys** for both custom metadata set names AND field names in the REST API
- The display name "Genie Spaces Details" appears as something like `yVXFsjFSYh1C7R32iwrbf7` in `entity.businessAttributes`
- Field names like `spaceId` also appear as hashes like `i7n9E99lc3MzJ5w8evW3gw`
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
- **Env vars on Render:** `DATABRICKS_WORKSPACE_URL`, `DATABRICKS_TOKEN`, `ATLAN_INSTANCE_URL`

### genie-assets (local/scheduled)
- Run locally with Python 3.11+
- Uses PyAtlan SDK with API key authentication
- Env vars in `.env`: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `ATLAN_BASE_URL`, `ATLAN_API_KEY`

## Target Atlan Instance

`https://databricks.atlan.com` (previously was `partner-sandbox.atlan.com`)

## Commands

```bash
# genie-tab local dev
cd genie-tab
pip install -r requirements.txt
python app.py

# genie-assets
cd genie-assets
pip install -r requirements.txt
python 03_extract_and_sync_genie_spaces.py
```

## Reference Projects (DO NOT MODIFY)

- `/Users/gene.arnold/WorkSpace/atlan-ext-details/` — Working example of Atlan Auth SDK + REST API pattern
- `/Users/gene.arnold/WorkSpace/genie_space_connector/` — Original genie-assets source (being replaced by this repo)
- `/Users/gene.arnold/WorkSpace/atlan-iframe/atlan-sdr-databricks/` — Original genie-tab source (being replaced by this repo)

## What's Next

See `TODO.md` at the root for the full roadmap.
