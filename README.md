# Atlan + Databricks Genie Connector

Integrates Databricks Genie Spaces into Atlan as browsable, searchable assets with an embedded AI chat interface. Users can discover Genie Spaces in Atlan's catalog and chat with Genie directly from the asset profile page.

## How It Works

This project has two components that work together:

### 1. [`genie-assets/`](genie-assets/) — Asset Creator (CLI Scripts)

Python scripts that connect to your Databricks workspace, extract all Genie Space metadata (tables, columns, instructions, sample questions), and create corresponding `CustomEntity` assets in Atlan with custom metadata fields including the Databricks `spaceId`.

**Run these first** to populate Atlan with Genie Space assets.

### 2. [`genie-tab/`](genie-tab/) — Embedded Chat Tab (Web App)

A Flask web app deployed to Render that appears as a custom tab on Genie Space assets in Atlan. When a user clicks the tab, the app:
- Authenticates via Atlan's OAuth SDK (`@atlanhq/atlan-auth`)
- Reads the asset's custom metadata to find the Databricks space ID
- Provides a chat interface powered by the Databricks Genie Conversational API

**Deploy this to Render** after assets are created.

## Architecture

```
Databricks                       Atlan (databricks.atlan.com)        Render
┌─────────────────┐              ┌─────────────────┐                ┌─────────────────┐
│                 │   extract    │                 │     iframe     │                 │
│  Genie Spaces   │─────────────→│  Genie Assets   │───────────────→│   Genie Tab     │
│  (Workspaces)   │ genie-assets │  (CustomEntity) │   genie-tab    │   (Flask app)   │
│                 │              │                 │                │                 │
└────────┬────────┘              └─────────────────┘                └────────┬────────┘
         │                                                                   │
         │                    Genie Conversational API                       │
         └───────────────────────────────────────────────────────────────────┘
```

## Setup From Scratch

### Prerequisites
- Databricks workspace with Genie Spaces
- Atlan instance (currently `databricks.atlan.com`)
- Render.com account for hosting the tab
- Python 3.11+

### Step 1: Create Genie Space Assets in Atlan

```bash
cd genie-assets
cp .env.example .env
# Edit .env with your Databricks and Atlan credentials

pip install -r requirements.txt

python 01_create_genie_connection.py          # One-time: create connector type
python 02_setup_genie_metadata.py             # One-time: create metadata fields
python 03_extract_and_sync_genie_spaces.py    # Extract + sync Genie Spaces
python 04_create_lineage_wide_world.py        # Optional: create lineage
```

### Step 2: Deploy the Chat Tab to Render

1. Create a new **Web Service** on Render
2. Connect to this GitHub repo: `GeneArnold/atlan-databricks-genie-connector`
3. Set **Root Directory** to `genie-tab`
4. Set **Start Command** to `gunicorn app:app`
5. Add environment variables:
   - `DATABRICKS_WORKSPACE_URL` = your Databricks workspace URL
   - `DATABRICKS_TOKEN` = your Databricks token
   - `ATLAN_INSTANCE_URL` = `https://databricks.atlan.com`
6. Deploy

### Step 3: Register in Atlan

Add the external tab configuration via LaunchDarkly `external-iframe-tabs` flag:

```json
{
  "databricks-genie-launcher": {
    "allowed_origins": ["https://YOUR-RENDER-URL.onrender.com"],
    "company": "Atlan",
    "description": "Chat with Databricks Genie",
    "display_name": "Launch Genie",
    "icon": "Sparkles",
    "iframe_url": "https://YOUR-RENDER-URL.onrender.com",
    "render_at": [
      {
        "slot": "asset-profile-tab",
        "when": { "assetTypes": ["CustomEntity"] }
      }
    ]
  }
}
```

Register the Render URL as an OAuth redirect URI in Atlan's Keycloak (needed for standalone mode).

## Current Deployment

| Component | Location |
|-----------|----------|
| GitHub repo | `github.com/GeneArnold/atlan-databricks-genie-connector` |
| Render service | `https://databricks-atlan-com-genie-tab.onrender.com` |
| Target Atlan | `https://databricks.atlan.com` |
| Databricks workspace | `https://dbc-8d941db8-48cd.cloud.databricks.com` |

## Roadmap

See [TODO.md](TODO.md) for the full list. Key items:
- Persona-based access gating (restrict Genie tab by Atlan persona)
- Databricks OAuth M2M (replace PAT with service principal)
- UI refresh (Databricks branding)
