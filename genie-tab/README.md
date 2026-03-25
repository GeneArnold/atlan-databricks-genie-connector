# Genie Tab — Embedded Genie Chat Interface for Atlan

Flask web app that provides a Databricks Genie chat interface as a custom tab inside Atlan's asset profile pages. Deployed to Render.

## How It Works

1. User clicks the Genie tab on a Genie Space asset in Atlan
2. Atlan loads this app in an iframe, authenticates via `@atlanhq/atlan-auth` SDK (OAuth)
3. App reads the asset's custom metadata to find the Databricks Genie space ID
4. User chats with Genie through the embedded interface

## Prerequisites

- Genie Space assets must exist in Atlan (created by `genie-assets/` scripts)
- Databricks workspace with Genie API access
- Atlan instance with OAuth redirect URI registered for the deployed URL

## Local Development

```bash
cp .env.example .env
# Edit .env with your credentials

pip install -r requirements.txt
python app.py
```

Open `http://localhost:10000`. OAuth will redirect to Atlan for login.

## Render Deployment

1. Create a new Web Service in Render
2. Connect to the `atlan-databricks-genie-connector` GitHub repo
3. Set **Root Directory** to `genie-tab`
4. Set environment variables (see below)
5. Deploy

Render will auto-detect `Procfile` and `.python-version`.

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DATABRICKS_WORKSPACE_URL` | Databricks workspace URL | `https://dbc-xxx.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Databricks personal access token | `dapi...` |
| `ATLAN_INSTANCE_URL` | Atlan instance URL | `https://databricks.atlan.com` |
| `PORT` | Server port (Render sets automatically) | `10000` |

## Atlan Configuration

Register this app in Atlan via the `external-iframe-tabs` LaunchDarkly flag:

```json
{
  "genie-chat-tab": {
    "display_name": "Genie Chat",
    "iframe_url": "https://YOUR-RENDER-URL.onrender.com",
    "allowed_origins": ["https://YOUR-RENDER-URL.onrender.com"],
    "icon": "Analytics",
    "description": "Chat with Databricks Genie",
    "render_at": [
      {
        "slot": "asset-profile-tab",
        "when": {
          "assetTypes": ["CustomEntity"]
        }
      }
    ]
  }
}
```

Also register the Render URL as an OAuth redirect URI in Atlan's Keycloak.
