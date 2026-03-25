# Genie Tab — Embedded Genie Chat Interface for Atlan

Flask web app deployed to Render that provides a Databricks Genie chat interface as a custom tab inside Atlan's asset profile pages.

## How It Works

1. User navigates to a Genie Space asset in Atlan and clicks the "Launch Genie" tab
2. Atlan loads this app in a sandboxed iframe
3. The `@atlanhq/atlan-auth` SDK handles OAuth authentication automatically
4. A raw postMessage listener (registered before SDK init) captures the asset GUID from the iframe context
5. The Flask backend fetches the asset via Atlan REST API using the OAuth Bearer token
6. It finds the Databricks `spaceId` in the asset's custom metadata (`entity.businessAttributes`)
7. The chat interface connects to the Databricks Genie Conversational API
8. User chats with Genie, getting SQL-powered answers about their data

## Key Technical Details

### OAuth Authentication
- Uses `@atlanhq/atlan-auth` SDK loaded from CDN
- **Embedded mode** (in Atlan iframe): Token delivered via postMessage — no redirect needed
- **Standalone mode** (direct browser access): OAuth redirect to Atlan login — requires redirect URI registration in Keycloak
- PyAtlan SDK does NOT work with OAuth tokens — all Atlan API calls use direct REST with Bearer token

### Asset GUID Capture
The Atlan Auth SDK **strips page context** in embedded mode. A raw `window.addEventListener('message', ...)` registered BEFORE `atlan.init()` captures `payload.page.params.id` from the `ATLAN_AUTH_CONTEXT` message.

### Custom Metadata Parsing
Atlan uses internal **hashed keys** for `businessAttributes` — not display names. The code:
1. Fetches `GET /api/meta/entity/guid/{guid}` with Bearer token
2. Iterates all values in `entity.businessAttributes`
3. Matches string values against a hex ID pattern (`/^[0-9a-f]{20,}$/`) to find the Databricks space ID
4. This avoids hardcoding hashed key names which may differ between Atlan instances

### Databricks Genie API
- `POST /api/2.0/genie/spaces/{spaceId}/start-conversation` — Start a new conversation
- `POST .../conversations/{id}/messages` — Send a follow-up message
- `GET .../conversations/{id}/messages/{id}` — Poll for response (status: COMPLETED/FAILED)
- Currently authenticated with a Personal Access Token (PAT) — moving to OAuth M2M

## Local Development

```bash
cp .env.example .env
# Edit .env with your credentials

pip install -r requirements.txt
python app.py
```

Opens at `http://localhost:10000`. OAuth redirect won't work on localhost unless registered — use `?test=true` or access via Atlan iframe.

## Render Deployment

### Service Configuration
| Setting | Value |
|---------|-------|
| **Repository** | `GeneArnold/atlan-databricks-genie-connector` |
| **Branch** | `main` |
| **Root Directory** | `genie-tab` |
| **Runtime** | Python 3 |
| **Build Command** | `pip install -r requirements.txt` |
| **Start Command** | `gunicorn app:app` |

### Environment Variables
| Variable | Value |
|----------|-------|
| `DATABRICKS_WORKSPACE_URL` | `https://dbc-8d941db8-48cd.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Databricks PAT |
| `ATLAN_INSTANCE_URL` | `https://databricks.atlan.com` |

Note: `.python-version` (3.11.10) is auto-detected by Render. Do NOT use Python 3.14+ — it breaks pydantic compilation.

### Atlan Custom Tab Configuration

LaunchDarkly flag `external-iframe-tabs` on `databricks.atlan.com`:

```json
{
  "databricks-genie-launcher": {
    "allowed_origins": ["https://databricks-atlan-com-genie-tab.onrender.com"],
    "company": "Atlan",
    "description": "Chat with Databricks Genie",
    "display_name": "Launch Genie",
    "icon": "Sparkles",
    "iframe_url": "https://databricks-atlan-com-genie-tab.onrender.com",
    "render_at": [
      {
        "slot": "asset-profile-tab",
        "when": { "assetTypes": ["CustomEntity"] }
      }
    ]
  }
}
```

## File Structure

```
genie-tab/
├── app.py              # Flask backend — Atlan REST API proxy + Genie chat proxy
├── templates/
│   └── chat.html       # Frontend — Atlan Auth SDK + chat UI
├── requirements.txt    # Python deps (Flask, flask-cors, requests, httpx, gunicorn)
├── .python-version     # 3.11.10 (critical for Render)
├── Procfile            # gunicorn app:app
└── .env.example        # Environment variable template
```

## TODO

- [ ] **Persona-based access gating** — Check user's Atlan persona before allowing Genie access
- [ ] **Databricks OAuth M2M** — Replace PAT with service principal (client_credentials flow against `/oidc/v1/token`)
- [ ] **UI refresh** — Databricks branding (red #FF3621, dark backgrounds, Genie-themed avatar)
- [ ] **Remove debug logging** — Reduce businessAttributes dump logging before production
- [ ] **Token expiry handling** — Handle 401 mid-session with re-auth prompt
- [ ] **Custom domain** — Avoid Chrome phishing warning on `*atlan*onrender.com`
