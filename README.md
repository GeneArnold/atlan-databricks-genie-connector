# Atlan + Databricks Genie Connector

Integrates Databricks Genie Spaces into Atlan as browsable, searchable assets with an embedded chat interface.

## Components

### [`genie-assets/`](genie-assets/) — Asset Creator

CLI scripts that extract Genie Space metadata from Databricks and create corresponding assets in Atlan with custom metadata (space ID, tables, warehouse ID, etc.).

**Run these first** to populate Atlan with Genie Space assets.

### [`genie-tab/`](genie-tab/) — Embedded Chat Tab

Flask web app deployed to Render that appears as a custom tab on Genie Space assets in Atlan. Uses OAuth via `@atlanhq/atlan-auth` SDK for authentication and the Databricks Genie Conversational API for chat.

**Deploy this to Render** after assets are created.

## Workflow

```
1. Configure credentials (.env files)
2. Run genie-assets scripts to create Genie Space assets in Atlan
3. Deploy genie-tab to Render
4. Register the Render URL in Atlan (LaunchDarkly flag + OAuth redirect URI)
5. Users see Genie Chat tab on Genie Space assets in Atlan
```

## Architecture

```
Databricks                    Atlan                         Render
┌──────────────┐    extract   ┌──────────────┐    iframe    ┌──────────────┐
│ Genie Spaces │ ──────────→  │ Genie Assets │ ──────────→  │  Genie Tab   │
│ (API)        │              │ (CustomEntity│              │  (Flask app) │
└──────────────┘              └──────────────┘              └──────┬───────┘
       ↑                                                          │
       └──────────────── Genie Chat API ──────────────────────────┘
```
