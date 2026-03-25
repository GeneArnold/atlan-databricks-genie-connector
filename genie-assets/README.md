# Genie Assets — Databricks Genie Space Connector for Atlan

CLI scripts that extract Databricks Genie Spaces and create corresponding assets in Atlan with custom metadata.

## What It Does

1. Extracts Genie Space details from Databricks (tables, columns, descriptions, instructions)
2. Creates a "DATABRICKS_GENIE" connection type in Atlan (one-time setup)
3. Creates custom metadata attributes for Genie Spaces in Atlan (one-time setup)
4. Syncs Genie Spaces as CustomEntity assets in Atlan with full metadata
5. Optionally creates lineage between Genie Spaces and source tables

## Prerequisites

- Python 3.11+
- Databricks workspace with Genie Spaces
- Atlan instance with API key

## Setup

```bash
cp .env.example .env
# Edit .env with your credentials

pip install -r requirements.txt
```

## Scripts

Run in order for first-time setup:

| Script | Purpose | When to Run |
|--------|---------|-------------|
| `01_create_genie_connection.py` | Create DATABRICKS_GENIE connection in Atlan | Once |
| `02_setup_genie_metadata.py` | Create custom metadata attributes | Once |
| `03_extract_and_sync_genie_spaces.py` | Extract from Databricks + sync to Atlan | Each sync |
| `04_create_lineage_wide_world.py` | Create lineage relationships | Optional |

For extraction only (without sync):

| Script | Purpose |
|--------|---------|
| `00_extract_genie_spaces.py` | Extract Genie Space details to JSON |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `DATABRICKS_HOST` | Databricks workspace URL |
| `DATABRICKS_TOKEN` | Databricks personal access token |
| `ATLAN_BASE_URL` | Atlan instance URL |
| `ATLAN_API_KEY` | Atlan API key |
