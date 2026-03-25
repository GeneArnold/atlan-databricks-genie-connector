# Genie Assets — Databricks Genie Space Connector for Atlan

Python CLI scripts that extract Databricks Genie Spaces and create corresponding `CustomEntity` assets in Atlan with custom metadata. These assets are the foundation for the embedded Genie chat tab (`genie-tab/`).

## What It Creates in Atlan

- A **DATABRICKS_GENIE** custom connector type
- A **Databricks Genie Spaces** connection
- **CustomEntity** assets for each Genie Space with custom metadata:
  - `spaceId` — Databricks space identifier (used by genie-tab to launch chat)
  - `warehouseId` — SQL warehouse ID
  - `tableCount` — Number of tables in the space
  - `tables` — Comma-separated list of table qualified names
  - `hasInstructions` — Whether the space has AI instructions
  - `category` — Space category (e.g. "AI/BI")
  - `createdBy` — Space creator email
  - And more: totalQueries, uniqueUsers, lastAccessed, avgResponseTime

## What It Extracts from Databricks

- Genie Space details (ID, title, warehouse, descriptions)
- Table structures with full column configurations
- Column metadata (names, types, format assistance flags)
- Business context (instructions, sample questions)
- Technical details (SQL components, joins, filters)

Uses the `?include_serialized_space=true` API parameter for complete metadata extraction.

## Scripts

Run in order for first-time setup:

| # | Script | Purpose | Frequency |
|---|--------|---------|-----------|
| 01 | `01_create_genie_connection.py` | Create DATABRICKS_GENIE connector and connection in Atlan | Once per Atlan instance |
| 02 | `02_setup_genie_metadata.py` | Create custom metadata attributes on the connection | Once per Atlan instance |
| 03 | `03_extract_and_sync_genie_spaces.py` | Extract from Databricks + create/update assets in Atlan | Each sync |
| 04 | `04_create_lineage_wide_world.py` | Create lineage between Genie Space and source tables | Optional |

For extraction only (no Atlan sync):

| Script | Purpose |
|--------|---------|
| `00_extract_genie_spaces.py` | Extract Genie Space details to JSON files |

## Setup

```bash
cp .env.example .env
# Edit .env with your credentials

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `DATABRICKS_HOST` | Databricks workspace URL (e.g. `https://dbc-xxx.cloud.databricks.com`) |
| `DATABRICKS_TOKEN` | Databricks personal access token |
| `ATLAN_BASE_URL` | Atlan instance URL (e.g. `https://databricks.atlan.com`) |
| `ATLAN_API_KEY` | Atlan API key (not OAuth — these are server-side scripts) |

## Dependencies

- `pyatlan>=2.0.0` — Atlan SDK for creating assets and custom metadata
- `requests>=2.31.0` — HTTP client for Databricks API
- `python-dotenv>=1.0.0` — Environment variable loading
- `rich>=13.0.0` — Pretty console output

## Notes

- These scripts use **PyAtlan SDK with API keys**, not OAuth. API key auth is appropriate for server-side automation scripts.
- The custom metadata field names appear as internal hashed keys in Atlan's REST API (e.g. `spaceId` becomes `i7n9E99lc3MzJ5w8evW3gw`). PyAtlan abstracts this; the genie-tab REST API code handles it by searching for hex-pattern values.
- `04_create_lineage_wide_world.py` is currently hardcoded for the "Wide World Importers" demo space. Needs generalization for other spaces.

## TODO

- [ ] Validate scripts against `databricks.atlan.com` (tested on `partner-sandbox.atlan.com`)
- [ ] Generalize lineage script for any Genie Space
- [ ] Consider scheduled/automated sync (cron, Temporal, or Databricks Jobs)
- [ ] Move from Databricks PAT to service principal auth
