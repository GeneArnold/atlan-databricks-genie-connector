# Genie Assets — Databricks Genie Space Connector for Atlan

Python CLI scripts that extract Databricks Genie Spaces and create corresponding `CustomEntity` assets in Atlan with custom metadata. These assets are the foundation for the embedded Genie chat tab (`genie-tab/`).

## What It Creates in Atlan

- A **databricks-genie** custom connector with Databricks icon
- A **Databricks Genie Spaces** connection
- **CustomEntity** assets for each Genie Space with custom metadata:
  - `spaceId` — Databricks space identifier (used by genie-tab to launch chat)
  - `warehouseId` — SQL warehouse ID
  - `tableCount` — Number of tables in the space
  - `tables` — Comma-separated list of table qualified names
  - `hasInstructions` — Whether the space has AI instructions
  - `category` — Space category (e.g. "AI/BI")
  - `createdBy` — Space creator email
  - `totalQueries`, `uniqueUsers`, `avgResponseTime` — Usage metrics (populated if available)
- **README** on each asset with tables, sample questions, and Databricks link
- **Lineage** from source Databricks tables to Genie Space entities (via Process assets)

## Scripts

All scripts are **idempotent** — safe to re-run. They detect existing data before creating.

Run in order for first-time setup:

| # | Script | Purpose | Frequency |
|---|--------|---------|-----------|
| 01 | `01_create_genie_connection.py` | Find or create connection, set Databricks icon | Once (idempotent) |
| 02 | `02_setup_genie_metadata.py` | Verify or create custom metadata fields | Once (idempotent) |
| 03 | `03_extract_and_sync_genie_spaces.py` | Extract from Databricks + create/update assets | Each sync |
| 04 | `04_create_lineage.py` | Create lineage between Genie Spaces and source tables | After tables crawled |

For extraction only (no Atlan sync):

| Script | Purpose |
|--------|---------|
| `00_extract_genie_spaces.py` | Extract Genie Space details to JSON files |

Script 04 supports targeting a single space: `python 04_create_lineage.py "Wide World"`

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
| `DATABRICKS_HOST` | Databricks workspace URL where Genie Spaces live |
| `DATABRICKS_TOKEN` | Databricks personal access token |
| `ATLAN_BASE_URL` | Atlan instance URL (e.g. `https://databricks.atlan.com`) |
| `ATLAN_API_KEY` | Atlan API key (not OAuth — these are server-side scripts) |

## Dependencies

- `pyatlan>=4.0.0` — Atlan SDK for creating assets and custom metadata
- `requests>=2.31.0` — HTTP client for Databricks API and Atlan REST API
- `python-dotenv>=1.0.0` — Environment variable loading
- `rich>=13.0.0` — Pretty console output (script 00 only)

## Notes

- These scripts use **PyAtlan SDK with API keys**, not OAuth. API key auth is appropriate for server-side automation scripts.
- The custom metadata field names appear as internal hashed keys in Atlan's REST API (e.g. `spaceId` becomes `MXYw8KMPKDIqjnWBQkbWJe`). PyAtlan abstracts this; the genie-tab REST API code handles it by searching for hex-pattern values.
- PyAtlan v4.2.5 does not support `AtlanConnectorType.CREATE_CUSTOM()`. Scripts use manual entity construction for the custom `databricks-genie` connector type.
- Script 03 auto-discovers the Genie connection QN via search (falls back to `connection_info.txt`).
- Script 04 auto-discovers all Databricks connections and searches across them for source tables.
