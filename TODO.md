# TODO — Atlan Databricks Genie Connector

## Genie Tab (genie-tab/)

### High Priority

- [x] **Policy-based access gating** — *(Completed 2026-03-25)* Implemented in `app.py`. Searches for the "Genie Space Access" AuthPolicy by name via indexsearch, resolves the parent persona's Keycloak role, then checks if the OAuth user has that role. Admins (`$admin`) bypass automatically. Uses the user's OAuth token for all API calls (no server-side API key). Includes caching (5-min TTL) and multiple fallback paths (users API → Keycloak roles → JWT decode). Frontend shows a lock/access denied screen on 403. Env var `GENIE_ACCESS_POLICY_NAME` controls the policy name (default: "Genie Space Access").

- [ ] **Databricks OAuth M2M** — Replace the Databricks Personal Access Token (PAT) with OAuth service principal credentials. Use client_credentials grant against `https://{workspace}/oidc/v1/token`. Requires creating a service principal in Databricks Account Console, generating an OAuth secret, and granting it access to Genie spaces + SQL warehouse + Unity Catalog tables. The `databricks-sdk` Python package handles automatic token refresh. See research notes in the project memory.

- [ ] **Register OAuth redirect URI on databricks.atlan.com** — The Render URL (`https://databricks-atlan-com-genie-tab.onrender.com`) needs to be registered as a valid redirect URI in Atlan's Keycloak for standalone mode to work. Embedded mode (iframe) works without this.

- [ ] **Fix "Open in Databricks" hyperlink** — The "Open in Databricks" link in the Genie tab UI currently doesn't work. Need to determine the correct Databricks URL format to open a Genie space directly (e.g., `https://{workspace}/genie/rooms/{space_id}` or similar). This is the same URL pattern needed by genie-assets for the README link on Atlan assets.

### Medium Priority

- [ ] **UI refresh — Databricks branding** — Update colors from purple (#667eea) to Databricks red (#FF3621). Dark background (#1B3139), Databricks-themed bot avatar, "Powered by Databricks Genie" footer. Update user message bubbles, send button, focus states.

- [ ] **Remove debug logging** — The `businessAttributes` debug logging in `app.py` (lines that dump full metadata contents) should be removed or reduced to WARNING level before production use.

- [ ] **Error handling for token expiry** — Add frontend handling for 401 responses mid-session (token expired). Should prompt re-auth or auto-refresh via SDK.

### Low Priority

- [ ] **Custom domain** — Consider setting up a custom domain on Render instead of `*.onrender.com` to avoid Chrome's phishing warning (domain contains "atlan").

- [ ] **Production WSGI** — Currently using gunicorn with default settings. May want to tune worker count, timeouts for the Genie API polling.

## Genie Assets (genie-assets/)

### High Priority

- [x] **Validate scripts against databricks.atlan.com** — *(Completed 2026-03-26)* All 4 scripts updated and tested. Scripts are now idempotent — they detect existing connections, custom metadata, and entities before creating. Adapted for PyAtlan v4.2.5 API changes. Connection icon set to Databricks SVG. Extracted 8 Genie Spaces, created 2 new + updated 5 existing entities with custom metadata and READMEs.

- [x] **Review and clean up scripts** — *(Completed 2026-03-26)* Removed hardcoded workspace URLs, sample/demo data fallbacks, and Wide World Importers-specific code. All scripts use .env vars. Script 03 auto-discovers connection QN. Script 04 generalized for any Genie Space.

- [x] **Update asset icon to Databricks icon** — *(Completed 2026-03-26)* Script 01 now sets `connection.asset_icon = "https://assets.atlan.com/assets/databricks.svg"`. Icon applied to the existing connection on databricks.atlan.com.

- [x] **Lineage script generalization** — *(Completed 2026-03-26)* Replaced `04_create_lineage_wide_world.py` with `04_create_lineage.py`. Works for any/all Genie Spaces, auto-discovers connections, looks up tables dynamically. Supports targeting a single space via CLI arg.

- [x] **Run lineage after Databricks connector crawl** — *(Completed 2026-03-26)* Lineage created for 5 of 7 spaces (20 table connections total). Finance DW (5 tables), Wide World Importers (4 tables x2), FinServ Compliance (4 tables), Revenue Analytics (3 tables). 2 spaces skipped (kdrp_mv and sales_metrics tables not crawled).

- [ ] **Fix "Open in Databricks" link on Genie assets** — The README on each Genie asset currently uses `{DATABRICKS_HOST}/genie/rooms/{space_id}` as the link format. Need to verify this is the correct URL pattern. Once confirmed, this is done.

### Medium Priority

- [ ] **Polish README template** — The README template now includes all the right data (tables with column counts, sample questions, SQL examples, instructions, links). Needs visual cleanup to look cleaner and more professional — better formatting, spacing, section styling.

- [ ] **Populate totalQueries / uniqueUsers / avgResponseTime** — These 3 custom metadata fields are empty because the Genie Spaces API doesn't expose usage metrics. Options: (1) pull from Databricks SQL query history API (`/api/2.0/sql/history/queries`) filtering by warehouse ID, (2) query Unity Catalog audit system tables (`system.access.audit`), or (3) populate with representative demo values.

- [x] **Handle deleted Genie Spaces** — *(Completed 2026-03-26)* Script 03 now compares Atlan entities against Databricks spaces after sync and deletes orphaned entities.

- [ ] **Automate sync** — Currently manual CLI execution. Consider scheduled runs (cron, Temporal, or Databricks Jobs) to keep Atlan assets in sync with Genie Spaces.

### Low Priority

- [ ] **Move to Databricks OAuth** — Currently uses Databricks PAT for extraction. Could move to service principal auth for consistency with genie-tab.

## Infrastructure

- [ ] **Clean up old projects** — Once everything is validated in the new monorepo, archive or delete:
  - `/Users/gene.arnold/WorkSpace/genie_space_connector/`
  - `/Users/gene.arnold/WorkSpace/atlan-iframe/atlan-sdr-databricks/`
  - `/Users/gene.arnold/WorkSpace/atlan-iframe/atlan-genie-chat/`
  - `/Users/gene.arnold/WorkSpace/atlan-oauth-test/`
  - Old Render service `atlan-sdr-metadata`

- [ ] **GitHub repo cleanup** — Archive old repos:
  - `GeneArnold/genie_space_connector`
  - `GeneArnold/atlan-sdr-metadata`
