# TODO — Atlan Databricks Genie Connector

## Genie Tab (genie-tab/)

### High Priority

- [ ] **Persona-based access gating** — After OAuth, fetch user's personas from Atlan API. If user doesn't have access to the required persona, block the Genie chat. Need to discover the correct Atlan API endpoint for personas (tried `/api/service/personas`, `/api/meta/user/current`, `/api/service/accesscontrol` — need to test which returns useful data).

- [ ] **Databricks OAuth M2M** — Replace the Databricks Personal Access Token (PAT) with OAuth service principal credentials. Use client_credentials grant against `https://{workspace}/oidc/v1/token`. Requires creating a service principal in Databricks Account Console, generating an OAuth secret, and granting it access to Genie spaces + SQL warehouse + Unity Catalog tables. The `databricks-sdk` Python package handles automatic token refresh. See research notes in the project memory.

- [ ] **Register OAuth redirect URI on databricks.atlan.com** — The Render URL (`https://databricks-atlan-com-genie-tab.onrender.com`) needs to be registered as a valid redirect URI in Atlan's Keycloak for standalone mode to work. Embedded mode (iframe) works without this.

### Medium Priority

- [ ] **UI refresh — Databricks branding** — Update colors from purple (#667eea) to Databricks red (#FF3621). Dark background (#1B3139), Databricks-themed bot avatar, "Powered by Databricks Genie" footer. Update user message bubbles, send button, focus states.

- [ ] **Remove debug logging** — The `businessAttributes` debug logging in `app.py` (lines that dump full metadata contents) should be removed or reduced to WARNING level before production use.

- [ ] **Error handling for token expiry** — Add frontend handling for 401 responses mid-session (token expired). Should prompt re-auth or auto-refresh via SDK.

### Low Priority

- [ ] **Custom domain** — Consider setting up a custom domain on Render instead of `*.onrender.com` to avoid Chrome's phishing warning (domain contains "atlan").

- [ ] **Production WSGI** — Currently using gunicorn with default settings. May want to tune worker count, timeouts for the Genie API polling.

## Genie Assets (genie-assets/)

### High Priority

- [ ] **Validate scripts against databricks.atlan.com** — Scripts were last tested against `partner-sandbox.atlan.com`. Need to run the full workflow (01-04) against the new instance.

- [ ] **Review and clean up scripts** — The scripts were copied from the original project. May need updates for the new Atlan instance, connection names, etc.

### Medium Priority

- [ ] **Automate sync** — Currently manual CLI execution. Consider scheduled runs (cron, Temporal, or Databricks Jobs) to keep Atlan assets in sync with Genie Spaces.

- [ ] **Lineage script generalization** — `04_create_lineage_wide_world.py` is hardcoded for the "Wide World Importers" demo space. Needs to be generalized for any Genie Space.

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
