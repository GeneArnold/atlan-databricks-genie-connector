#!/usr/bin/env python3
"""
Script 3: Extract from Databricks and Sync Genie Spaces to Atlan
=================================================================
Extracts Genie Spaces from Databricks API and creates/updates
CustomEntity assets in Atlan with custom metadata and READMEs.

Auto-discovers the Genie connection in Atlan (no connection_info.txt needed,
though it will use it as a fallback if present).

Author: Gene Arnold
"""

import os
import json
from datetime import datetime
from typing import List, Dict, Optional
from dotenv import load_dotenv
import requests

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Connection, CustomEntity, Readme
from pyatlan.model.custom_metadata import CustomMetadataDict
from pyatlan.model.fluent_search import FluentSearch

print("=" * 70)
print("SCRIPT 3: EXTRACT AND SYNC GENIE SPACES TO ATLAN")
print("=" * 70)
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# ============================================================================
# SETUP
# ============================================================================

print("🔧 Loading environment...")
load_dotenv()

ATLAN_BASE_URL = os.getenv("ATLAN_BASE_URL")
ATLAN_API_KEY = os.getenv("ATLAN_API_KEY")

if not ATLAN_BASE_URL or not ATLAN_API_KEY:
    print("❌ Missing ATLAN_BASE_URL or ATLAN_API_KEY in .env!")
    exit(1)

# Load Databricks workspaces — multi-workspace config or single from .env
WORKSPACES_FILE = "databricks_workspaces.json"
DATABRICKS_WORKSPACES = []

if os.path.exists(WORKSPACES_FILE):
    with open(WORKSPACES_FILE) as f:
        DATABRICKS_WORKSPACES = json.load(f)
    print(f"✓ Loaded {len(DATABRICKS_WORKSPACES)} workspace(s) from {WORKSPACES_FILE}")
else:
    # Fallback to single workspace from .env
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    if not host or not token:
        print("❌ No databricks_workspaces.json and no DATABRICKS_HOST/TOKEN in .env!")
        exit(1)
    DATABRICKS_WORKSPACES = [{"name": "default", "host": host, "token": token}]
    print(f"✓ Using single workspace from .env")

print(f"✓ Atlan: {ATLAN_BASE_URL}")
for ws in DATABRICKS_WORKSPACES:
    print(f"✓ Databricks: {ws['host']} ({ws['name']})")

# Initialize Atlan client
client = AtlanClient(base_url=ATLAN_BASE_URL, api_key=ATLAN_API_KEY)


# ============================================================================
# AUTO-DISCOVER GENIE CONNECTION
# ============================================================================

def discover_genie_connection() -> str:
    """Find the Genie connection QN in Atlan, or fall back to connection_info.txt."""
    print("\n🔍 Auto-discovering Genie connection in Atlan...")

    try:
        search_request = (
            FluentSearch()
            .where(Connection.CONNECTOR_NAME.eq("databricks-genie"))
            .page_size(5)
        ).to_request()

        results = client.asset.search(search_request)
        for asset in results:
            if isinstance(asset, Connection):
                print(f"✓ Found connection: {asset.qualified_name}")
                return asset.qualified_name
    except Exception as e:
        print(f"⚠ Search failed: {e}")

    # Fallback to connection_info.txt
    if os.path.exists("connection_info.txt"):
        print("⚠ Falling back to connection_info.txt...")
        with open("connection_info.txt", "r") as f:
            for line in f:
                if "CONNECTION_QN=" in line:
                    qn = line.strip().split("=", 1)[1]
                    print(f"✓ Using connection QN from file: {qn}")
                    return qn

    print("❌ Could not find Genie connection! Run 01_create_genie_connection.py first.")
    exit(1)


CONNECTION_QN = discover_genie_connection()


# ============================================================================
# STEP 1: EXTRACT FROM DATABRICKS
# ============================================================================

print("\n" + "=" * 70)
print("STEP 1: EXTRACTING FROM DATABRICKS")
print("=" * 70)


def extract_metadata_from_serialized(serialized_space) -> Dict:
    """Extract metadata from a v2 serialized_space payload.

    v2 shape:
      { version: 2,
        config:       { sample_questions: [{id, question:[str]}] },
        data_sources: { tables: [...], metric_views: [{identifier}] },
        instructions: { text_instructions: [{id, content:[str]}],
                        join_specs: [...],
                        sql_snippets: { filters, measures, dimensions } } }
    """
    if isinstance(serialized_space, str):
        try:
            serialized_space = json.loads(serialized_space)
        except (json.JSONDecodeError, TypeError):
            return create_empty_metadata()
    if not isinstance(serialized_space, dict):
        return create_empty_metadata()

    meta = create_empty_metadata()

    ds = serialized_space.get("data_sources") or {}
    if isinstance(ds, dict):
        meta["tables"]       = ds.get("tables") or []
        meta["metric_views"] = ds.get("metric_views") or []

    for q in (serialized_space.get("config") or {}).get("sample_questions", []):
        if isinstance(q, dict):
            meta["sample_questions"].append(" ".join(q.get("question") or []))
        elif isinstance(q, str):
            meta["sample_questions"].append(q)

    instr = serialized_space.get("instructions") or {}
    if isinstance(instr, dict):
        text_blocks = []
        for block in instr.get("text_instructions") or []:
            if isinstance(block, dict):
                text_blocks.append("".join(block.get("content") or []))
        meta["instructions"] = "\n\n".join(b for b in text_blocks if b) or None

        meta["join_specs"]      = instr.get("join_specs") or []
        meta["example_queries"] = instr.get("example_question_sqls") or []

        snippets = instr.get("sql_snippets") or {}
        if isinstance(snippets, dict):
            meta["filters"]    = snippets.get("filters") or []
            meta["measures"]   = snippets.get("measures") or []
            meta["dimensions"] = snippets.get("dimensions") or []

    return meta


def create_empty_metadata() -> Dict:
    return {
        "tables": [],
        "metric_views": [],
        "sample_questions": [],
        "instructions": None,
        "join_specs": [],
        "example_queries": [],
        "filters": [],
        "measures": [],
        "dimensions": [],
    }


def extract_from_workspace(host: str, token: str, workspace_name: str) -> List[Dict]:
    """Extract Genie Spaces from a single Databricks workspace."""
    print(f"\n🔍 Fetching from {host} ({workspace_name})...")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Get list of spaces
    list_url = f"{host}/api/2.0/genie/spaces"
    response = requests.get(list_url, headers=headers, timeout=30)
    response.raise_for_status()

    # Databricks returns the workspace org id (the `?o=` share-URL param) as a
    # response header on every API call. Capture it once for use in README links.
    workspace_org_id = response.headers.get("x-databricks-org-id", "")

    spaces = response.json().get("spaces", [])
    print(f"   ✅ Found {len(spaces)} Genie Space(s)  (org id: {workspace_org_id or 'unknown'})")

    # Get detailed info for each space
    detailed_spaces = []
    for space in spaces:
        title = space.get("title", "Unknown")
        space_id = space["space_id"]
        print(f"   📊 Fetching details for: {title}")

        detail_url = f"{host}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true"
        detail_response = requests.get(detail_url, headers=headers, timeout=30)

        if detail_response.status_code == 200:
            detailed = detail_response.json()

            # Merge timestamps and workspace info into basic info
            for key in ("created_timestamp", "last_updated_timestamp", "parent_path"):
                if key in detailed:
                    space[key] = detailed[key]
            space["_workspace_name"] = workspace_name
            space["_workspace_host"] = host
            space["_workspace_org_id"] = workspace_org_id

            metadata = extract_metadata_from_serialized(detailed.get("serialized_space", {}))
            detailed_spaces.append({
                "basic_info": space,
                "extracted_metadata": metadata,
            })
        else:
            print(f"      ⚠ Failed to get details: {detail_response.status_code}")

    return detailed_spaces


# Extract from all Databricks workspaces
genie_spaces = []
for ws in DATABRICKS_WORKSPACES:
    try:
        spaces = extract_from_workspace(ws["host"], ws["token"], ws["name"])
        genie_spaces.extend(spaces)
    except Exception as e:
        print(f"   ❌ Error extracting from {ws['name']}: {e}")

print(f"\n✅ Total: {len(genie_spaces)} Genie Space(s) across {len(DATABRICKS_WORKSPACES)} workspace(s)")

# Save to JSON for reference
print(f"\n💾 Saving extracted data to genie_spaces_detailed_analysis.json...")
with open("genie_spaces_detailed_analysis.json", "w") as f:
    json.dump(genie_spaces, f, indent=2)
print(f"✓ Saved {len(genie_spaces)} space(s)")


# ============================================================================
# STEP 2: SYNC TO ATLAN
# ============================================================================

print("\n" + "=" * 70)
print("STEP 2: SYNCING TO ATLAN")
print("=" * 70)

# Find existing entities
print("\n🔍 Checking for existing entities in Atlan...")
existing_entities = {}
try:
    search_request = (
        FluentSearch()
        .where(CustomEntity.CONNECTION_QUALIFIED_NAME.eq(CONNECTION_QN))
        .page_size(100)
    ).to_request()

    results = client.asset.search(search_request)
    for asset in results:
        if isinstance(asset, CustomEntity):
            existing_entities[asset.name] = asset

    print(f"✓ Found {len(existing_entities)} existing entity(ies)")
except Exception as e:
    print(f"⚠ No existing entities found: {e}")


def generate_readme(basic_info: Dict, metadata: Dict) -> str:
    """Generate README content for a Genie Space (v2 payload shape)."""
    title = basic_info.get("title", "Unknown")
    space_id = basic_info.get("space_id", "N/A")
    warehouse_id = basic_info.get("warehouse_id", "N/A")
    workspace_host = basic_info.get("_workspace_host", "")
    workspace_org_id = basic_info.get("_workspace_org_id", "")
    created_by = basic_info.get("parent_path", "").replace("/Users/", "") or "Unknown"

    lines: List[str] = []

    def section(heading: str) -> None:
        if lines:
            lines.extend(["", "", "---", "", ""])
        lines.append(f"## {heading}")
        lines.extend(["", ""])

    # Header
    lines.append(f"# {title}")
    lines.extend(["", ""])
    lines.append("Databricks Genie AI-powered analytics space for natural language data querying and exploration.")

    # Tables
    tables = metadata.get("tables", [])
    section(f"Tables Used ({len(tables)})")
    if tables:
        for t in tables:
            if not isinstance(t, dict):
                lines.append(f"- **{t}**")
                lines.append("")
                continue
            ident = t.get("identifier") or t.get("name") or str(t)
            cols = t.get("column_configs") or []
            col_word = "column" if len(cols) == 1 else "columns"
            desc = t.get("description") or []
            desc_text = desc[0] if isinstance(desc, list) and desc else (desc if isinstance(desc, str) else "")
            lines.append(f"- **{ident}** — {len(cols)} {col_word}")
            if desc_text:
                lines.append("")
                lines.append(f"  > {desc_text}")
            lines.append("")
    else:
        lines.append("_No tables configured_")

    # Metric Views
    metric_views = metadata.get("metric_views", [])
    if metric_views:
        section(f"Metric Views ({len(metric_views)})")
        for mv in metric_views:
            ident = mv.get("identifier") if isinstance(mv, dict) else str(mv)
            lines.append(f"- **{ident}**")

    # Sample Questions
    questions = metadata.get("sample_questions", [])
    section(f"Sample Questions ({len(questions)})")
    if questions:
        for i, q in enumerate(questions, 1):
            lines.append(f"{i}. {q}")
    else:
        lines.append("_No sample questions configured_")

    # Example Queries — saved question + verified SQL pairs
    example_queries = metadata.get("example_queries", [])
    if example_queries:
        section(f"Example Queries ({len(example_queries)})")
        for i, eq in enumerate(example_queries):
            if not isinstance(eq, dict):
                continue
            if i > 0:
                lines.extend(["", ""])
            q_parts = eq.get("question") or []
            q_text = " ".join(q_parts).strip() if isinstance(q_parts, list) else str(q_parts).strip()
            sql_parts = eq.get("sql") or []
            sql_text = "".join(sql_parts).strip() if isinstance(sql_parts, list) else str(sql_parts).strip()
            lines.append(f"### {q_text or 'Example'}")
            lines.append("")
            if sql_text:
                lines.append("```sql")
                lines.append(sql_text)
                lines.append("```")

    # Business Context — full text_instructions verbatim
    instructions = metadata.get("instructions")
    if instructions:
        section("Business Context")
        lines.append(str(instructions).strip())

    # Table Relationships
    joins = metadata.get("join_specs", [])
    if joins:
        section(f"Table Relationships ({len(joins)})")
        for idx, j in enumerate(joins):
            if not isinstance(j, dict):
                continue
            if idx > 0:
                lines.extend(["", ""])
            left  = j.get("left") or {}
            right = j.get("right") or {}
            left_id  = left.get("identifier")  if isinstance(left, dict)  else str(left)
            right_id = right.get("identifier") if isinstance(right, dict) else str(right)
            instr_text   = " ".join(j.get("instruction") or []).strip()
            comment_text = " ".join(j.get("comment") or []).strip()
            sql_parts = j.get("sql") or []
            lines.append(f"### `{left_id}` ⟷ `{right_id}`")
            lines.append("")
            if instr_text:
                lines.append(f"**Join type:** {instr_text}")
                lines.append("")
            if comment_text:
                lines.append(f"**Comment:** {comment_text}")
                lines.append("")
            if sql_parts:
                sql_text = " AND ".join(s.strip() for s in sql_parts if s and s.strip())
                if sql_text:
                    lines.append("```sql")
                    lines.append(sql_text)
                    lines.append("```")

    # SQL Library — filters, measures, dimensions
    filters    = metadata.get("filters", [])
    measures   = metadata.get("measures", [])
    dimensions = metadata.get("dimensions", [])
    if filters or measures or dimensions:
        section("SQL Library")
        first_subsection = True
        for label, items in (("Filters", filters), ("Measures", measures), ("Dimensions", dimensions)):
            if not items:
                continue
            if not first_subsection:
                lines.extend(["", ""])
            first_subsection = False
            lines.append(f"### {label} ({len(items)})")
            lines.append("")
            for i, it in enumerate(items):
                if not isinstance(it, dict):
                    continue
                if i > 0:
                    lines.append("")
                name = it.get("display_name") or it.get("alias") or it.get("id", "unnamed")
                sql_parts = it.get("sql") or []
                sql_text = "\n".join(sql_parts) if isinstance(sql_parts, list) else str(sql_parts)
                lines.append(f"**{name}**")
                lines.append("")
                if sql_text.strip():
                    lines.append("```sql")
                    lines.append(sql_text.strip())
                    lines.append("```")

    # Links & Metadata
    section("Links & Metadata")
    lines.append(f"- **Space ID**: `{space_id}`")
    lines.append(f"- **Warehouse ID**: `{warehouse_id}`")
    lines.append(f"- **Created By**: {created_by}")
    if workspace_host:
        db_url = f"{workspace_host}/genie/rooms/{space_id}"
        if workspace_org_id:
            db_url += f"?o={workspace_org_id}"
        lines.append(f"- **Open in Databricks**: [{title}]({db_url})")
    lines.extend(["", "", "---", ""])
    lines.append(f"*Last synced: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

    return "\n".join(lines) + "\n"


# Process each space
print("\n" + "=" * 70)
print("PROCESSING GENIE SPACES")
print("=" * 70)

entities_to_save = []

for space_data in genie_spaces:
    basic_info = space_data.get("basic_info", {})
    metadata = space_data.get("extracted_metadata", {})

    space_id = basic_info.get("space_id", "unknown")
    title = basic_info.get("title", "Unknown Genie Space")
    warehouse_id = basic_info.get("warehouse_id", "unknown")

    print(f"\n📊 Processing: {title}")
    print(f"   Space ID: {space_id}")

    # Check if exists
    existing = existing_entities.get(title)

    if existing:
        print(f"   ✓ Found existing entity (GUID: {existing.guid})")
        entity = existing
    else:
        print(f"   🆕 Creating new entity")
        # Build manually — CustomEntity.creator() can't handle custom connector types
        entity = CustomEntity()
        entity.name = title
        entity.qualified_name = f"{CONNECTION_QN}/{title}"
        entity.connection_qualified_name = CONNECTION_QN
        entity.connector_name = "databricks-genie"

    # Set attributes
    entity.sub_type = "Genie Space"
    entity.user_description = "Databricks Genie Space for AI-powered data exploration"

    # Build table list string
    tables = metadata.get("tables", [])
    table_names = []
    for t in tables:
        if isinstance(t, dict):
            table_names.append(t.get("identifier", t.get("name", str(t))))
        else:
            table_names.append(str(t))
    tables_str = ", ".join(table_names)

    # Extract creator from parent_path
    created_by = basic_info.get("parent_path", "").replace("/Users/", "") or "Unknown"

    # Apply custom metadata
    cm = CustomMetadataDict("Genie Spaces Details")
    cm["spaceId"] = space_id
    cm["warehouseId"] = warehouse_id
    cm["tableCount"] = len(tables)
    cm["tables"] = tables_str
    cm["hasInstructions"] = bool(metadata.get("instructions"))
    cm["category"] = "AI/BI"
    cm["createdBy"] = created_by
    cm["workspaceUrl"] = basic_info.get("_workspace_host", "")
    sample_questions = metadata.get("sample_questions", [])
    cm["sampleQuestions"] = json.dumps(sample_questions) if sample_questions else ""
    entity.set_custom_metadata(cm)

    entities_to_save.append(entity)


# ============================================================================
# SAVE TO ATLAN
# ============================================================================

print("\n" + "=" * 70)
print("SAVING TO ATLAN")
print("=" * 70)

if entities_to_save:
    print(f"\n💾 Saving {len(entities_to_save)} entity(ies)...")
    try:
        response = client.asset.save(entities_to_save)
        created = response.assets_created(asset_type=CustomEntity)
        updated = response.assets_updated(asset_type=CustomEntity)
        print(f"✅ Created: {len(created)}  |  Updated: {len(updated)}")

    except Exception as e:
        print(f"❌ Error saving entities: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
else:
    print("ℹ No entities to save")

# PyAtlan's save() doesn't reliably push custom metadata updates for existing entities.
# Use the Atlan REST API businessmetadata endpoint to ensure CM fields are written.
print(f"\n📋 Updating custom metadata via REST API...")
_api_headers = {
    "Authorization": f"Bearer {ATLAN_API_KEY}",
    "Content-Type": "application/json",
}

# Resolve internal hashed names for the CM set and its fields
_cm_internal_name = None
_cm_field_map = {}  # {display_name: internal_name}
try:
    resp = requests.get(
        f"{ATLAN_BASE_URL}/api/meta/types/typedefs",
        headers=_api_headers, timeout=30,
    )
    resp.raise_for_status()
    for bm in resp.json().get("businessMetadataDefs", []):
        if bm.get("displayName") == "Genie Spaces Details":
            _cm_internal_name = bm["name"]
            for attr in bm.get("attributeDefs", []):
                _cm_field_map[attr.get("displayName", "")] = attr["name"]
            break
except Exception as e:
    print(f"   ⚠ Failed to resolve CM field names: {e}")

if _cm_internal_name and _cm_field_map:
    for space_data in genie_spaces:
        basic_info = space_data.get("basic_info", {})
        metadata = space_data.get("extracted_metadata", {})
        title = basic_info.get("title", "Unknown")
        space_id = basic_info.get("space_id", "")
        warehouse_id = basic_info.get("warehouse_id", "")
        workspace_host = basic_info.get("_workspace_host", "")
        sample_questions = metadata.get("sample_questions", [])

        # Find the entity's GUID
        entity = existing_entities.get(title)
        if not entity or not entity.guid:
            # Try to find newly created entity
            try:
                sr = (
                    FluentSearch()
                    .where(CustomEntity.CONNECTION_QUALIFIED_NAME.eq(CONNECTION_QN))
                    .where(CustomEntity.NAME.eq(title))
                    .page_size(1)
                ).to_request()
                for a in client.asset.search(sr):
                    if isinstance(a, CustomEntity):
                        entity = a
                        break
            except Exception:
                pass

        if not entity or not entity.guid:
            print(f"   ⚠ Skipping CM update for {title} — no GUID")
            continue

        # Build CM payload with internal field names
        tables = metadata.get("tables", [])
        table_names = []
        for t in tables:
            if isinstance(t, dict):
                table_names.append(t.get("identifier", t.get("name", str(t))))
            else:
                table_names.append(str(t))

        created_by = basic_info.get("parent_path", "").replace("/Users/", "") or "Unknown"

        cm_payload = {}
        field_values = {
            "spaceId": space_id,
            "warehouseId": warehouse_id,
            "tableCount": len(tables),
            "tables": ", ".join(table_names),
            "hasInstructions": bool(metadata.get("instructions")),
            "category": "AI/BI",
            "createdBy": created_by,
            "workspaceUrl": workspace_host,
            "sampleQuestions": json.dumps(sample_questions) if sample_questions else "",
        }

        for display_name, value in field_values.items():
            internal = _cm_field_map.get(display_name)
            if internal:
                cm_payload[internal] = value

        try:
            resp = requests.post(
                f"{ATLAN_BASE_URL}/api/meta/entity/guid/{entity.guid}/businessmetadata?isOverwrite=false",
                headers=_api_headers,
                json={_cm_internal_name: cm_payload},
                timeout=30,
            )
            if resp.status_code == 204:
                print(f"   ✓ CM: {title}")
            else:
                print(f"   ⚠ CM update for {title}: HTTP {resp.status_code}")
        except Exception as e:
            print(f"   ⚠ CM update for {title} failed: {e}")
else:
    print("   ⚠ Could not resolve CM field names — skipping REST API update")

# PyAtlan's CustomEntity schema has no `assetUserDefinedType` field, so save() can't
# write it. That attribute is what drives the "Genie Space" vs "Custom" UI label in
# Atlan's asset-type filter. Set it via REST on every synced entity.
print(f"\n🏷  Setting assetUserDefinedType via REST API...")
for space_data in genie_spaces:
    title = space_data["basic_info"]["title"]
    entity = existing_entities.get(title)
    if not entity or not entity.guid:
        try:
            sr = (
                FluentSearch()
                .where(CustomEntity.CONNECTION_QUALIFIED_NAME.eq(CONNECTION_QN))
                .where(CustomEntity.NAME.eq(title))
                .page_size(1)
            ).to_request()
            for a in client.asset.search(sr):
                if isinstance(a, CustomEntity):
                    entity = a
                    break
        except Exception:
            pass

    if not entity or not entity.guid:
        print(f"   ⚠ Skipping assetUserDefinedType for {title} — no GUID")
        continue

    try:
        resp = requests.post(
            f"{ATLAN_BASE_URL}/api/meta/entity/bulk",
            headers=_api_headers,
            json={"entities": [{
                "typeName": "CustomEntity",
                "guid": entity.guid,
                "attributes": {
                    "qualifiedName": entity.qualified_name,
                    "name": title,
                    "assetUserDefinedType": "Genie Space",
                },
            }]},
            timeout=30,
        )
        if resp.status_code in (200, 204):
            print(f"   ✓ assetUserDefinedType: {title}")
        else:
            print(f"   ⚠ assetUserDefinedType for {title}: HTTP {resp.status_code} — {resp.text[:200]}")
    except Exception as e:
        print(f"   ⚠ assetUserDefinedType for {title} failed: {e}")

# Always update READMEs for all entities (README content isn't diffed by PyAtlan)
print(f"\n📝 Updating READMEs for all {len(entities_to_save)} entity(ies)...")
for space_data in genie_spaces:
    title = space_data["basic_info"]["title"]
    entity = existing_entities.get(title)
    if not entity or not entity.guid:
        # Might be a newly created entity — look it up
        try:
            search_request = (
                FluentSearch()
                .where(CustomEntity.CONNECTION_QUALIFIED_NAME.eq(CONNECTION_QN))
                .where(CustomEntity.NAME.eq(title))
                .page_size(1)
            ).to_request()
            results = client.asset.search(search_request)
            for asset in results:
                if isinstance(asset, CustomEntity):
                    entity = asset
                    break
        except Exception:
            pass

    if not entity or not entity.guid:
        print(f"   ⚠ Could not find entity for: {title}")
        continue

    readme_content = generate_readme(
        space_data["basic_info"],
        space_data["extracted_metadata"],
    )
    readme = Readme.creator(asset=entity, content=readme_content)
    try:
        client.asset.save(readme)
        print(f"   ✓ README: {title}")
    except Exception as e:
        print(f"   ⚠ README failed for {title}: {e}")


# ============================================================================
# REMOVE ORPHANED ENTITIES
# ============================================================================
# Entities in Atlan that no longer have a matching Genie Space in Databricks

print("\n" + "=" * 70)
print("CHECKING FOR ORPHANED ENTITIES")
print("=" * 70)

synced_names = {space_data["basic_info"]["title"] for space_data in genie_spaces}
orphaned = {name: entity for name, entity in existing_entities.items() if name not in synced_names}

deleted_count = 0
if orphaned:
    print(f"\n🗑 Found {len(orphaned)} orphaned entity(ies) (no longer in Databricks):")
    for name, entity in orphaned.items():
        print(f"   - {name} (GUID: {entity.guid})")

    for name, entity in orphaned.items():
        try:
            client.asset.delete_by_guid(entity.guid)
            print(f"   ✅ Deleted: {name}")
            deleted_count += 1
        except Exception as e:
            print(f"   ⚠ Failed to delete {name}: {e}")
else:
    print("\n✓ No orphaned entities found")


# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print(f"✅ Extracted {len(genie_spaces)} Genie Space(s) from Databricks")
print(f"✅ Synced {len(entities_to_save)} entity(ies) to Atlan")
if deleted_count:
    print(f"🗑 Deleted {deleted_count} orphaned entity(ies)")
print(f"✅ Connection: {CONNECTION_QN}")
print(f"✅ Data saved to genie_spaces_detailed_analysis.json")
print(f"\n✨ Sync complete!")
print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
