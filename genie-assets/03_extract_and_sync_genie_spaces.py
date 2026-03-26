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
    """Extract metadata from serialized_space field."""
    if isinstance(serialized_space, str):
        try:
            serialized_space = json.loads(serialized_space)
        except (json.JSONDecodeError, TypeError):
            return create_empty_metadata()

    if not isinstance(serialized_space, dict):
        return create_empty_metadata()

    metadata = create_empty_metadata()

    # Extract data sources and tables
    data_sources = serialized_space.get("data_sources", {})
    if isinstance(data_sources, dict):
        metadata["tables"] = data_sources.get("tables", []) or data_sources.get("metric_views", [])
        metadata["table_descriptions"] = data_sources.get("table_descriptions", {})
        metadata["column_configs"] = data_sources.get("column_configurations", [])

    # Extract instructions
    if "text_instructions" in serialized_space:
        metadata["instructions"] = serialized_space["text_instructions"]
    elif "instructions" in serialized_space and isinstance(serialized_space["instructions"], dict):
        instr = serialized_space["instructions"]
        if "example_question_sqls" in instr:
            all_text = []
            for item in instr["example_question_sqls"]:
                all_text.extend(item.get("question", []))
                all_text.extend(item.get("sql", []))
            metadata["instructions"] = "".join(all_text)

    # Extract sample questions
    if "sample_questions" in serialized_space:
        metadata["sample_questions"] = serialized_space["sample_questions"]
    elif "config" in serialized_space:
        raw_questions = serialized_space.get("config", {}).get("sample_questions", [])
        for q in raw_questions:
            if isinstance(q, dict) and "question" in q:
                metadata["sample_questions"].append(" ".join(q["question"]))
            elif isinstance(q, str):
                metadata["sample_questions"].append(q)

    # Extract SQL examples
    if "sql_examples" in serialized_space:
        metadata["example_sql"] = serialized_space["sql_examples"]
    elif "instructions" in serialized_space and isinstance(serialized_space["instructions"], dict):
        metadata["example_sql"] = serialized_space["instructions"].get("example_question_sqls", [])

    # Extract SQL snippets
    for snippet in serialized_space.get("sql_snippets", []):
        snippet_type = snippet.get("type", "unknown")
        if snippet_type == "filter":
            metadata["filters"].append(snippet)
        elif snippet_type == "measure":
            metadata["measures"].append(snippet)
        elif snippet_type == "dimension":
            metadata["dimensions"].append(snippet)
        else:
            metadata["sql_snippets"].append(snippet)

    # Extract join specifications
    metadata["join_specs"] = serialized_space.get("join_specifications", [])

    return metadata


def create_empty_metadata() -> Dict:
    """Create empty metadata structure."""
    return {
        "tables": [],
        "table_descriptions": {},
        "column_configs": [],
        "sample_questions": [],
        "instructions": None,
        "example_sql": [],
        "join_specs": [],
        "sql_snippets": [],
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

    spaces = response.json().get("spaces", [])
    print(f"   ✅ Found {len(spaces)} Genie Space(s)")

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
    """Generate rich README content for a Genie Space, matching the original template."""
    title = basic_info.get("title", "Unknown")
    space_id = basic_info.get("space_id", "N/A")
    warehouse_id = basic_info.get("warehouse_id", "N/A")
    workspace_host = basic_info.get("_workspace_host", "")
    created_by = basic_info.get("parent_path", "").replace("/Users/", "") or "Unknown"

    content = f"# ✨ {title}\n\n"
    content += "## 📊 Quick Overview\n\n"
    content += "Databricks Genie AI-powered analytics space for natural language data querying and exploration.\n\n"

    # ── Tables section with column details ──
    tables = metadata.get("tables", [])
    content += f"## 📋 Tables Used ({len(tables)})\n\n"

    if tables:
        for table in tables:
            if isinstance(table, dict):
                table_name = table.get("identifier", table.get("name", str(table)))
                columns = table.get("column_configs", [])
                col_count = len(columns)
                entity_count = sum(1 for c in columns if c.get("enable_entity_matching"))

                col_info = f"{col_count} columns"
                if entity_count:
                    col_info += f" ({entity_count} with entity matching)"
                content += f"- **{table_name}** - {col_info}\n"
            else:
                content += f"- **{table}**\n"
        content += "\n"
    else:
        content += "No tables configured\n\n"

    # ── Sample Questions ──
    questions = metadata.get("sample_questions", [])
    content += "## ❓ Sample Questions\n\n"
    if questions:
        for i, question in enumerate(questions, 1):
            content += f"{i}. {question}\n"
        content += "\n"
    else:
        content += "No sample questions available\n\n"

    # ── Example SQL Queries ──
    example_sql = metadata.get("example_sql", [])
    content += "## 💻 Example SQL Queries\n\n"
    if example_sql:
        for ex in example_sql:
            if isinstance(ex, dict):
                question_parts = ex.get("question", [])
                sql_parts = ex.get("sql", [])
                question_text = " ".join(question_parts) if isinstance(question_parts, list) else str(question_parts)
                sql_text = "".join(sql_parts) if isinstance(sql_parts, list) else str(sql_parts)

                content += f"**Q: {question_text}**\n\n"
                content += f"```sql\n{sql_text.strip()}\n```\n\n"
            else:
                content += f"- {ex}\n"
        content += "\n"
    else:
        content += "No SQL examples available\n\n"

    # ── Instructions & Business Logic ──
    instructions = metadata.get("instructions")
    content += "## 📚 Instructions & Business Logic\n\n"
    if instructions:
        # Clean up: the instructions field sometimes contains concatenated SQL from example_question_sqls
        # Show a reasonable preview
        preview = str(instructions)
        if len(preview) > 1000:
            preview = preview[:1000] + "..."
        content += f"{preview}\n\n"
    else:
        content += "No detailed instructions configured\n\n"

    # ── SQL Components (filters, measures, dimensions) ──
    filters = metadata.get("filters", [])
    measures = metadata.get("measures", [])
    dimensions = metadata.get("dimensions", [])
    join_specs = metadata.get("join_specs", [])

    if any([filters, measures, dimensions, join_specs]):
        content += "## 🔧 SQL Components\n\n"

        if join_specs:
            content += f"### Joins ({len(join_specs)})\n\n"
            for join in join_specs:
                if isinstance(join, dict):
                    left = join.get("left_table", "")
                    right = join.get("right_table", "")
                    condition = join.get("join_condition", join.get("condition", ""))
                    content += f"- `{left}` ↔ `{right}`"
                    if condition:
                        content += f": `{condition}`"
                    content += "\n"
            content += "\n"

        if filters:
            content += f"### Filters ({len(filters)})\n\n"
            for f in filters:
                alias = f.get("alias", "unnamed")
                expr = f.get("sql_expression", "")
                content += f"- **{alias}**: `{expr}`\n"
            content += "\n"

        if measures:
            content += f"### Measures ({len(measures)})\n\n"
            for m in measures:
                alias = m.get("alias", "unnamed")
                expr = m.get("sql_expression", "")
                content += f"- **{alias}**: `{expr}`\n"
            content += "\n"

        if dimensions:
            content += f"### Dimensions ({len(dimensions)})\n\n"
            for d in dimensions:
                alias = d.get("alias", "unnamed")
                expr = d.get("sql_expression", "")
                content += f"- **{alias}**: `{expr}`\n"
            content += "\n"

    # ── Links & Metadata ──
    content += "## 🔗 Links & Metadata\n\n"
    content += f"- **Space ID**: `{space_id}`\n"
    content += f"- **Warehouse ID**: `{warehouse_id}`\n"
    content += f"- **Created By**: {created_by}\n"
    if workspace_host:
        content += f"- **Open in Databricks**: [{title} →]({workspace_host}/genie/rooms/{space_id})\n"
    content += f"\n---\n*Last synced: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n"

    return content


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
