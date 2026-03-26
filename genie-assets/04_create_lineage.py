#!/usr/bin/env python3
"""
Script 4: Create Lineage for Genie Spaces
==========================================
Creates lineage between Genie Space CustomEntity assets and their
source tables in Databricks. Works for ANY Genie Space — reads the
table list from the last Databricks extraction (genie_spaces_detailed_analysis.json)
and looks up each table in Atlan via search.

Can target a single space by name or process all spaces.

Usage:
  python 04_create_lineage.py                          # All spaces
  python 04_create_lineage.py "Wide World Importers"   # Single space (partial match)

Result:
  [table_1]  ────┐
  [table_2]  ────┤
  [table_3]  ────┼─→ [Process] ─→ [Genie Space]
  [table_N]  ────┘

Author: Gene Arnold
"""

import os
import sys
import json
from datetime import datetime
from dotenv import load_dotenv
import requests

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Connection, CustomEntity, Table, Process
from pyatlan.model.fluent_search import FluentSearch

print("=" * 70)
print("SCRIPT 4: CREATE LINEAGE FOR GENIE SPACES")
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

client = AtlanClient(base_url=ATLAN_BASE_URL, api_key=ATLAN_API_KEY)
print("✅ Connected to Atlan")

REST_HEADERS = {
    "Authorization": f"Bearer {ATLAN_API_KEY}",
    "Content-Type": "application/json",
}

# Optional: filter to a specific space name
TARGET_SPACE = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else None
if TARGET_SPACE:
    print(f"🎯 Targeting space: '{TARGET_SPACE}'")


# ============================================================================
# STEP 1: DISCOVER CONNECTIONS & LOAD EXTRACTION DATA
# ============================================================================

print("\n🔍 Step 1: Setup...")


def find_genie_connection_qn() -> str:
    """Find the Genie connection QN via REST search."""
    body = {
        "dsl": {
            "from": 0,
            "size": 5,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"__typeName.keyword": "Connection"}},
                        {"term": {"connectorName": "databricks-genie"}},
                    ]
                }
            },
        },
        "attributes": ["qualifiedName"],
    }
    resp = requests.post(
        f"{ATLAN_BASE_URL}/api/meta/search/indexsearch",
        headers=REST_HEADERS, json=body, timeout=30,
    )
    resp.raise_for_status()
    entities = resp.json().get("entities", [])
    if entities:
        return entities[0]["attributes"]["qualifiedName"]

    # Fallback
    if os.path.exists("connection_info.txt"):
        with open("connection_info.txt", "r") as f:
            for line in f:
                if "CONNECTION_QN=" in line:
                    return line.strip().split("=", 1)[1]

    print("❌ Genie connection not found!")
    exit(1)


GENIE_CONNECTION_QN = find_genie_connection_qn()
print(f"✓ Genie connection: {GENIE_CONNECTION_QN}")

# Load extracted Genie Space data (from script 03)
EXTRACTION_FILE = "genie_spaces_detailed_analysis.json"
if not os.path.exists(EXTRACTION_FILE):
    print(f"❌ {EXTRACTION_FILE} not found! Run 03_extract_and_sync_genie_spaces.py first.")
    exit(1)

with open(EXTRACTION_FILE) as f:
    extracted_spaces = json.load(f)
print(f"✓ Loaded {len(extracted_spaces)} space(s) from {EXTRACTION_FILE}")


# ============================================================================
# STEP 2: FIND GENIE SPACE ENTITIES IN ATLAN
# ============================================================================

print(f"\n🔍 Step 2: Finding Genie Space entities in Atlan...")

search_body = {
    "dsl": {
        "from": 0,
        "size": 50,
        "query": {
            "bool": {
                "must": [
                    {"term": {"__typeName.keyword": "CustomEntity"}},
                    {"term": {"connectionQualifiedName": GENIE_CONNECTION_QN}},
                ]
            }
        },
    },
    "attributes": ["name", "qualifiedName"],
}

resp = requests.post(
    f"{ATLAN_BASE_URL}/api/meta/search/indexsearch",
    headers=REST_HEADERS, json=search_body, timeout=30,
)
resp.raise_for_status()
atlan_entities = {
    e["attributes"]["name"]: e["guid"]
    for e in resp.json().get("entities", [])
}
print(f"✓ Found {len(atlan_entities)} Genie Space entity(ies) in Atlan")


# ============================================================================
# STEP 3: BUILD TABLE INDEX VIA SEARCH
# ============================================================================

print(f"\n🔍 Step 3: Building table index...")


def search_table_by_qn_suffix(catalog_schema_table: str) -> dict:
    """
    Search for a table by its catalog.schema.table qualified name suffix.
    Returns {guid, qualifiedName} or None.
    Uses wildcard search on qualifiedName to find across all connections.
    """
    # Convert catalog.schema.table to */catalog/schema/table
    path = catalog_schema_table.replace(".", "/")

    body = {
        "dsl": {
            "from": 0,
            "size": 5,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"__typeName.keyword": "Table"}},
                        {"wildcard": {"qualifiedName": f"default/databricks/*/{path}"}},
                    ]
                }
            },
        },
        "attributes": ["name", "qualifiedName", "connectionQualifiedName"],
    }

    resp = requests.post(
        f"{ATLAN_BASE_URL}/api/meta/search/indexsearch",
        headers=REST_HEADERS, json=body, timeout=30,
    )
    resp.raise_for_status()
    entities = resp.json().get("entities", [])

    if entities:
        # Return the first match
        return {
            "guid": entities[0]["guid"],
            "qualifiedName": entities[0]["attributes"]["qualifiedName"],
        }
    return None


# ============================================================================
# STEP 4: CREATE LINEAGE FOR EACH SPACE
# ============================================================================

print(f"\n🔗 Step 4: Creating lineage...")

total_created = 0
total_skipped = 0

for space_data in extracted_spaces:
    basic_info = space_data.get("basic_info", {})
    metadata = space_data.get("extracted_metadata", {})

    name = basic_info.get("title", "Unknown")
    space_id = basic_info.get("space_id", "")

    # Filter by target if specified
    if TARGET_SPACE and TARGET_SPACE.lower() not in name.lower():
        continue

    print(f"\n{'─'*60}")
    print(f"📊 {name}")

    # Check entity exists in Atlan
    entity_guid = atlan_entities.get(name)
    if not entity_guid:
        print(f"   ⚠ Entity not found in Atlan, skipping")
        total_skipped += 1
        continue

    # Get table list from extraction data
    tables = metadata.get("tables", [])
    if not tables:
        print(f"   ⚠ No tables in extraction data, skipping")
        total_skipped += 1
        continue

    # Parse table names
    table_qnames = []
    for t in tables:
        if isinstance(t, dict):
            table_qnames.append(t.get("identifier", t.get("name", str(t))))
        else:
            table_qnames.append(str(t))

    print(f"   Tables ({len(table_qnames)}): {', '.join(t.split('.')[-1] for t in table_qnames)}")

    # Look up each table via search
    table_references = []
    for table_qname in table_qnames:
        result = search_table_by_qn_suffix(table_qname)
        if result:
            table_references.append(Table.ref_by_guid(result["guid"]))
            conn_name = result["qualifiedName"].split("/")[2]  # connection epoch
            print(f"   ✅ {table_qname.split('.')[-1]} ({result['qualifiedName']})")
        else:
            print(f"   ⚠ {table_qname} — not found")

    if not table_references:
        print(f"   ⚠ No tables found in Atlan, skipping lineage")
        total_skipped += 1
        continue

    # Create Process asset for lineage
    process_name = f"Genie Query: {name}"
    print(f"   🔗 Creating process: '{process_name}'")
    print(f"      {len(table_references)} input(s) → {name}")

    process = Process.creator(
        name=process_name,
        connection_qualified_name=GENIE_CONNECTION_QN,
        inputs=table_references,
        outputs=[CustomEntity.ref_by_guid(entity_guid)],
    )
    process.description = (
        f"Databricks Genie AI queries against {len(table_references)} "
        f"source table(s)"
    )

    try:
        response = client.asset.save(process)
        created_processes = response.assets_created(asset_type=Process)
        if created_processes:
            print(f"   ✅ Lineage created! Process GUID: {created_processes[0].guid}")
        else:
            updated = response.assets_updated(asset_type=Process)
            if updated:
                print(f"   ✅ Lineage updated! Process GUID: {updated[0].guid}")
            else:
                print(f"   ✅ Lineage saved (no changes detected)")
        total_created += 1
    except Exception as e:
        print(f"   ❌ Error creating lineage: {e}")
        total_skipped += 1


# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print(f"✅ Lineage created/updated: {total_created}")
if total_skipped:
    print(f"⚠ Skipped: {total_skipped}")
print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
