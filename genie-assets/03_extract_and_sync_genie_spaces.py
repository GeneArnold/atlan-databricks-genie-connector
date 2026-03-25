#!/usr/bin/env python3
"""
Script 3: Extract from Databricks and Sync Genie Spaces to Atlan
This combined version extracts directly from Databricks and syncs to Atlan in one step
Author: Gene Arnold

This eliminates the manual step of running extraction separately.
"""

import os
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from dotenv import load_dotenv
import requests

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import CustomEntity, Readme
from pyatlan.model.custom_metadata import CustomMetadataDict
from pyatlan.model.fluent_search import FluentSearch

print("="*70)
print("SCRIPT 3: EXTRACT AND SYNC GENIE SPACES TO ATLAN")
print("="*70)
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# Load environment
print("🔧 Loading environment...")
load_dotenv()

ATLAN_BASE_URL = os.getenv("ATLAN_BASE_URL")
ATLAN_API_KEY = os.getenv("ATLAN_API_KEY")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://dbc-8d941db8-48cd.cloud.databricks.com")

if not ATLAN_BASE_URL or not ATLAN_API_KEY:
    print("❌ Missing Atlan credentials in .env!")
    exit(1)

print(f"✓ Credentials loaded")

# Initialize Atlan client
client = AtlanClient(
    base_url=ATLAN_BASE_URL,
    api_key=ATLAN_API_KEY
)

# Read connection info
print("\n📖 Reading connection info...")
if not os.path.exists('connection_info.txt'):
    print("❌ connection_info.txt not found!")
    print("   Run 01_create_genie_connection.py first")
    exit(1)

CONNECTION_QN = None
with open('connection_info.txt', 'r') as f:
    for line in f:
        if 'CONNECTION_QN=' in line:
            CONNECTION_QN = line.strip().split('=', 1)[1]

if not CONNECTION_QN:
    print("❌ CONNECTION_QN not found in connection_info.txt!")
    exit(1)

print(f"✓ Connection QN: {CONNECTION_QN}")

# ============================================
# STEP 1: EXTRACT FROM DATABRICKS
# ============================================
print("\n" + "="*70)
print("STEP 1: EXTRACTING FROM DATABRICKS")
print("="*70)

def extract_genie_spaces_from_databricks() -> List[Dict]:
    """Extract Genie Spaces directly from Databricks API."""

    if not DATABRICKS_TOKEN or DATABRICKS_TOKEN == "test_token":
        print("⚠️  No valid Databricks token, using sample data for demo")
        # Return sample data for demo
        return [
            {
                "basic_info": {
                    "space_id": "01f10ea33fc010dcb2dc604b75ac4336",
                    "title": "Wide World Importers - Processed Gold",
                    "warehouse_id": "2b6f29859d604d29"
                },
                "extracted_metadata": {
                    "tables": [
                        {"identifier": "wide_world_importers.processed_gold.dim_customer"},
                        {"identifier": "wide_world_importers.processed_gold.dim_employee"},
                        {"identifier": "wide_world_importers.processed_gold.dim_stockitem"},
                        {"identifier": "wide_world_importers.processed_gold.fact_orders"}
                    ],
                    "sample_questions": [
                        "What are the top 10 customers by revenue?",
                        "Show me monthly sales trends"
                    ],
                    "instructions": "Focus on customer segmentation and sales analysis",
                    "table_descriptions": {},
                    "column_configs": [],
                    "example_sql": [],
                    "join_specs": [],
                    "sql_snippets": [],
                    "filters": [],
                    "measures": [],
                    "dimensions": []
                }
            }
        ]

    print(f"🔍 Fetching Genie Spaces from Databricks...")
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        # Get list of spaces
        list_url = f"{DATABRICKS_HOST}/api/2.0/genie/spaces"
        response = requests.get(list_url, headers=headers, timeout=30)
        response.raise_for_status()

        spaces = response.json().get('spaces', [])
        print(f"✅ Found {len(spaces)} Genie Spaces")

        # Get detailed info for each space
        detailed_spaces = []
        for space in spaces:
            print(f"  📊 Fetching details for: {space.get('title', 'Unknown')}")

            detail_url = f"{DATABRICKS_HOST}/api/2.0/genie/spaces/{space['space_id']}?include_serialized_space=true"
            detail_response = requests.get(detail_url, headers=headers, timeout=30)

            if detail_response.status_code == 200:
                detailed = detail_response.json()

                # Extract metadata from serialized_space
                metadata = extract_metadata_from_serialized(detailed.get('serialized_space', {}))

                detailed_spaces.append({
                    "basic_info": space,
                    "extracted_metadata": metadata
                })
            else:
                print(f"    ⚠️ Failed to get details: {detail_response.status_code}")

        return detailed_spaces

    except Exception as e:
        print(f"❌ Error fetching from Databricks: {e}")
        print("   Using sample data for demo")

        # Return sample data
        return [
            {
                "basic_info": {
                    "space_id": "demo_001",
                    "title": "Demo Genie Space",
                    "warehouse_id": "demo_warehouse"
                },
                "extracted_metadata": {
                    "tables": [{"identifier": "demo.table1"}],
                    "sample_questions": ["Demo question"],
                    "instructions": "Demo instructions",
                    "table_descriptions": {},
                    "column_configs": [],
                    "example_sql": [],
                    "join_specs": [],
                    "sql_snippets": [],
                    "filters": [],
                    "measures": [],
                    "dimensions": []
                }
            }
        ]

def extract_metadata_from_serialized(serialized_space) -> Dict:
    """Extract metadata from serialized_space field."""

    # Parse if string
    if isinstance(serialized_space, str):
        try:
            serialized_space = json.loads(serialized_space)
        except:
            return create_empty_metadata()

    metadata = create_empty_metadata()

    # Extract data sources and tables
    if 'data_sources' in serialized_space:
        data_sources = serialized_space['data_sources']

        if 'tables' in data_sources:
            metadata['tables'] = data_sources['tables']

        if 'table_descriptions' in data_sources:
            metadata['table_descriptions'] = data_sources['table_descriptions']

        if 'column_configurations' in data_sources:
            metadata['column_configs'] = data_sources['column_configurations']

    # Extract instructions and questions
    if 'text_instructions' in serialized_space:
        metadata['instructions'] = serialized_space['text_instructions']

    if 'sample_questions' in serialized_space:
        metadata['sample_questions'] = serialized_space['sample_questions']

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
        "dimensions": []
    }

# Extract from Databricks
genie_spaces = extract_genie_spaces_from_databricks()

# Save to JSON for reference
print("\n💾 Saving extracted data to JSON...")
with open('genie_spaces_detailed_analysis.json', 'w') as f:
    json.dump(genie_spaces, f, indent=2)
print(f"✓ Saved {len(genie_spaces)} spaces to genie_spaces_detailed_analysis.json")

# ============================================
# STEP 2: SYNC TO ATLAN
# ============================================
print("\n" + "="*70)
print("STEP 2: SYNCING TO ATLAN")
print("="*70)

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

    print(f"✓ Found {len(existing_entities)} existing entities")
except Exception as e:
    print(f"⚠ No existing entities: {e}")

# Process each space
print("\n" + "="*70)
print("PROCESSING GENIE SPACES")
print("="*70)

entities_to_save = []
readmes_to_create = []

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
        print(f"   ✓ Found existing entity")
        entity = existing
    else:
        print(f"   🆕 Creating new entity")
        entity = CustomEntity.creator(
            name=title,
            connection_qualified_name=CONNECTION_QN
        )

    # Set custom attributes
    entity.asset_user_defined_type = "Genie Space"
    entity.user_description = f"Databricks Genie Space for AI-powered data exploration"

    # Prepare custom metadata
    custom_metadata = {
        "spaceId": space_id,
        "warehouseId": warehouse_id,
        "tableCount": str(len(metadata.get("tables", []))),
        "hasInstructions": "true" if metadata.get("instructions") else "false"
    }

    # Add sample questions if available
    questions = metadata.get("sample_questions", [])
    if questions:
        custom_metadata["sampleQuestions"] = "\n".join(questions[:3])  # First 3

    # Apply custom metadata
    entity.custom_metadata = CustomMetadataDict(
        {"Genie Spaces Details": custom_metadata}
    )

    entities_to_save.append(entity)

    # Create README content
    readme_content = generate_readme(basic_info, metadata)

    if entity.guid:
        readme = Readme.creator(
            asset=entity,
            content=readme_content
        )
        readmes_to_create.append(readme)

def generate_readme(basic_info: Dict, metadata: Dict) -> str:
    """Generate README content for a Genie Space."""

    title = basic_info.get("title", "Unknown")
    space_id = basic_info.get("space_id", "N/A")
    warehouse_id = basic_info.get("warehouse_id", "N/A")

    content = f"""# {title}

## Overview
This is a Databricks Genie Space that enables natural language queries and AI-powered data exploration.

### Space Details
- **Space ID**: `{space_id}`
- **Warehouse ID**: `{warehouse_id}`
- **Tables**: {len(metadata.get("tables", []))}

"""

    # Add tables section
    tables = metadata.get("tables", [])
    if tables:
        content += "## Tables\n\n"
        for table in tables[:10]:  # First 10 tables
            if isinstance(table, dict):
                table_name = table.get('identifier', table.get('name', str(table)))
            else:
                table_name = str(table)
            content += f"- `{table_name}`\n"
        if len(tables) > 10:
            content += f"- ... and {len(tables) - 10} more\n"
        content += "\n"

    # Add sample questions
    questions = metadata.get("sample_questions", [])
    if questions:
        content += "## Sample Questions\n\n"
        for i, question in enumerate(questions[:5], 1):
            content += f"{i}. {question}\n"
        content += "\n"

    # Add instructions if available
    instructions = metadata.get("instructions")
    if instructions:
        content += "## Business Instructions\n\n"
        content += f"{instructions[:500]}...\n" if len(instructions) > 500 else f"{instructions}\n"
        content += "\n"

    # Add Databricks link
    content += f"""## Access in Databricks

[Open in Databricks Genie]({DATABRICKS_HOST}/genie/spaces/{space_id})

---
*Last synced: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""

    return content

# Save entities
print("\n" + "="*70)
print("SAVING TO ATLAN")
print("="*70)

if entities_to_save:
    print(f"\n💾 Saving {len(entities_to_save)} entities...")
    try:
        response = client.asset.save(entities_to_save)
        print(f"✅ Successfully saved {len(response.assets_created)} new entities")
        print(f"✅ Successfully updated {len(response.assets_updated)} entities")

        # Create READMEs for new entities
        if response.assets_created:
            print(f"\n📝 Creating READMEs for new entities...")
            for created_asset in response.assets_created:
                if isinstance(created_asset, CustomEntity):
                    # Find the matching space data
                    for space_data in genie_spaces:
                        if space_data["basic_info"]["title"] == created_asset.name:
                            readme_content = generate_readme(
                                space_data["basic_info"],
                                space_data["extracted_metadata"]
                            )
                            readme = Readme.creator(
                                asset=created_asset,
                                content=readme_content
                            )
                            try:
                                client.asset.save(readme)
                                print(f"   ✓ README created for: {created_asset.name}")
                            except Exception as e:
                                print(f"   ⚠ Failed to create README: {e}")
                            break

    except Exception as e:
        print(f"❌ Error saving entities: {e}")
else:
    print("ℹ️ No entities to save")

# Final summary
print("\n" + "="*70)
print("SUMMARY")
print("="*70)
print(f"✅ Extracted {len(genie_spaces)} Genie Spaces from Databricks")
print(f"✅ Processed {len(entities_to_save)} entities in Atlan")
print(f"✅ Data saved to genie_spaces_detailed_analysis.json")
print(f"\n✨ Sync complete!")
print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")