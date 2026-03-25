#!/usr/bin/env python3
"""
Script 2: Create Custom Metadata Structure (RESTRICTED to Genie connection)
This creates custom metadata that ONLY appears on Genie Space CustomEntity assets
Author: Gene Arnold
"""

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.typedef import AttributeDef, CustomMetadataDef
from pyatlan.model.enums import AtlanCustomAttributePrimitiveType
from dotenv import load_dotenv
import os

print("="*70)
print("SCRIPT 2: Creating Restricted Custom Metadata")
print("="*70)

# Load environment
print("\n🔧 Loading environment...")
load_dotenv()

ATLAN_BASE_URL = os.getenv("ATLAN_BASE_URL")
ATLAN_API_KEY = os.getenv("ATLAN_API_KEY")

if not ATLAN_BASE_URL or not ATLAN_API_KEY:
    print("❌ Missing .env variables!")
    exit(1)

print(f"✓ Base URL: {ATLAN_BASE_URL}")

# Read connection info
print("\n📖 Reading connection info from connection_info.txt...")
if not os.path.exists('connection_info.txt'):
    print("❌ connection_info.txt not found!")
    print("   You must run 01_create_genie_connection.py first")
    exit(1)

CONNECTION_QN = None
with open('connection_info.txt', 'r') as f:
    for line in f:
        if 'CONNECTION_QN=' in line:
            CONNECTION_QN = line.strip().split('=', 1)[1]

print(f"✓ Connection QN: {CONNECTION_QN}")

# Connect
print("\n🔌 Connecting to Atlan...")
client = AtlanClient(base_url=ATLAN_BASE_URL, api_key=ATLAN_API_KEY)
print("✅ Connected!")

# Create custom metadata structure
print("\n📋 Creating RESTRICTED custom metadata: 'Genie Spaces Details'...")
print(f"   Scope: ONLY connection '{CONNECTION_QN}'")
print(f"   Scope: ONLY asset type 'CustomEntity'")

cm_def = CustomMetadataDef.create(display_name="Genie Spaces Details")

print("\n🔒 Creating 11 restricted attributes...")

# In Python SDK, pass restrictions directly to create() method
# Use sets for applicable_connections and applicable_asset_types
cm_def.attribute_defs = [
    AttributeDef.create(
        client=client,
        display_name="spaceId",
        attribute_type=AtlanCustomAttributePrimitiveType.STRING,
        multi_valued=False,
        applicable_connections={CONNECTION_QN},
        applicable_asset_types={"CustomEntity"}
    ),
    AttributeDef.create(
        client=client,
        display_name="warehouseId",
        attribute_type=AtlanCustomAttributePrimitiveType.STRING,
        multi_valued=False,
        applicable_connections={CONNECTION_QN},
        applicable_asset_types={"CustomEntity"}
    ),
    AttributeDef.create(
        client=client,
        display_name="tableCount",
        attribute_type=AtlanCustomAttributePrimitiveType.INTEGER,
        multi_valued=False,
        applicable_connections={CONNECTION_QN},
        applicable_asset_types={"CustomEntity"}
    ),
    AttributeDef.create(
        client=client,
        display_name="tables",
        attribute_type=AtlanCustomAttributePrimitiveType.STRING,
        multi_valued=False,
        applicable_connections={CONNECTION_QN},
        applicable_asset_types={"CustomEntity"}
    ),
    AttributeDef.create(
        client=client,
        display_name="hasInstructions",
        attribute_type=AtlanCustomAttributePrimitiveType.BOOLEAN,
        multi_valued=False,
        applicable_connections={CONNECTION_QN},
        applicable_asset_types={"CustomEntity"}
    ),
    AttributeDef.create(
        client=client,
        display_name="category",
        attribute_type=AtlanCustomAttributePrimitiveType.STRING,
        multi_valued=False,
        applicable_connections={CONNECTION_QN},
        applicable_asset_types={"CustomEntity"}
    ),
    AttributeDef.create(
        client=client,
        display_name="createdBy",
        attribute_type=AtlanCustomAttributePrimitiveType.STRING,
        multi_valued=False,
        applicable_connections={CONNECTION_QN},
        applicable_asset_types={"CustomEntity"}
    ),
    AttributeDef.create(
        client=client,
        display_name="totalQueries",
        attribute_type=AtlanCustomAttributePrimitiveType.INTEGER,
        multi_valued=False,
        applicable_connections={CONNECTION_QN},
        applicable_asset_types={"CustomEntity"}
    ),
    AttributeDef.create(
        client=client,
        display_name="uniqueUsers",
        attribute_type=AtlanCustomAttributePrimitiveType.INTEGER,
        multi_valued=False,
        applicable_connections={CONNECTION_QN},
        applicable_asset_types={"CustomEntity"}
    ),
    AttributeDef.create(
        client=client,
        display_name="lastAccessed",
        attribute_type=AtlanCustomAttributePrimitiveType.DATE,
        multi_valued=False,
        applicable_connections={CONNECTION_QN},
        applicable_asset_types={"CustomEntity"}
    ),
    AttributeDef.create(
        client=client,
        display_name="avgResponseTime",
        attribute_type=AtlanCustomAttributePrimitiveType.INTEGER,
        multi_valued=False,
        applicable_connections={CONNECTION_QN},
        applicable_asset_types={"CustomEntity"}
    ),
]

# Use your notebook/pencil icon
METADATA_ICON_URL = "https://unpkg.com/ionicons@7.1.0/dist/svg/create-outline.svg"

cm_def.options = CustomMetadataDef.Options.with_logo_from_url(
    url=METADATA_ICON_URL,
    locked=False
)

# Create it
print("\n💾 Creating custom metadata structure in Atlan...")
try:
    response = client.typedef.create(cm_def)
    print("\n✅ SUCCESS! Custom metadata structure created!")
    print("\n📋 Created 11 restricted properties:")
    print("  1. ✓ spaceId (Text)")
    print("  2. ✓ warehouseId (Text)")
    print("  3. ✓ tableCount (Integer)")
    print("  4. ✓ tables (Text - comma-separated list)")
    print("  5. ✓ hasInstructions (Boolean)")
    print("  6. ✓ category (Text)")
    print("  7. ✓ createdBy (Text)")
    print("  8. ✓ totalQueries (Integer)")
    print("  9. ✓ uniqueUsers (Integer)")
    print("  10. ✓ lastAccessed (Date)")
    print("  11. ✓ avgResponseTime (Integer)")

    print("\n🔒 SCOPE RESTRICTIONS:")
    print(f"   ✓ Connection: ONLY '{CONNECTION_QN}'")
    print(f"   ✓ Asset Type: ONLY 'CustomEntity'")
    print("\n✨ This will NOT appear on Snowflake, Postgres, Tableau, etc!")

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print("\n" + "="*70)
print("✅ SCRIPT 2 COMPLETE!")
print("="*70)
print("\n🎉 Custom metadata is properly scoped!")
print("📝 Icon: Notebook/pencil")
print("\n▶️  NEXT: Run 03_sync_genie_spaces.py")