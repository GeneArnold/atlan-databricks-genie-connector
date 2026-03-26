#!/usr/bin/env python3
"""
Script 2: Create Custom Metadata Structure (Idempotent)
=======================================================
Creates the "Genie Spaces Details" custom metadata set restricted to
the Genie connection's CustomEntity assets.

If the metadata set already exists, reports current state and skips creation.

Author: Gene Arnold
"""

import os
import requests
from pyatlan.client.atlan import AtlanClient
from pyatlan.model.typedef import AttributeDef, CustomMetadataDef
from pyatlan.model.enums import AtlanCustomAttributePrimitiveType
from dotenv import load_dotenv

METADATA_SET_NAME = "Genie Spaces Details"

# The 10 fields that exist on databricks.atlan.com
EXPECTED_FIELDS = [
    ("spaceId", AtlanCustomAttributePrimitiveType.STRING),
    ("warehouseId", AtlanCustomAttributePrimitiveType.STRING),
    ("tableCount", AtlanCustomAttributePrimitiveType.INTEGER),
    ("tables", AtlanCustomAttributePrimitiveType.STRING),
    ("hasInstructions", AtlanCustomAttributePrimitiveType.BOOLEAN),
    ("category", AtlanCustomAttributePrimitiveType.STRING),
    ("createdBy", AtlanCustomAttributePrimitiveType.STRING),
    ("totalQueries", AtlanCustomAttributePrimitiveType.INTEGER),
    ("uniqueUsers", AtlanCustomAttributePrimitiveType.INTEGER),
    ("avgResponseTime", AtlanCustomAttributePrimitiveType.INTEGER),
    ("workspaceUrl", AtlanCustomAttributePrimitiveType.STRING),
    ("sampleQuestions", AtlanCustomAttributePrimitiveType.STRING),
]

print("=" * 70)
print("SCRIPT 2: Create / Verify Custom Metadata")
print("=" * 70)

# Load environment
print("\n🔧 Loading environment...")
load_dotenv()

ATLAN_BASE_URL = os.getenv("ATLAN_BASE_URL")
ATLAN_API_KEY = os.getenv("ATLAN_API_KEY")

if not ATLAN_BASE_URL or not ATLAN_API_KEY:
    print("❌ Missing ATLAN_BASE_URL or ATLAN_API_KEY in .env!")
    exit(1)

print(f"✓ Base URL: {ATLAN_BASE_URL}")

# Read connection info
print("\n📖 Reading connection info from connection_info.txt...")
if not os.path.exists("connection_info.txt"):
    print("❌ connection_info.txt not found!")
    print("   Run 01_create_genie_connection.py first")
    exit(1)

CONNECTION_QN = None
with open("connection_info.txt", "r") as f:
    for line in f:
        if "CONNECTION_QN=" in line:
            CONNECTION_QN = line.strip().split("=", 1)[1]

if not CONNECTION_QN:
    print("❌ CONNECTION_QN not found in connection_info.txt!")
    exit(1)

print(f"✓ Connection QN: {CONNECTION_QN}")

# Connect
print("\n🔌 Connecting to Atlan...")
client = AtlanClient(base_url=ATLAN_BASE_URL, api_key=ATLAN_API_KEY)
print("✅ Connected!")


# ============================================================================
# Check if custom metadata already exists
# ============================================================================
# PyAtlan's typedef.get_by_name() requires the internal hashed name, not the
# display name. We use the REST API to find it by display name first.

print(f"\n🔍 Checking for existing '{METADATA_SET_NAME}' custom metadata...")

existing_internal_name = None
try:
    headers = {
        "Authorization": f"Bearer {ATLAN_API_KEY}",
        "Content-Type": "application/json",
    }
    resp = requests.get(
        f"{ATLAN_BASE_URL}/api/meta/types/typedefs",
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    bm_defs = resp.json().get("businessMetadataDefs", [])
    for bm in bm_defs:
        if bm.get("displayName") == METADATA_SET_NAME:
            existing_internal_name = bm["name"]
            break
except Exception as e:
    print(f"⚠ REST API check failed: {e}")

if existing_internal_name:
    # Load full definition via PyAtlan
    existing_cm = client.typedef.get_by_name(existing_internal_name)

    print(f"✓ Custom metadata '{METADATA_SET_NAME}' already exists!")
    print(f"   Internal name: {existing_internal_name}")

    attr_defs = existing_cm.attribute_defs or []
    existing_fields = set()
    print(f"\n📋 Current fields ({len(attr_defs)}):")
    for attr in attr_defs:
        display = attr.display_name or attr.name or "???"
        type_name = attr.type_name or "???"
        print(f"   ✓ {display} ({type_name})")
        existing_fields.add(display)

    # Compare against expected
    expected_names = {name for name, _ in EXPECTED_FIELDS}
    missing = expected_names - existing_fields
    extra = existing_fields - expected_names

    if missing:
        print(f"\n⚠ Missing fields: {missing}")
        print(f"🆕 Adding {len(missing)} missing field(s)...")

        # Build new attribute defs for the missing fields
        new_attrs = [
            AttributeDef.create(
                display_name=field_name,
                attribute_type=field_type,
                multi_valued=False,
                applicable_connections={CONNECTION_QN},
                applicable_asset_types={"CustomEntity"},
            )
            for field_name, field_type in EXPECTED_FIELDS
            if field_name in missing
        ]

        # Append to existing attribute defs and update
        existing_cm.attribute_defs = list(attr_defs) + new_attrs
        try:
            client.typedef.update(existing_cm)
            print(f"✅ Added {len(missing)} field(s): {', '.join(missing)}")
        except Exception as e:
            print(f"❌ Error adding fields: {e}")
            import traceback
            traceback.print_exc()

    if extra:
        print(f"\nℹ Extra fields in Atlan: {extra}")
    if not missing:
        print(f"\n✅ All {len(EXPECTED_FIELDS)} expected fields are present. No changes needed.")

else:
    # Create new custom metadata
    print(f"🆕 '{METADATA_SET_NAME}' not found. Creating...")
    print(f"   Scope: ONLY connection '{CONNECTION_QN}'")
    print(f"   Scope: ONLY asset type 'CustomEntity'")

    cm_def = CustomMetadataDef.create(display_name=METADATA_SET_NAME)

    print(f"\n🔒 Creating {len(EXPECTED_FIELDS)} restricted attributes...")
    cm_def.attribute_defs = [
        AttributeDef.create(
            display_name=field_name,
            attribute_type=field_type,
            multi_valued=False,
            applicable_connections={CONNECTION_QN},
            applicable_asset_types={"CustomEntity"},
        )
        for field_name, field_type in EXPECTED_FIELDS
    ]

    METADATA_ICON_URL = "https://unpkg.com/ionicons@7.1.0/dist/svg/create-outline.svg"
    cm_def.options = CustomMetadataDef.Options.with_logo_from_url(
        url=METADATA_ICON_URL, locked=False
    )

    try:
        client.typedef.create(cm_def)
        print(f"\n✅ Custom metadata '{METADATA_SET_NAME}' created!")
        print(f"\n📋 Created {len(EXPECTED_FIELDS)} restricted properties:")
        for i, (name, ptype) in enumerate(EXPECTED_FIELDS, 1):
            print(f"   {i:2d}. ✓ {name} ({ptype.value})")

        print(f"\n🔒 SCOPE RESTRICTIONS:")
        print(f"   ✓ Connection: ONLY '{CONNECTION_QN}'")
        print(f"   ✓ Asset Type: ONLY 'CustomEntity'")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

print("\n" + "=" * 70)
print("✅ SCRIPT 2 COMPLETE!")
print("=" * 70)
print("\n▶️  NEXT: Run 03_extract_and_sync_genie_spaces.py")
