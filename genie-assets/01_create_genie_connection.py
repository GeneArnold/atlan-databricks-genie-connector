#!/usr/bin/env python3
"""
Script 1: Create Databricks Genie Connection (Idempotent)
=========================================================
Creates the DATABRICKS_GENIE custom connector and connection in Atlan.
If the connection already exists, updates it (icon, description).

Author: Gene Arnold
"""

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Connection
from pyatlan.model.fluent_search import FluentSearch
from dotenv import load_dotenv
import os
import time

print("=" * 70)
print("SCRIPT 1: Create / Update Databricks Genie Connection")
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

DATABRICKS_ICON_URL = "https://assets.atlan.com/assets/databricks.svg"
CONNECTION_NAME = "Databricks Genie Spaces"

# Connect
print("\n🔌 Connecting to Atlan...")
client = AtlanClient(base_url=ATLAN_BASE_URL, api_key=ATLAN_API_KEY)
print("✅ Connected!")

# Check if connection already exists
print(f"\n🔍 Searching for existing '{CONNECTION_NAME}' connection...")
existing_connection = None

try:
    search_request = (
        FluentSearch()
        .where(Connection.CONNECTOR_NAME.eq("databricks-genie"))
        .page_size(5)
    ).to_request()

    results = client.asset.search(search_request)
    for asset in results:
        if isinstance(asset, Connection):
            existing_connection = asset
            break
except Exception as e:
    print(f"⚠ Search error: {e}")

if existing_connection:
    print(f"✓ Connection already exists!")
    print(f"   Name: {existing_connection.name}")
    print(f"   QN:   {existing_connection.qualified_name}")
    print(f"   GUID: {existing_connection.guid}")

    # Update icon and description
    print(f"\n🎨 Updating connection icon to Databricks logo...")
    conn_update = Connection.updater(
        qualified_name=existing_connection.qualified_name,
        name=existing_connection.name,
    )
    conn_update.asset_icon = DATABRICKS_ICON_URL
    conn_update.description = "Databricks Genie AI-powered analytics spaces"

    try:
        response = client.asset.save(conn_update)
        print(f"✅ Connection updated with Databricks icon!")
    except Exception as e:
        print(f"⚠ Failed to update connection: {e}")

    connection_qn = existing_connection.qualified_name
    connection_guid = existing_connection.guid

else:
    # Create new connection
    print(f"\n🆕 Connection not found, creating '{CONNECTION_NAME}'...")
    print("   NOTE: Creating a custom connector requires pyatlan support for")
    print("   AtlanConnectorType.CREATE_CUSTOM which may vary by version.")
    print("   If this fails, create the connection manually in Atlan UI.")

    try:
        from pyatlan.model.enums import AtlanConnectorType, AtlanConnectionCategory

        AtlanConnectorType.CREATE_CUSTOM(
            name="DATABRICKS_GENIE",
            value="databricks-genie",
            category=AtlanConnectionCategory.BI,
        )

        admin_role_guid = client.role_cache.get_id_for_name("$admin")

        connection = Connection.creator(
            name=CONNECTION_NAME,
            connector_type=AtlanConnectorType.DATABRICKS_GENIE,
            admin_roles=[admin_role_guid],
        )
        connection.description = "Databricks Genie AI-powered analytics spaces"
        connection.asset_icon = DATABRICKS_ICON_URL

        response = client.asset.save(connection)
        created_conn = response.assets_created(asset_type=Connection)[0]

        print(f"\n✅ Connection created!")
        print(f"   Name: {created_conn.name}")
        print(f"   QN:   {created_conn.qualified_name}")
        print(f"   GUID: {created_conn.guid}")

        connection_qn = created_conn.qualified_name
        connection_guid = created_conn.guid

        print("\n⏳ Waiting 20 seconds for permissions to propagate...")
        time.sleep(20)

    except Exception as e:
        print(f"\n❌ Failed to create connection: {e}")
        print("   The connection must be created manually or with a compatible pyatlan version.")
        exit(1)

# Save connection info
print("\n💾 Saving connection info to connection_info.txt...")
with open("connection_info.txt", "w") as f:
    f.write(f"CONNECTION_QN={connection_qn}\n")
    f.write(f"CONNECTION_GUID={connection_guid}\n")
    f.write(f"CONNECTION_NAME={CONNECTION_NAME}\n")
print("✓ Saved!")

print("\n" + "=" * 70)
print("✅ SCRIPT 1 COMPLETE!")
print("=" * 70)
print(f"\n🔗 Connection: {connection_qn}")
print(f"🎨 Icon: {DATABRICKS_ICON_URL}")
print("\n▶️  NEXT: Run 02_setup_genie_metadata.py")
