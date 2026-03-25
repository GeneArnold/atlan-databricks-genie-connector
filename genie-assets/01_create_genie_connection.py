#!/usr/bin/env python3
"""
Script 1: Create Databricks Genie Connection
RUN THIS FIRST - We need the connection before creating custom metadata
Author: Gene Arnold
"""

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Connection
from pyatlan.model.enums import AtlanConnectorType, AtlanConnectionCategory
from dotenv import load_dotenv
import os
import time

print("="*70)
print("SCRIPT 1: Creating Databricks Genie Connection")
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
print(f"✓ API Key: {ATLAN_API_KEY[:15]}...")

# Connect
print("\n🔌 Connecting to Atlan...")
client = AtlanClient(base_url=ATLAN_BASE_URL, api_key=ATLAN_API_KEY)
print("✅ Connected!")

# Define custom connector
print("\n✨ Defining custom connector type 'databricks-genie'...")
AtlanConnectorType.CREATE_CUSTOM(
    name="DATABRICKS_GENIE",
    value="databricks-genie",
    category=AtlanConnectionCategory.BI
)
print("✓ Custom connector defined")

# Get admin role
print("\n👤 Getting admin role...")
admin_role_guid = client.role_cache.get_id_for_name("$admin")
print(f"✓ Admin role: {admin_role_guid}")

# Create connection
print("\n🔗 Creating connection 'Databricks Genie Spaces'...")
connection = Connection.creator(
    client=client,
    name="Databricks Genie Spaces",
    connector_type=AtlanConnectorType.DATABRICKS_GENIE,
    admin_roles=[admin_role_guid],
)

connection.description = "Databricks Genie AI-powered analytics spaces"

response = client.asset.save(connection)
created_conn = response.assets_created(asset_type=Connection)[0]

print(f"\n✅ Connection created!")
print(f"   Name: {created_conn.name}")
print(f"   Qualified Name: {created_conn.qualified_name}")
print(f"   GUID: {created_conn.guid}")

# Save to file
print("\n💾 Saving connection info to connection_info.txt...")
with open('connection_info.txt', 'w') as f:
    f.write(f"CONNECTION_QN={created_conn.qualified_name}\n")
    f.write(f"CONNECTION_GUID={created_conn.guid}\n")
    f.write(f"CONNECTION_NAME={created_conn.name}\n")

print("✓ Saved!")

# Wait for permissions
print("\n⏳ Waiting 20 seconds for permissions to propagate...")
time.sleep(20)

print("\n" + "="*70)
print("✅ SCRIPT 1 COMPLETE!")
print("="*70)
print("\n🔗 Connection is ready!")
print(f"   QN: {created_conn.qualified_name}")
print("\n📝 NOTE: To add the sparkles icon:")
print("   1. Go to Atlan UI → Connections")
print("   2. Find 'Databricks Genie Spaces'")
print("   3. Edit and upload icon from:")
print("      https://unpkg.com/ionicons@7.1.0/dist/svg/sparkles-outline.svg")
print("\n▶️  NEXT: Run 02_setup_genie_metadata.py")