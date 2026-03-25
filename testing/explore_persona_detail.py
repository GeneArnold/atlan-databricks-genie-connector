"""
Focused exploration of the "Databricks Genie" persona and its policies.
Dumps full JSON without truncation so we can see all fields.
"""

import json
from pathlib import Path
import requests

ATLAN_BASE_URL = "https://databricks.atlan.com"
TOKEN = (Path(__file__).parent / ".atlan-api-token").read_text().strip()
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

DATABRICKS_GENIE_GUID = "bedd6813-742e-4dcb-adfa-8987ee4120ee"
GENIE_SPACE_ACCESS_POLICY_GUID = "b98e76e8-29ae-4cc4-afc3-14d61e2fd795"


def search(body):
    resp = requests.post(
        f"{ATLAN_BASE_URL}/api/meta/search/indexsearch",
        headers=HEADERS,
        json=body,
    )
    resp.raise_for_status()
    return resp.json()


def dump(label, data):
    print(f"\n{'='*70}")
    print(f"  {label}")
    print(f"{'='*70}")
    print(json.dumps(data, indent=2, default=str))


def main():
    # ── 1. Full persona details ───────────────────────────────────────
    persona = search({
        "dsl": {
            "from": 0, "size": 1,
            "query": {"term": {"__guid": DATABRICKS_GENIE_GUID}},
        },
        "attributes": [
            "name", "displayName", "description",
            "personaUsers", "personaGroups",
            "policies",
            "isAccessControlEnabled",
            "denyCustomMetadataGuids", "denyAssetTabs",
            "denyAssetFilters", "denyAssetTypes",
            "createdBy", "updatedBy", "createTime", "updateTime",
            "qualifiedName", "readme", "channelLink",
        ],
        "relationAttributes": [
            "name", "displayName",
            "policyType", "policyCategory", "policySubCategory",
            "policyActions", "policyResources", "policyResourceCategory",
            "policyUsers", "policyGroups", "policyRoles",
            "policyCondition", "connectionName",
        ],
    })
    entities = persona.get("entities", [])
    if entities:
        dump("DATABRICKS GENIE PERSONA (full)", entities[0])

    # ── 2. Full policy details ────────────────────────────────────────
    policy = search({
        "dsl": {
            "from": 0, "size": 1,
            "query": {"term": {"__guid": GENIE_SPACE_ACCESS_POLICY_GUID}},
        },
        "attributes": [
            "name", "displayName", "description",
            "policyType", "policyCategory", "policySubCategory",
            "policyActions", "policyResources", "policyResourceCategory",
            "policyUsers", "policyGroups", "policyRoles",
            "policyCondition", "connectionName",
            "accessControl",
            "createdBy", "updatedBy", "createTime", "updateTime",
            "qualifiedName",
        ],
        "relationAttributes": ["name", "displayName"],
    })
    entities = policy.get("entities", [])
    if entities:
        dump("GENIE SPACE ACCESS POLICY (full)", entities[0])

    # ── 3. Check: can we look up a user's personas? ───────────────────
    # Try getting all personas and checking personaUsers/personaGroups
    all_personas = search({
        "dsl": {
            "from": 0, "size": 50,
            "query": {"term": {"__typeName.keyword": "Persona"}},
        },
        "attributes": ["name", "personaUsers", "personaGroups", "isAccessControlEnabled"],
    })
    print(f"\n{'='*70}")
    print("  ALL PERSONAS — users & groups summary")
    print(f"{'='*70}")
    for e in all_personas.get("entities", []):
        attrs = e.get("attributes", {})
        print(f"\n  Persona: {attrs.get('name')}")
        print(f"    enabled: {attrs.get('isAccessControlEnabled')}")
        print(f"    users:  {attrs.get('personaUsers', [])}")
        print(f"    groups: {attrs.get('personaGroups', [])}")

    # ── 4. Check: what does the user endpoint return for a real user? ─
    # List some users to see format
    print(f"\n{'='*70}")
    print("  ATLAN USERS (first 5)")
    print(f"{'='*70}")
    resp = requests.get(
        f"{ATLAN_BASE_URL}/api/service/users",
        headers=HEADERS,
        params={"limit": 5, "offset": 0, "sort": "username"},
    )
    if resp.status_code == 200:
        data = resp.json()
        # Show just username + id for each
        records = data if isinstance(data, list) else data.get("records", data.get("users", [data]))
        for u in (records[:5] if isinstance(records, list) else [records]):
            print(json.dumps(u, indent=2, default=str)[:500])
    else:
        print(f"  status: {resp.status_code}")
        print(f"  error: {resp.text[:500]}")

    # ── 5. Try /api/service/groups ────────────────────────────────────
    print(f"\n{'='*70}")
    print("  ATLAN GROUPS (first 5)")
    print(f"{'='*70}")
    resp = requests.get(
        f"{ATLAN_BASE_URL}/api/service/groups",
        headers=HEADERS,
        params={"limit": 5, "offset": 0},
    )
    if resp.status_code == 200:
        print(json.dumps(resp.json(), indent=2, default=str)[:2000])
    else:
        print(f"  status: {resp.status_code}")
        print(f"  error: {resp.text[:500]}")


if __name__ == "__main__":
    main()
