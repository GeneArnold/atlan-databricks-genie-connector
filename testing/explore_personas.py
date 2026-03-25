"""
Explore Atlan REST API for persona and policy information.

Goal: Understand how to programmatically discover:
  1. What personas exist (specifically "Databricks Genie")
  2. What policies are attached to a persona (specifically "Genie Space Access")
  3. What users/groups are associated with a persona
  4. How to check if a given user belongs to a persona

This will inform the persona-based access gating for genie-tab.
"""

import json
import os
import sys
from pathlib import Path

import requests

ATLAN_BASE_URL = "https://databricks.atlan.com"
TOKEN_FILE = Path(__file__).parent / ".atlan-api-token"

def load_token():
    token = TOKEN_FILE.read_text().strip()
    return token

def headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

def api_get(token, path, params=None):
    url = f"{ATLAN_BASE_URL}{path}"
    print(f"\n{'='*70}")
    print(f"GET {url}")
    if params:
        print(f"  params: {params}")
    resp = requests.get(url, headers=headers(token), params=params)
    print(f"  status: {resp.status_code}")
    if resp.status_code == 200:
        try:
            data = resp.json()
            print(json.dumps(data, indent=2, default=str)[:3000])
            if len(json.dumps(data, default=str)) > 3000:
                print("\n  ... (truncated)")
            return data
        except Exception:
            print(f"  body (text): {resp.text[:1000]}")
            return resp.text
    else:
        print(f"  error: {resp.text[:1000]}")
        return None

def api_post(token, path, body=None):
    url = f"{ATLAN_BASE_URL}{path}"
    print(f"\n{'='*70}")
    print(f"POST {url}")
    if body:
        print(f"  body: {json.dumps(body, indent=2)[:500]}")
    resp = requests.post(url, headers=headers(token), json=body)
    print(f"  status: {resp.status_code}")
    if resp.status_code in (200, 201):
        try:
            data = resp.json()
            print(json.dumps(data, indent=2, default=str)[:3000])
            if len(json.dumps(data, default=str)) > 3000:
                print("\n  ... (truncated)")
            return data
        except Exception:
            print(f"  body (text): {resp.text[:1000]}")
            return resp.text
    else:
        print(f"  error: {resp.text[:1000]}")
        return None


def main():
    token = load_token()
    print("Loaded API token")
    print(f"Target: {ATLAN_BASE_URL}")

    # ── 1. List all personas ──────────────────────────────────────────
    print("\n\n" + "#"*70)
    print("# 1. LIST ALL PERSONAS")
    print("#"*70)

    # Try the known endpoints
    personas_data = None
    for path in [
        "/api/service/personas",
        "/api/service/personas?limit=50",
    ]:
        result = api_get(token, path)
        if result is not None:
            personas_data = result
            break

    # ── 2. Search for persona by name via Atlas search ────────────────
    print("\n\n" + "#"*70)
    print("# 2. SEARCH FOR 'Databricks Genie' PERSONA VIA INDEX SEARCH")
    print("#"*70)

    search_body = {
        "dsl": {
            "from": 0,
            "size": 10,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"__typeName.keyword": "Persona"}},
                    ]
                }
            }
        },
        "attributes": [
            "name",
            "displayName",
            "description",
            "personaUsers",
            "personaGroups",
            "policies",
            "isAccessControlEnabled",
            "channelLink",
            "readme",
        ],
        "relationAttributes": ["name", "displayName", "policyType", "policyCategory"],
    }
    search_result = api_post(token, "/api/meta/search/indexsearch", search_body)

    # ── 3. If we found personas, get details on each ──────────────────
    print("\n\n" + "#"*70)
    print("# 3. GET PERSONA DETAILS")
    print("#"*70)

    persona_guids = []
    if search_result and "entities" in search_result:
        for entity in search_result["entities"]:
            guid = entity.get("guid")
            name = entity.get("attributes", {}).get("name", "unknown")
            print(f"\n  Found persona: {name} (guid={guid})")
            persona_guids.append((guid, name))

    # For each persona, try to get full details
    for guid, name in persona_guids:
        print(f"\n--- Details for persona: {name} ---")
        api_get(token, f"/api/service/personas/{guid}")

    # ── 4. Search for policies ────────────────────────────────────────
    print("\n\n" + "#"*70)
    print("# 4. SEARCH FOR POLICIES (AuthPolicy type)")
    print("#"*70)

    policy_search = {
        "dsl": {
            "from": 0,
            "size": 20,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"__typeName.keyword": "AuthPolicy"}},
                    ]
                }
            }
        },
        "attributes": [
            "name",
            "displayName",
            "description",
            "policyType",
            "policyCategory",
            "policySubCategory",
            "policyActions",
            "policyResources",
            "policyResourceCategory",
            "policyUsers",
            "policyGroups",
            "policyRoles",
            "accessControl",
            "connectionName",
            "policyCondition",
        ],
        "relationAttributes": ["name", "displayName"],
    }
    api_post(token, "/api/meta/search/indexsearch", policy_search)

    # ── 5. Try purpose/access-control endpoints ───────────────────────
    print("\n\n" + "#"*70)
    print("# 5. OTHER ACCESS CONTROL ENDPOINTS")
    print("#"*70)

    for path in [
        "/api/service/accesscontrol",
        "/api/service/purposes",
    ]:
        api_get(token, path)

    # ── 6. Get current user info ──────────────────────────────────────
    print("\n\n" + "#"*70)
    print("# 6. CURRENT USER / SESSION INFO")
    print("#"*70)

    for path in [
        "/api/meta/user/current",
        "/api/service/users/current",
    ]:
        api_get(token, path)

    # ── 7. Check if we can list persona members ───────────────────────
    print("\n\n" + "#"*70)
    print("# 7. PERSONA MEMBERS / USERS")
    print("#"*70)

    if persona_guids:
        for guid, name in persona_guids:
            print(f"\n--- Members of persona: {name} ---")
            # Try getting persona with expanded relationships
            detail_search = {
                "dsl": {
                    "from": 0,
                    "size": 1,
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"__state": "ACTIVE"}},
                                {"term": {"__guid": guid}},
                            ]
                        }
                    }
                },
                "attributes": [
                    "name",
                    "displayName",
                    "personaUsers",
                    "personaGroups",
                    "policies",
                    "description",
                    "isAccessControlEnabled",
                    "denyCustomMetadataGuids",
                    "denyAssetTabs",
                    "denyAssetFilters",
                    "denyAssetTypes",
                    "personaMetadataPolicy",
                    "personaDataPolicy",
                    "personaGlossaryPolicy",
                ],
                "relationAttributes": [
                    "name",
                    "displayName",
                    "policyType",
                    "policyCategory",
                    "policyActions",
                    "policyResources",
                    "policyResourceCategory",
                    "policyUsers",
                    "policyGroups",
                ],
            }
            api_post(token, "/api/meta/search/indexsearch", detail_search)

    print("\n\n" + "="*70)
    print("EXPLORATION COMPLETE")
    print("="*70)


if __name__ == "__main__":
    main()
