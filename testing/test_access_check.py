"""
Test the full access check flow that will be implemented in genie-tab.

This script validates:
  1. Policy search — find "Genie Space Access" by name
  2. Persona role derivation — extract persona qualifiedName → Keycloak role
  3. User lookup — get user details including workspaceRole and roles
  4. Admin bypass — $admin users get automatic access
  5. Role matching — check if user's roles contain the persona role
  6. JWT decode fallback — extract username from token without API call
"""

import hashlib
import json
import time
from pathlib import Path

import jwt
import requests

ATLAN_BASE_URL = "https://databricks.atlan.com"
TOKEN = (Path(__file__).parent / ".atlan-api-token").read_text().strip()
GENIE_ACCESS_POLICY_NAME = "Genie Space Access"


def make_headers(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# ─── Step 1: Policy search ──────────────────────────────────────────

def get_genie_persona_role(token):
    """
    Search for the access policy by name, extract its parent persona,
    and derive the Keycloak role name.

    Returns the role string like "persona_yZbu3EfEtFOfaZxyKqBgTO" or None.
    """
    print("\n=== STEP 1: Policy Search ===")
    resp = requests.post(
        f"{ATLAN_BASE_URL}/api/meta/search/indexsearch",
        headers=make_headers(token),
        json={
            "dsl": {
                "from": 0,
                "size": 1,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"__typeName.keyword": "AuthPolicy"}},
                            {"term": {"name.keyword": GENIE_ACCESS_POLICY_NAME}},
                        ]
                    }
                },
            },
            "attributes": ["name", "policyType", "policyCategory", "accessControl"],
            "relationAttributes": ["name", "qualifiedName"],
        },
    )

    if resp.status_code != 200:
        print(f"  FAIL: indexsearch returned {resp.status_code}: {resp.text[:300]}")
        return None

    entities = resp.json().get("entities", [])
    if not entities:
        print(f"  FAIL: No policy found with name '{GENIE_ACCESS_POLICY_NAME}'")
        return None

    policy = entities[0]
    print(f"  Found policy: {policy['attributes']['name']}")
    print(f"  Policy GUID: {policy['guid']}")

    # Extract parent persona from the accessControl relation
    access_control = policy["attributes"].get("accessControl", {})
    if not access_control:
        print("  FAIL: Policy has no accessControl (parent persona) relation")
        return None

    persona_name = access_control.get("attributes", {}).get("name", "?")
    persona_qn = access_control.get("uniqueAttributes", {}).get("qualifiedName", "")
    print(f"  Parent persona: {persona_name}")
    print(f"  Persona qualifiedName: {persona_qn}")

    # Derive Keycloak role: "persona_{suffix}" where suffix is the last part of qualifiedName
    # qualifiedName format: "default/yZbu3EfEtFOfaZxyKqBgTO"
    if "/" in persona_qn:
        suffix = persona_qn.split("/")[-1]
        role_name = f"persona_{suffix}"
        print(f"  Derived Keycloak role: {role_name}")
        return role_name
    else:
        print(f"  FAIL: Can't derive role from qualifiedName: {persona_qn}")
        return None


# ─── Step 2: Get user identity ───────────────────────────────────────

def get_username_from_api(token):
    """Get username via /api/service/users/current."""
    print("\n=== STEP 2a: Get User Identity (API) ===")
    resp = requests.get(
        f"{ATLAN_BASE_URL}/api/service/users/current",
        headers=make_headers(token),
    )
    if resp.status_code == 200:
        data = resp.json()
        username = data.get("username", "")
        user_id = data.get("id", "")
        print(f"  Username: {username}")
        print(f"  User ID: {user_id}")
        return username, user_id
    else:
        print(f"  FAIL: {resp.status_code} — {resp.text[:200]}")
        return None, None


def get_username_from_jwt(token):
    """Fallback: decode JWT without verification to extract username."""
    print("\n=== STEP 2b: Get User Identity (JWT Decode Fallback) ===")
    try:
        decoded = jwt.decode(token, options={"verify_signature": False})
        username = decoded.get("preferred_username") or decoded.get("username") or ""
        user_id = decoded.get("userId") or decoded.get("sub") or ""
        print(f"  Username (from JWT): {username}")
        print(f"  User ID (from JWT): {user_id}")
        return username, user_id
    except Exception as e:
        print(f"  FAIL: JWT decode error: {e}")
        return None, None


# ─── Step 3: Get full user details ───────────────────────────────────

def get_user_details(token, username):
    """Get full user record including workspaceRole and roles."""
    print(f"\n=== STEP 3: Get User Details for '{username}' ===")

    # Try the users list endpoint with filter
    filter_json = json.dumps({"$and": [{"username": username}]})
    resp = requests.get(
        f"{ATLAN_BASE_URL}/api/service/users",
        headers=make_headers(token),
        params={"limit": 1, "offset": 0, "filter": filter_json, "sort": "username"},
    )

    if resp.status_code == 200:
        data = resp.json()
        records = data if isinstance(data, list) else data.get("records", [])
        if records:
            user = records[0]
            workspace_role = user.get("workspaceRole", "")
            roles = user.get("roles", [])
            personas = user.get("personas", [])
            print(f"  workspaceRole: {workspace_role}")
            print(f"  roles: {roles}")
            print(f"  personas: {personas}")
            return {
                "username": user.get("username"),
                "workspaceRole": workspace_role,
                "roles": roles,
                "personas": personas,
            }
        else:
            print(f"  FAIL: No user found matching '{username}'")
            return None
    else:
        print(f"  FAIL: {resp.status_code} — {resp.text[:200]}")
        return None


def get_user_roles_keycloak(token, user_id):
    """Fallback: get roles via Keycloak admin endpoint."""
    print(f"\n=== STEP 3b: Get Roles via Keycloak (fallback) for user {user_id} ===")
    resp = requests.get(
        f"{ATLAN_BASE_URL}/auth/admin/realms/default/users/{user_id}/role-mappings/realm",
        headers=make_headers(token),
    )
    if resp.status_code == 200:
        roles = resp.json()
        role_names = [r.get("name", "") for r in roles]
        print(f"  Keycloak roles: {role_names}")
        return role_names
    else:
        print(f"  FAIL: {resp.status_code} — {resp.text[:200]}")
        return []


# ─── Step 4: Full access check ───────────────────────────────────────

def check_genie_access(token):
    """
    Full access check — returns (allowed, username, reason).

    This is the function that will be ported to genie-tab/app.py.
    """
    print("\n" + "=" * 70)
    print("  FULL ACCESS CHECK")
    print("=" * 70)

    # 1. Find the persona role for the Genie access policy
    persona_role = get_genie_persona_role(token)
    if not persona_role:
        return False, "unknown", "policy_not_found"

    # 2. Get user identity
    username, user_id = get_username_from_api(token)
    if not username:
        print("  API endpoint failed, trying JWT fallback...")
        username, user_id = get_username_from_jwt(token)
    if not username:
        return False, "unknown", "identity_failed"

    # 3. Get full user details
    user_details = get_user_details(token, username)

    if user_details:
        # 4a. Admin bypass
        if user_details["workspaceRole"] == "$admin":
            print(f"\n  ✅ ALLOWED — {username} is an admin ($admin)")
            return True, username, "admin"

        # 4b. Check persona role
        if persona_role in user_details["roles"]:
            print(f"\n  ✅ ALLOWED — {username} has role {persona_role}")
            return True, username, "policy_match"

        print(f"\n  ❌ DENIED — {username} does not have role {persona_role}")
        return False, username, "no_access"

    else:
        # Fallback to Keycloak role check
        print("  User details endpoint failed, trying Keycloak fallback...")
        role_names = get_user_roles_keycloak(token, user_id)
        if persona_role in role_names:
            print(f"\n  ✅ ALLOWED — {username} has role {persona_role} (via Keycloak)")
            return True, username, "policy_match_keycloak"

        print(f"\n  ❌ DENIED — {username} does not have role {persona_role}")
        return False, username, "no_access"


# ─── Run it ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"Target: {ATLAN_BASE_URL}")
    print(f"Policy: {GENIE_ACCESS_POLICY_NAME}")

    allowed, username, reason = check_genie_access(TOKEN)

    print("\n" + "=" * 70)
    print("  RESULT")
    print("=" * 70)
    print(f"  User:    {username}")
    print(f"  Allowed: {allowed}")
    print(f"  Reason:  {reason}")
