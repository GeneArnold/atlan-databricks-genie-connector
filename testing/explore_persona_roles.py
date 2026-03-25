"""
Explore how persona membership works in Atlan.

Key finding so far: the "Databricks Genie" persona has:
  - personaUsers: []  (empty)
  - personaGroups: []  (empty)
  - policy has policyRoles: ["persona_yZbu3EfEtFOfaZxyKqBgTO"]

This script explores:
  1. How users get assigned to personas (users vs groups vs roles)
  2. What a user's role list looks like
  3. Whether we can check persona membership for an OAuth-authenticated user
"""

import json
from pathlib import Path
import requests

ATLAN_BASE_URL = "https://databricks.atlan.com"
TOKEN = (Path(__file__).parent / ".atlan-api-token").read_text().strip()
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

PERSONA_QUALIFIED_NAME = "default/yZbu3EfEtFOfaZxyKqBgTO"
PERSONA_ROLE = "persona_yZbu3EfEtFOfaZxyKqBgTO"

def dump(label, data):
    print(f"\n{'='*70}")
    print(f"  {label}")
    print(f"{'='*70}")
    if isinstance(data, (dict, list)):
        print(json.dumps(data, indent=2, default=str))
    else:
        print(data)


def main():
    # ── 1. List all users with their roles ────────────────────────────
    print("Fetching users...")
    resp = requests.get(
        f"{ATLAN_BASE_URL}/api/service/users",
        headers=HEADERS,
        params={"limit": 20, "offset": 0, "sort": "username"},
    )
    if resp.status_code == 200:
        users = resp.json()
        records = users if isinstance(users, list) else users.get("records", [])
        dump("USERS SUMMARY", None)
        for u in records:
            username = u.get("username", "?")
            email = u.get("email", "?")
            uid = u.get("id", "?")
            roles = u.get("defaultRoles", [])
            assigned = u.get("assignedRole", {})
            personas = [r for r in roles if r.startswith("persona_")]
            print(f"\n  {username} ({email})")
            print(f"    id: {uid}")
            print(f"    assignedRole: {assigned.get('name', '?')}")
            print(f"    defaultRoles: {roles}")
            if personas:
                print(f"    ** PERSONA ROLES: {personas}")
    else:
        print(f"  Users endpoint failed: {resp.status_code} {resp.text[:300]}")

    # ── 2. Check current user's roles (API token) ────────────────────
    resp = requests.get(f"{ATLAN_BASE_URL}/api/service/users/current", headers=HEADERS)
    if resp.status_code == 200:
        dump("CURRENT USER (API token)", resp.json())

    # ── 3. Try Keycloak role-mapping endpoint ─────────────────────────
    # The persona role "persona_yZbu3EfEtFOfaZxyKqBgTO" might be a
    # Keycloak realm role. Try to list role members.
    print(f"\n{'='*70}")
    print(f"  KEYCLOAK ROLE EXPLORATION")
    print(f"{'='*70}")

    # Try listing realm roles
    for path in [
        "/api/service/roles",
        "/auth/admin/realms/default/roles",
    ]:
        resp = requests.get(f"{ATLAN_BASE_URL}{path}", headers=HEADERS)
        print(f"\n  GET {path}: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                # Filter for persona roles
                persona_roles = [r for r in data if "persona" in str(r.get("name", "")).lower()]
                print(f"    Found {len(data)} total roles, {len(persona_roles)} persona roles:")
                for r in persona_roles:
                    print(f"      {r.get('name')}: {r.get('description', '')}")
            else:
                print(f"    {json.dumps(data, indent=2, default=str)[:1000]}")
        else:
            print(f"    {resp.text[:300]}")

    # ── 4. Try to get members of the persona role ─────────────────────
    print(f"\n{'='*70}")
    print(f"  PERSONA ROLE MEMBERS")
    print(f"{'='*70}")

    for path in [
        f"/auth/admin/realms/default/roles/{PERSONA_ROLE}/users",
        f"/api/service/roles/{PERSONA_ROLE}/users",
    ]:
        resp = requests.get(f"{ATLAN_BASE_URL}{path}", headers=HEADERS)
        print(f"\n  GET {path}: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            print(f"    {json.dumps(data, indent=2, default=str)[:1000]}")
        else:
            print(f"    {resp.text[:300]}")

    # ── 5. For a specific user (gene.arnold), check their roles ───────
    print(f"\n{'='*70}")
    print(f"  GENE.ARNOLD USER DETAILS")
    print(f"{'='*70}")

    resp = requests.get(
        f"{ATLAN_BASE_URL}/api/service/users",
        headers=HEADERS,
        params={"limit": 1, "filter": '{"username":"gene.arnold"}'},
    )
    if resp.status_code == 200:
        data = resp.json()
        records = data if isinstance(data, list) else data.get("records", [])
        for u in records:
            dump(f"User: {u.get('username')}", u)
            uid = u.get("id")
            # Try to get role mappings for this user
            if uid:
                for rpath in [
                    f"/auth/admin/realms/default/users/{uid}/role-mappings/realm",
                    f"/api/service/users/{uid}/roles",
                ]:
                    resp2 = requests.get(f"{ATLAN_BASE_URL}{rpath}", headers=HEADERS)
                    print(f"\n  GET {rpath}: {resp2.status_code}")
                    if resp2.status_code == 200:
                        rdata = resp2.json()
                        if isinstance(rdata, list):
                            persona_roles = [r for r in rdata if "persona" in str(r.get("name", ""))]
                            print(f"    Total roles: {len(rdata)}, persona roles: {len(persona_roles)}")
                            for r in persona_roles:
                                print(f"      {r.get('name')}")
                        else:
                            print(f"    {json.dumps(rdata, indent=2, default=str)[:500]}")
                    else:
                        print(f"    {resp2.text[:300]}")
    else:
        print(f"  Failed: {resp.status_code} {resp.text[:300]}")


if __name__ == "__main__":
    main()
