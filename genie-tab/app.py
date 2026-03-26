"""
Atlan Genie Chat Interface
Embedded chat interface for Databricks Genie spaces within Atlan
"""
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import json
import os
import hashlib
import httpx
import jwt
import requests as http_requests
import time
import logging
from typing import Dict, Any, Optional, Tuple
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Enable CORS for Atlan domains
CORS(app, origins=[
    "https://*.atlan.com",
    "https://home.atlan.com",
    "https://partner-sandbox.atlan.com",
    "http://localhost:*"
])

# Configuration from environment
ATLAN_INSTANCE_URL = os.getenv('ATLAN_INSTANCE_URL', 'https://databricks.atlan.com')
GENIE_ACCESS_POLICY_NAME = os.getenv('GENIE_ACCESS_POLICY_NAME', 'Genie Space Access')

# Databricks workspace configuration — supports multiple workspaces
# Option 1: DATABRICKS_WORKSPACES env var (JSON array of {url, token} objects)
# Option 2: Legacy single-workspace DATABRICKS_WORKSPACE_URL + DATABRICKS_TOKEN
DATABRICKS_WORKSPACES = {}  # {normalized_url: token}
_workspaces_json = os.getenv('DATABRICKS_WORKSPACES', '')
if _workspaces_json:
    try:
        for ws in json.loads(_workspaces_json):
            url = ws['url'].rstrip('/')
            DATABRICKS_WORKSPACES[url] = ws['token']
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Failed to parse DATABRICKS_WORKSPACES: {e}")

# Fallback: single workspace from legacy env vars
_legacy_url = os.getenv('DATABRICKS_WORKSPACE_URL', '')
_legacy_token = os.getenv('DATABRICKS_TOKEN', '')
if _legacy_url and _legacy_token:
    DATABRICKS_WORKSPACES.setdefault(_legacy_url.rstrip('/'), _legacy_token)

if DATABRICKS_WORKSPACES:
    logger.info(f"Configured {len(DATABRICKS_WORKSPACES)} Databricks workspace(s)")
    for url in DATABRICKS_WORKSPACES:
        logger.info(f"  - {url}")
else:
    logger.warning("No Databricks workspaces configured")

# Cache for policy-to-role mapping and user access results
_genie_persona_role_cache = None
_access_cache = {}  # {token_hash: (allowed, username, reason, timestamp)}
ACCESS_CACHE_TTL = 300  # 5 minutes


def get_bearer_token():
    """Extract Bearer token from Authorization header (passed from frontend OAuth)."""
    auth_header = request.headers.get('Authorization', '')
    if not auth_header.startswith('Bearer '):
        return None
    return auth_header.replace('Bearer ', '')

def get_genie_persona_role(token):
    """
    Search for the Genie access policy by name and derive its persona's Keycloak role.

    Searches for an AuthPolicy named GENIE_ACCESS_POLICY_NAME, extracts the parent
    persona's qualifiedName, and derives the Keycloak role (persona_{suffix}).
    Result is cached at app level since the policy rarely changes.

    Returns the role string (e.g. "persona_yZbu3EfEtFOfaZxyKqBgTO") or None.
    """
    global _genie_persona_role_cache
    if _genie_persona_role_cache:
        return _genie_persona_role_cache

    try:
        resp = http_requests.post(
            f"{ATLAN_INSTANCE_URL}/api/meta/search/indexsearch",
            headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
            json={
                "dsl": {
                    "from": 0, "size": 1,
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"__typeName.keyword": "AuthPolicy"}},
                                {"term": {"name.keyword": GENIE_ACCESS_POLICY_NAME}},
                            ]
                        }
                    },
                },
                "attributes": ["name", "accessControl"],
                "relationAttributes": ["name", "qualifiedName"],
            },
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning(f"Policy search failed: {resp.status_code}")
            return None

        entities = resp.json().get("entities", [])
        if not entities:
            logger.warning(f"No policy found named '{GENIE_ACCESS_POLICY_NAME}'")
            return None

        access_control = entities[0]["attributes"].get("accessControl", {})
        persona_qn = access_control.get("uniqueAttributes", {}).get("qualifiedName", "")
        if "/" not in persona_qn:
            logger.warning(f"Cannot derive role from persona qualifiedName: {persona_qn}")
            return None

        suffix = persona_qn.split("/")[-1]
        role_name = f"persona_{suffix}"
        _genie_persona_role_cache = role_name
        logger.info(f"Genie persona role resolved: {role_name}")
        return role_name

    except Exception as e:
        logger.error(f"Error searching for Genie access policy: {e}")
        return None


def check_genie_access(token):
    """
    Check if the user (identified by their OAuth token) has access to Genie spaces.

    Access is granted if:
      1. The user has workspaceRole "$admin" (admin / all-assets bypass), OR
      2. The user has the Keycloak role for the persona that owns the
         "Genie Space Access" policy.

    Returns: (allowed: bool, username: str, reason: str)
    """
    # Check cache first
    token_hash = hashlib.sha256(token.encode()).hexdigest()[:16]
    cached = _access_cache.get(token_hash)
    if cached:
        allowed, username, reason, ts = cached
        if time.time() - ts < ACCESS_CACHE_TTL:
            return allowed, username, reason

    # 1. Resolve the persona role for the Genie access policy
    persona_role = get_genie_persona_role(token)
    if not persona_role:
        return False, "unknown", "policy_not_found"

    # 2. Get user identity
    username, user_id = _get_user_identity(token)
    if not username:
        return False, "unknown", "identity_failed"

    # 3. Get user details and check access
    allowed, reason = _check_user_roles(token, username, user_id, persona_role)

    # Cache the result
    _access_cache[token_hash] = (allowed, username, reason, time.time())
    return allowed, username, reason


def _get_user_identity(token):
    """Get username and user ID from token. Tries API first, falls back to JWT decode."""
    try:
        resp = http_requests.get(
            f"{ATLAN_INSTANCE_URL}/api/service/users/current",
            headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("username", ""), data.get("id", "")
    except Exception as e:
        logger.warning(f"users/current API failed: {e}")

    # Fallback: decode JWT without verification
    try:
        decoded = jwt.decode(token, options={"verify_signature": False})
        username = decoded.get("preferred_username") or decoded.get("username") or ""
        user_id = decoded.get("userId") or decoded.get("sub") or ""
        return username, user_id
    except Exception as e:
        logger.error(f"JWT decode failed: {e}")
        return None, None


def _check_user_roles(token, username, user_id, persona_role):
    """Check user's workspace role and persona membership. Returns (allowed, reason)."""
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

    # Try /api/service/users endpoint for full user details
    try:
        filter_json = json.dumps({"$and": [{"username": username}]})
        resp = http_requests.get(
            f"{ATLAN_INSTANCE_URL}/api/service/users",
            headers=headers,
            params={"limit": 1, "offset": 0, "filter": filter_json, "sort": "username"},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            records = data if isinstance(data, list) else data.get("records", [])
            if records:
                user = records[0]
                if user.get("workspaceRole") == "$admin":
                    logger.info(f"Access granted for {username}: admin")
                    return True, "admin"
                if persona_role in user.get("roles", []):
                    logger.info(f"Access granted for {username}: policy match")
                    return True, "policy_match"
                logger.info(f"Access denied for {username}: missing role {persona_role}")
                return False, "no_access"
    except Exception as e:
        logger.warning(f"Users endpoint failed: {e}")

    # Fallback: check Keycloak role mappings directly
    if user_id:
        try:
            resp = http_requests.get(
                f"{ATLAN_INSTANCE_URL}/auth/admin/realms/default/users/{user_id}/role-mappings/realm",
                headers=headers,
                timeout=10,
            )
            if resp.status_code == 200:
                roles = [r.get("name", "") for r in resp.json()]
                if "$admin" in roles:
                    logger.info(f"Access granted for {username}: admin (keycloak)")
                    return True, "admin"
                if persona_role in roles:
                    logger.info(f"Access granted for {username}: policy match (keycloak)")
                    return True, "policy_match"
                return False, "no_access"
        except Exception as e:
            logger.warning(f"Keycloak roles endpoint failed: {e}")

    return False, "check_failed"


class GenieClient:
    """Simplified Genie client for chat interface"""

    def __init__(self, workspace_url: str, token: str):
        self.workspace_url = workspace_url.rstrip("/")
        self.token = token
        self.api_base = f"{self.workspace_url}/api/2.0/genie"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def start_conversation(self, space_id: str, question: str) -> Tuple[str, str]:
        """Start a new conversation"""
        url = f"{self.api_base}/spaces/{space_id}/start-conversation"
        payload = {"content": question}

        with httpx.Client(timeout=30.0) as client:
            response = client.post(url, json=payload, headers=self.headers)
            if response.status_code == 401:
                raise Exception("Authentication failed. Check your Databricks token.")
            response.raise_for_status()
            data = response.json()
            return data["conversation_id"], data["message_id"]

    def continue_conversation(self, space_id: str, conversation_id: str, question: str) -> str:
        """Continue existing conversation"""
        url = f"{self.api_base}/spaces/{space_id}/conversations/{conversation_id}/messages"
        payload = {"content": question}

        with httpx.Client(timeout=30.0) as client:
            response = client.post(url, json=payload, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            return data["message_id"]

    def get_message_status(self, space_id: str, conversation_id: str, message_id: str) -> Dict[str, Any]:
        """Get message status and results"""
        url = f"{self.api_base}/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"

        with httpx.Client(timeout=30.0) as client:
            response = client.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()

    def wait_for_response(
        self,
        space_id: str,
        conversation_id: str,
        message_id: str,
        max_wait: int = 30
    ) -> Dict[str, Any]:
        """Poll and wait for response"""
        start_time = time.time()
        poll_interval = 1.0

        while time.time() - start_time < max_wait:
            try:
                message = self.get_message_status(space_id, conversation_id, message_id)
                status = message.get("status", "UNKNOWN")

                if status == "COMPLETED":
                    # Extract response
                    result = {
                        "status": "completed",
                        "text_response": None,
                        "sql_query": None
                    }

                    attachments = message.get("attachments", [])
                    for attachment in attachments:
                        if "text" in attachment and "content" in attachment["text"]:
                            result["text_response"] = attachment["text"]["content"]
                        if attachment.get("query"):
                            result["sql_query"] = attachment["query"].get("query")

                    return result

                elif status in ["FAILED", "CANCELLED"]:
                    return {"status": "failed", "error": f"Message {status}"}

                time.sleep(min(poll_interval * 1.5, 5))

            except Exception as e:
                return {"status": "error", "error": str(e)}

        return {"status": "timeout", "error": "Response timeout"}

# Initialize Genie clients — one per workspace
genie_clients = {}  # {normalized_url: GenieClient}
for ws_url, ws_token in DATABRICKS_WORKSPACES.items():
    genie_clients[ws_url] = GenieClient(ws_url, ws_token)

def get_genie_client(workspace_url: Optional[str] = None) -> Optional[GenieClient]:
    """Get the GenieClient for a workspace URL, or the first available one as fallback."""
    if workspace_url:
        normalized = workspace_url.rstrip('/')
        if normalized in genie_clients:
            return genie_clients[normalized]
        logger.warning(f"No token configured for workspace: {normalized}")
    # Fallback to first configured workspace
    if genie_clients:
        return next(iter(genie_clients.values()))
    return None

@app.route('/')
def index():
    """Main chat interface"""
    return render_template('chat.html', atlan_instance_url=ATLAN_INSTANCE_URL)

@app.route('/api/space/<space_guid>')
def get_space_info(space_guid):
    """Get space information from Atlan asset using OAuth Bearer token via REST API."""

    # Demo mode fallback
    if space_guid == 'demo-space-guid':
        demo_ws = next(iter(genie_clients), '')
        return jsonify({
            'success': True,
            'space_id': '01f10ea33fc010dcb2dc604b75ac4336',
            'name': 'Wide World Importers Sales (Demo)',
            'description': 'Demo Genie space for testing',
            'workspace_url': demo_ws,
            'databricks_url': f"{demo_ws}/genie/spaces/01f10ea33fc010dcb2dc604b75ac4336",
            'sample_questions': []
        })

    # Extract OAuth Bearer token from frontend
    token = get_bearer_token()
    if not token:
        return jsonify({
            'success': False,
            'error': 'No authorization token provided. Please authenticate with Atlan.',
            'demo_available': True
        })

    # Policy-based access check
    if GENIE_ACCESS_POLICY_NAME:
        allowed, username, reason = check_genie_access(token)
        if not allowed:
            logger.warning(f"Access denied for {username}: {reason}")
            return jsonify({
                'success': False,
                'error': 'You do not have access to Genie spaces. '
                         'Contact your Atlan admin to request the '
                         f'"{GENIE_ACCESS_POLICY_NAME}" policy.'
            }), 403

    try:
        # Fetch asset via Atlan REST API with user's OAuth token
        api_url = f"{ATLAN_INSTANCE_URL}/api/meta/entity/guid/{space_guid}"
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

        logger.info(f"Fetching asset: {api_url}")
        response = http_requests.get(api_url, headers=headers, timeout=30)

        if response.status_code == 401:
            return jsonify({
                'success': False,
                'error': 'Atlan authentication failed. Your session may have expired.',
                'demo_available': True
            })
        elif response.status_code == 404:
            return jsonify({
                'success': False,
                'error': f'Asset not found: {space_guid}'
            })
        elif response.status_code != 200:
            return jsonify({
                'success': False,
                'error': f'Atlan API error: {response.status_code}'
            })

        # Parse the REST API response
        data = response.json()
        entity = data.get('entity', data)
        attributes = entity.get('attributes', {})
        business_attributes = entity.get('businessAttributes', {})

        logger.info(f"Asset: {attributes.get('name')} | businessAttributes keys: {list(business_attributes.keys())}")

        # Log full businessAttributes content so we can see the actual field names
        for bkey, bval in business_attributes.items():
            logger.info(f"businessAttributes['{bkey}'] = {bval}")

        # Find custom metadata containing spaceId
        # Note: Atlan uses internal hashed keys for both the metadata set AND its fields
        genie_metadata = None
        genie_key_found = None
        databricks_space_id = None

        import re
        # Databricks space IDs are 32-char hex strings like '01f10ea33fc010dcb2dc604b75ac4336'
        hex_id_pattern = re.compile(r'^[0-9a-f]{20,}$')

        for key, value in business_attributes.items():
            if isinstance(value, dict):
                # Try known field names first
                sid = value.get('spaceId') or value.get('space_id') or value.get('spaceid')
                if not sid:
                    # Field names may also be hashed — search values for hex ID pattern
                    for fkey, fval in value.items():
                        if isinstance(fval, str) and hex_id_pattern.match(fval):
                            logger.info(f"  Matched hex ID in field '{fkey}' = '{fval}'")
                            sid = fval
                            break
                if sid:
                    genie_metadata = value
                    genie_key_found = key
                    databricks_space_id = sid
                    logger.info(f"Found spaceId '{sid}' in businessAttributes key '{key}'")
                    break

        if databricks_space_id:
            # Extract workspace URL from metadata (workspaceUrl field)
            workspace_url = None
            sample_questions = []
            if genie_metadata:
                for fkey, fval in genie_metadata.items():
                    if isinstance(fval, str) and fval.startswith('https://') and '.cloud.databricks.com' in fval:
                        workspace_url = fval.rstrip('/')
                    elif isinstance(fval, str) and fval.startswith('['):
                        # Likely JSON-encoded sample questions
                        try:
                            parsed = json.loads(fval)
                            if isinstance(parsed, list) and parsed:
                                sample_questions = parsed
                        except (json.JSONDecodeError, ValueError):
                            pass

            # Fall back to first configured workspace if not in metadata
            if not workspace_url and genie_clients:
                workspace_url = next(iter(genie_clients))

            return jsonify({
                'success': True,
                'space_id': databricks_space_id,
                'name': attributes.get('name') or entity.get('displayText') or 'Genie Space',
                'description': attributes.get('userDescription') or attributes.get('description') or 'Databricks Genie space for data analysis',
                'workspace_url': workspace_url or '',
                'databricks_url': f"{workspace_url or ''}/genie/spaces/{databricks_space_id}",
                'sample_questions': sample_questions[:5]
            })
        else:
            return jsonify({
                'success': False,
                'error': 'No Databricks space ID found in Genie Spaces Details custom metadata',
                'debug': {
                    'asset_name': attributes.get('name'),
                    'business_attribute_keys': list(business_attributes.keys()),
                    'genie_key_found': genie_key_found,
                    'genie_metadata': genie_metadata
                }
            })

    except http_requests.exceptions.RequestException as e:
        logger.error(f"Request error: {e}")
        return jsonify({
            'success': False,
            'error': f'Failed to connect to Atlan API: {str(e)}'
        })
    except Exception as e:
        logger.error(f"Error fetching asset: {e}")
        return jsonify({
            'success': False,
            'error': f'Error fetching from Atlan: {str(e)}'
        })

@app.route('/api/chat', methods=['POST'])
def chat():
    """Process chat message with Genie"""
    data = request.json
    space_id = data.get('space_id')
    message = data.get('message')
    conversation_id = data.get('conversation_id')
    workspace_url = data.get('workspace_url')

    if not space_id or not message:
        return jsonify({'success': False, 'error': 'Missing space_id or message'}), 400

    client = get_genie_client(workspace_url)
    if not client:
        return jsonify({
            'success': False,
            'error': 'No Databricks workspace configured for this Genie space.'
        }), 503

    try:
        # Start or continue conversation
        if not conversation_id:
            conversation_id, message_id = client.start_conversation(space_id, message)
        else:
            message_id = client.continue_conversation(space_id, conversation_id, message)

        # Wait for response
        result = client.wait_for_response(space_id, conversation_id, message_id)

        if result["status"] == "completed":
            return jsonify({
                'success': True,
                'conversation_id': conversation_id,
                'response': result.get('text_response', 'Query processed successfully'),
                'sql': result.get('sql_query')
            })
        else:
            return jsonify({
                'success': False,
                'error': result.get('error', 'Failed to get response')
            })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/config')
def get_config():
    """Check configuration status"""
    return jsonify({
        'configured': bool(genie_clients),
        'workspace_count': len(genie_clients)
    })

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'app': 'atlan-genie-chat',
        'timestamp': datetime.now().isoformat()
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)