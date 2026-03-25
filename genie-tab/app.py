"""
Atlan Genie Chat Interface
Embedded chat interface for Databricks Genie spaces within Atlan
"""
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import os
import httpx
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
DATABRICKS_WORKSPACE_URL = os.getenv('DATABRICKS_WORKSPACE_URL', '')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN', '')
ATLAN_INSTANCE_URL = os.getenv('ATLAN_INSTANCE_URL', 'https://databricks.atlan.com')


def get_bearer_token():
    """Extract Bearer token from Authorization header (passed from frontend OAuth)."""
    auth_header = request.headers.get('Authorization', '')
    if not auth_header.startswith('Bearer '):
        return None
    return auth_header.replace('Bearer ', '')

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

# Initialize Genie client
genie_client = None
if DATABRICKS_WORKSPACE_URL and DATABRICKS_TOKEN:
    genie_client = GenieClient(DATABRICKS_WORKSPACE_URL, DATABRICKS_TOKEN)

@app.route('/')
def index():
    """Main chat interface"""
    return render_template('chat.html', atlan_instance_url=ATLAN_INSTANCE_URL)

@app.route('/api/space/<space_guid>')
def get_space_info(space_guid):
    """Get space information from Atlan asset using OAuth Bearer token via REST API."""

    # Demo mode fallback
    if space_guid == 'demo-space-guid':
        return jsonify({
            'success': True,
            'space_id': '01f10ea33fc010dcb2dc604b75ac4336',
            'name': 'Wide World Importers Sales (Demo)',
            'description': 'Demo Genie space for testing',
            'databricks_url': f"{DATABRICKS_WORKSPACE_URL}/genie/spaces/01f10ea33fc010dcb2dc604b75ac4336"
        })

    # Extract OAuth Bearer token from frontend
    token = get_bearer_token()
    if not token:
        return jsonify({
            'success': False,
            'error': 'No authorization token provided. Please authenticate with Atlan.',
            'demo_available': True
        })

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
            return jsonify({
                'success': True,
                'space_id': databricks_space_id,
                'name': attributes.get('name') or entity.get('displayText') or 'Genie Space',
                'description': attributes.get('userDescription') or attributes.get('description') or 'Databricks Genie space for data analysis',
                'databricks_url': f"{DATABRICKS_WORKSPACE_URL}/genie/spaces/{databricks_space_id}"
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
    if not genie_client:
        return jsonify({
            'success': False,
            'error': 'Genie not configured. Set DATABRICKS_WORKSPACE_URL and DATABRICKS_TOKEN.'
        }), 503

    data = request.json
    space_id = data.get('space_id')
    message = data.get('message')
    conversation_id = data.get('conversation_id')

    if not space_id or not message:
        return jsonify({'success': False, 'error': 'Missing space_id or message'}), 400

    try:
        # Start or continue conversation
        if not conversation_id:
            conversation_id, message_id = genie_client.start_conversation(space_id, message)
        else:
            message_id = genie_client.continue_conversation(space_id, conversation_id, message)

        # Wait for response
        result = genie_client.wait_for_response(space_id, conversation_id, message_id)

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
        'configured': bool(DATABRICKS_WORKSPACE_URL and DATABRICKS_TOKEN),
        'workspace_url': DATABRICKS_WORKSPACE_URL[:30] + '...' if DATABRICKS_WORKSPACE_URL else None
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