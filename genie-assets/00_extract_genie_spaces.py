#!/usr/bin/env python3
"""
Databricks Genie Spaces DETAILED Exploration Script
====================================================

This enhanced version extracts ALL the rich metadata from the serialized_space field,
including tables, descriptions, sample questions, instructions, SQL examples, and more!

This will help us design a comprehensive Atlan asset model.
"""

import os
import json
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any
import requests
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.json import JSON
from rich.tree import Tree
from rich import print as rprint

# Load environment variables
load_dotenv()

# Initialize Rich console for pretty output
console = Console()


class DatabricksGenieDetailedClient:
    """Enhanced client for extracting detailed Genie Space metadata"""

    def __init__(self, workspace_url: str, token: str):
        """Initialize the Databricks Genie client."""
        self.workspace_url = workspace_url.rstrip('/')
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    def list_genie_spaces(self) -> Optional[List[Dict]]:
        """Retrieve all Genie Spaces from the Databricks workspace."""
        endpoint = f"{self.workspace_url}/api/2.0/genie/spaces"
        console.print(f"[cyan]🔍 Fetching Genie Spaces from:[/cyan] {endpoint}")

        try:
            response = requests.get(endpoint, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            spaces = data.get('spaces', [])
            console.print(f"[green]✅ Found {len(spaces)} Genie Space(s)[/green]")
            return spaces
        except requests.exceptions.RequestException as e:
            console.print(f"[red]❌ Error: {e}[/red]")
            return None

    def get_genie_space_details(self, space_id: str) -> Optional[Dict]:
        """Get FULL details including serialized_space data."""
        endpoint = f"{self.workspace_url}/api/2.0/genie/spaces/{space_id}?include_serialized_space=true"

        console.print(f"[cyan]🔍 Fetching detailed data for space: {space_id}[/cyan]")

        try:
            response = requests.get(endpoint, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            console.print(f"[red]❌ Error fetching details: {e}[/red]")
            return None


def extract_metadata_from_serialized_space(serialized_space: Any) -> Dict:
    """
    Extract all valuable metadata from the serialized_space field.

    Returns a structured dictionary with all extracted information.
    """
    # Parse if it's a string
    if isinstance(serialized_space, str):
        try:
            serialized_space = json.loads(serialized_space)
            console.print("[yellow]  ℹ Parsed serialized_space from JSON string[/yellow]")
        except json.JSONDecodeError as e:
            console.print(f"[red]  ❌ Failed to parse serialized_space: {e}[/red]")
            return {}

    metadata = {
        "tables": [],
        "table_descriptions": {},
        "column_configs": [],
        "sample_questions": [],
        "instructions": None,
        "example_sql": [],
        "join_specs": [],
        "sql_snippets": [],
        "filters": [],
        "measures": [],
        "dimensions": []
    }

    # Extract data sources (tables or metric_views)
    if 'data_sources' in serialized_space:
        data_sources = serialized_space['data_sources']

        # Extract tables (some spaces use 'tables', others use 'metric_views')
        if 'tables' in data_sources:
            metadata['tables'] = data_sources['tables']
            console.print(f"[green]  ✓ Found {len(metadata['tables'])} tables[/green]")
        elif 'metric_views' in data_sources:
            metadata['tables'] = data_sources['metric_views']
            console.print(f"[green]  ✓ Found {len(metadata['tables'])} metric views[/green]")

        if metadata['tables'] and len(metadata['tables']) > 0:
            first_table = metadata['tables'][0]
            console.print(f"[dim]    Table format: {type(first_table).__name__}[/dim]")
            if isinstance(first_table, dict):
                console.print(f"[dim]    Keys: {list(first_table.keys())}[/dim]")

        # Extract table descriptions
        if 'table_descriptions' in data_sources:
            metadata['table_descriptions'] = data_sources['table_descriptions']
            console.print(f"[green]  ✓ Found {len(metadata['table_descriptions'])} table descriptions[/green]")

        # Extract column configurations
        if 'column_configurations' in data_sources:
            metadata['column_configs'] = data_sources['column_configurations']
            console.print(f"[green]  ✓ Found column configurations[/green]")

    # Extract text instructions (check both top-level and nested paths)
    if 'text_instructions' in serialized_space:
        metadata['instructions'] = serialized_space['text_instructions']
        console.print(f"[green]  ✓ Found business instructions ({len(metadata['instructions'])} chars)[/green]")
    elif 'instructions' in serialized_space and isinstance(serialized_space['instructions'], dict):
        # Instructions may contain example_question_sqls with embedded business logic
        instr = serialized_space['instructions']
        if 'example_question_sqls' in instr:
            # Concatenate all SQL/instruction text from example_question_sqls
            all_text = []
            for item in instr['example_question_sqls']:
                question_parts = item.get('question', [])
                sql_parts = item.get('sql', [])
                all_text.extend(question_parts)
                all_text.extend(sql_parts)
            metadata['instructions'] = ''.join(all_text)
            console.print(f"[green]  ✓ Found business instructions from example_question_sqls ({len(metadata['instructions'])} chars)[/green]")

    # Extract sample questions (check both top-level and config.sample_questions)
    if 'sample_questions' in serialized_space:
        metadata['sample_questions'] = serialized_space['sample_questions']
        console.print(f"[green]  ✓ Found {len(metadata['sample_questions'])} sample questions[/green]")
    elif 'config' in serialized_space and 'sample_questions' in serialized_space.get('config', {}):
        raw_questions = serialized_space['config']['sample_questions']
        # Questions may be dicts with 'question' key containing a list of strings
        for q in raw_questions:
            if isinstance(q, dict) and 'question' in q:
                metadata['sample_questions'].append(' '.join(q['question']))
            elif isinstance(q, str):
                metadata['sample_questions'].append(q)
        console.print(f"[green]  ✓ Found {len(metadata['sample_questions'])} sample questions (from config)[/green]")

    # Extract SQL examples (check both top-level and instructions.example_question_sqls)
    if 'sql_examples' in serialized_space:
        metadata['example_sql'] = serialized_space['sql_examples']
        console.print(f"[green]  ✓ Found {len(metadata['example_sql'])} SQL examples[/green]")
    elif 'instructions' in serialized_space and isinstance(serialized_space['instructions'], dict):
        example_sqls = serialized_space['instructions'].get('example_question_sqls', [])
        if example_sqls:
            metadata['example_sql'] = example_sqls
            console.print(f"[green]  ✓ Found {len(metadata['example_sql'])} SQL examples (from instructions)[/green]")

    # Extract SQL snippets (filters, measures, etc.)
    if 'sql_snippets' in serialized_space:
        snippets = serialized_space['sql_snippets']

        for snippet in snippets:
            snippet_type = snippet.get('type', 'unknown')
            if snippet_type == 'filter':
                metadata['filters'].append(snippet)
            elif snippet_type == 'measure':
                metadata['measures'].append(snippet)
            elif snippet_type == 'dimension':
                metadata['dimensions'].append(snippet)
            else:
                metadata['sql_snippets'].append(snippet)

        console.print(f"[green]  ✓ Found {len(snippets)} SQL snippets[/green]")
        if metadata['filters']:
            console.print(f"[green]    - {len(metadata['filters'])} filters[/green]")
        if metadata['measures']:
            console.print(f"[green]    - {len(metadata['measures'])} measures[/green]")
        if metadata['dimensions']:
            console.print(f"[green]    - {len(metadata['dimensions'])} dimensions[/green]")

    # Extract join specifications
    if 'join_specifications' in serialized_space:
        metadata['join_specs'] = serialized_space['join_specifications']
        console.print(f"[green]  ✓ Found {len(metadata['join_specs'])} join specifications[/green]")

    return metadata


def display_detailed_analysis(space: Dict, metadata: Dict):
    """Display a comprehensive analysis of a Genie Space."""

    # Extract owner from parent_path
    parent_path = space.get('parent_path', 'N/A')
    owner = parent_path.replace('/Users/', '') if parent_path else 'N/A'

    # Format timestamps if available
    created_ts = space.get('created_timestamp')
    updated_ts = space.get('last_updated_timestamp')
    created_str = datetime.fromtimestamp(created_ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC') if created_ts else 'N/A'
    updated_str = datetime.fromtimestamp(updated_ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC') if updated_ts else 'N/A'

    # Create main info panel
    console.print(Panel.fit(
        f"[bold cyan]Genie Space: {space.get('title', 'Unknown')}[/bold cyan]\n"
        f"Space ID: {space.get('space_id', 'N/A')}\n"
        f"Warehouse ID: {space.get('warehouse_id', 'N/A')}\n"
        f"Owner: {owner}\n"
        f"Created: {created_str}\n"
        f"Last Updated: {updated_str}",
        title="Space Overview",
        border_style="cyan"
    ))

    # Display tables
    if metadata['tables']:
        table = Table(title="📊 Data Tables", show_header=True)
        table.add_column("Table Name", style="cyan")
        table.add_column("Description", style="yellow")

        for table_item in metadata['tables']:
            # Handle both string and dict formats
            if isinstance(table_item, dict):
                table_name = table_item.get('identifier', '') or table_item.get('name', '') or table_item.get('table', '') or str(table_item)
            else:
                table_name = str(table_item)

            desc = metadata['table_descriptions'].get(table_name, "No description") if isinstance(metadata['table_descriptions'], dict) else "No description"
            # Truncate long descriptions
            if len(str(desc)) > 60:
                desc = str(desc)[:57] + "..."
            table.add_row(table_name, str(desc))

        console.print(table)

    # Display sample questions
    if metadata['sample_questions']:
        console.print("\n[bold magenta]💭 Sample Questions:[/bold magenta]")
        for i, question in enumerate(metadata['sample_questions'][:5], 1):  # Show first 5
            console.print(f"  {i}. {question}")
        if len(metadata['sample_questions']) > 5:
            console.print(f"  ... and {len(metadata['sample_questions']) - 5} more")

    # Display business instructions
    if metadata['instructions']:
        console.print("\n[bold yellow]📝 Business Instructions:[/bold yellow]")
        # Show first 200 characters
        preview = metadata['instructions'][:200]
        if len(metadata['instructions']) > 200:
            preview += "..."
        console.print(f"  {preview}")

    # Display SQL snippets summary
    if any([metadata['filters'], metadata['measures'], metadata['dimensions']]):
        console.print("\n[bold green]🔧 SQL Components:[/bold green]")

        if metadata['filters']:
            console.print(f"  • Filters: {len(metadata['filters'])}")
            for f in metadata['filters'][:2]:  # Show first 2
                console.print(f"    - {f.get('alias', 'unnamed')}: {f.get('sql_expression', '')[:50]}...")

        if metadata['measures']:
            console.print(f"  • Measures: {len(metadata['measures'])}")
            for m in metadata['measures'][:2]:  # Show first 2
                console.print(f"    - {m.get('alias', 'unnamed')}: {m.get('sql_expression', '')[:50]}...")

        if metadata['dimensions']:
            console.print(f"  • Dimensions: {len(metadata['dimensions'])}")
            for d in metadata['dimensions'][:2]:  # Show first 2
                console.print(f"    - {d.get('alias', 'unnamed')}: {d.get('sql_expression', '')[:50]}...")


def create_atlan_metadata_summary(space: Dict, metadata: Dict) -> Dict:
    """
    Create a summary of metadata suitable for Atlan CustomEntity.
    This shows what we'll store in Atlan.
    """

    # Build a rich description for Atlan
    description_parts = []

    if metadata['instructions']:
        description_parts.append(f"Business Logic: {metadata['instructions'][:200]}...")

    if metadata['tables']:
        # Extract table names from dict or string format
        table_names = []
        for t in metadata['tables'][:5]:
            if isinstance(t, dict):
                name = t.get('identifier', '') or t.get('name', '') or t.get('table', '') or str(t)
            else:
                name = str(t)
            table_names.append(name)
        description_parts.append(f"Tables: {', '.join(table_names)}")

    if metadata['sample_questions']:
        description_parts.append(f"Sample Questions: {len(metadata['sample_questions'])} available")

    rich_description = "\n\n".join(description_parts)

    # Create the Atlan asset metadata
    atlan_metadata = {
        "name": space.get('title', 'Unknown Genie Space'),
        "qualified_name": f"genie-spaces/{space.get('space_id')}",
        "asset_user_defined_type": "Genie Space",
        "user_description": rich_description,
        "custom_attributes": {
            "space_id": space.get('space_id'),
            "warehouse_id": space.get('warehouse_id'),
            "table_count": len(metadata['tables']),
            "tables": metadata['tables'],
            "sample_questions": metadata['sample_questions'][:10],  # Store first 10
            "has_instructions": bool(metadata['instructions']),
            "filter_count": len(metadata['filters']),
            "measure_count": len(metadata['measures']),
            "dimension_count": len(metadata['dimensions']),
            "databricks_url": f"https://dbc-8d941db8-48cd.cloud.databricks.com/genie/spaces/{space.get('space_id')}",
            # Fields from API timestamps
            "category": "AI/BI",
            "createdBy": space.get('parent_path', '').replace('/Users/', '') or "Unknown",
            "totalQueries": 0,  # Not available from Genie Spaces API
            "uniqueUsers": 0,  # Not available from Genie Spaces API
            "lastAccessed": (
                datetime.fromtimestamp(space['last_updated_timestamp'] / 1000, tz=timezone.utc).isoformat()
                if space.get('last_updated_timestamp')
                else datetime.now(timezone.utc).isoformat()
            ),
            "avgResponseTime": 0,  # Not available from Genie Spaces API
            "created_timestamp": space.get('created_timestamp'),
            "last_updated_timestamp": space.get('last_updated_timestamp'),
        }
    }

    return atlan_metadata


def main():
    """Main execution function"""

    console.print(Panel.fit(
        "[bold cyan]Databricks Genie Spaces DETAILED Explorer[/bold cyan]\n"
        "Extracting comprehensive metadata for Atlan integration",
        border_style="cyan"
    ))

    # Configuration
    DATABRICKS_URL = os.getenv("DATABRICKS_HOST")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

    if not DATABRICKS_URL or not DATABRICKS_TOKEN:
        console.print("[red]❌ Error: DATABRICKS_HOST and DATABRICKS_TOKEN must be set in .env[/red]")
        return

    # Initialize client
    client = DatabricksGenieDetailedClient(DATABRICKS_URL, DATABRICKS_TOKEN)

    # Fetch all spaces
    spaces = client.list_genie_spaces()

    if not spaces:
        console.print("[yellow]No Genie Spaces found or error occurred.[/yellow]")
        return

    # Process each space with detailed extraction
    all_detailed_data = []
    all_atlan_metadata = []

    for space in spaces:
        console.print(f"\n[bold]{'='*60}[/bold]")
        console.print(f"[bold cyan]Processing: {space.get('title', 'Unknown')}[/bold cyan]")

        # Get detailed data
        detailed = client.get_genie_space_details(space['space_id'])

        if detailed and 'serialized_space' in detailed:
            # Merge additional fields from detailed response into space dict
            for key in ('created_timestamp', 'last_updated_timestamp', 'parent_path'):
                if key in detailed:
                    space[key] = detailed[key]

            # Extract metadata
            metadata = extract_metadata_from_serialized_space(detailed['serialized_space'])

            # Display analysis
            display_detailed_analysis(space, metadata)

            # Create Atlan metadata
            atlan_meta = create_atlan_metadata_summary(space, metadata)
            all_atlan_metadata.append(atlan_meta)

            # Store for file output
            all_detailed_data.append({
                "basic_info": space,
                "extracted_metadata": metadata,
                "raw_serialized": detailed.get('serialized_space')
            })
        else:
            console.print("[yellow]  ⚠️  No detailed data available[/yellow]")

    # Save comprehensive data
    if all_detailed_data:
        # Save detailed extraction
        with open('genie_spaces_detailed_analysis.json', 'w') as f:
            json.dump(all_detailed_data, f, indent=2)
        console.print(f"\n[green]💾 Detailed analysis saved to genie_spaces_detailed_analysis.json[/green]")

        # Save Atlan metadata preview
        with open('genie_spaces_atlan_preview.json', 'w') as f:
            json.dump(all_atlan_metadata, f, indent=2)
        console.print(f"[green]💾 Atlan metadata preview saved to genie_spaces_atlan_preview.json[/green]")

    # Final summary
    console.print(f"\n[bold]{'='*60}[/bold]")
    console.print(Panel.fit(
        f"[bold green]✨ Analysis Complete![/bold green]\n\n"
        f"Processed: {len(spaces)} Genie Space(s)\n\n"
        f"[cyan]Key Findings:[/cyan]\n"
        f"• Rich metadata available in serialized_space\n"
        f"• Tables, descriptions, and business logic captured\n"
        f"• Sample questions and SQL components extracted\n"
        f"• Ready to design comprehensive Atlan assets!\n\n"
        f"[yellow]Next Steps:[/yellow]\n"
        f"1. Review the JSON files for complete details\n"
        f"2. Confirm which metadata fields to include in Atlan\n"
        f"3. Build the Temporal workflow for syncing",
        title="Summary",
        border_style="green"
    ))


if __name__ == "__main__":
    main()