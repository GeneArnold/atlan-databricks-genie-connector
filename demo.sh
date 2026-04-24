#!/usr/bin/env bash
# One-command demo runner for the Atlan + Databricks Genie integration.
# Extracts every Genie Space from all configured Databricks workspaces,
# syncs them as CustomEntity assets in Atlan with rich READMEs, then
# creates/updates lineage between source tables and each Genie Space.
#
# Usage:  ./demo.sh
set -e

cd "$(dirname "$0")/genie-assets"

echo ""
echo "================================================================"
echo "  [1/2] Extracting and syncing Genie Spaces to Atlan"
echo "================================================================"
python 03_extract_and_sync_genie_spaces.py

echo ""
echo "================================================================"
echo "  [2/2] Creating lineage (source tables → Genie Spaces)"
echo "================================================================"
python 04_create_lineage.py

echo ""
echo "================================================================"
echo "  Demo sync complete."
echo "================================================================"
