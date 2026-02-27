#!/bin/bash
# Daily pipeline refresh wrapper — invoked by macOS LaunchAgent.
set -euo pipefail

REPO="/Users/nihalvootkoor/Downloads/Auto Supply Chain"
cd "$REPO"

# Use the pip3-managed Python that has all dependencies.
PYTHON=$(which python3 2>/dev/null || echo /usr/bin/python3)

"$PYTHON" "$REPO/scripts/refresh_pipeline.py" --log-json --interval-hours 24
