#!/bin/bash
# Daily pipeline refresh wrapper — invoked by macOS LaunchAgent.
set -euo pipefail

REPO="/Users/nihalvootkoor/Documents/AutoSupplyChain"
cd "$REPO"

# Hardcode the Python that has all project dependencies installed.
PYTHON="/Library/Developer/CommandLineTools/usr/bin/python3"

"$PYTHON" "$REPO/scripts/refresh_pipeline.py" --log-json --interval-hours 24
