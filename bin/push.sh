#!/bin/bash
set -e

if [ -z "$WORKSPACES" ]; then
  echo "Error: No workspaces defined in WORKSPACES environment variable."
  exit 1
fi

IFS=',' read -ra WORKSPACE_LIST <<< "$WORKSPACES"

for workspace in "${WORKSPACE_LIST[@]}"; do
  echo "=== Push: ${workspace} ==="
  if wmill workspace switch "$workspace" && wmill sync push --skip-variables <<< "Y"; then
    echo "Done: ${workspace}"
  else
    echo "Error: push failed for workspace '${workspace}' (skipping, continuing)." >&2
  fi
done
