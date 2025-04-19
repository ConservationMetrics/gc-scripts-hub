#!/bin/bash
set -e

if [ -z "$WORKSPACES" ]; then
  echo "Error: No workspaces defined in WORKSPACES environment variable."
  exit 1
fi

IFS=',' read -ra WORKSPACE_LIST <<< "$WORKSPACES"

for workspace in "${WORKSPACE_LIST[@]}"; do
  wmill workspace switch "$workspace"
  wmill sync push --skip-variables <<< "Y"
done
