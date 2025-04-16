#!/bin/bash

set -e

source .env

IFS=',' read -ra WORKSPACE_LIST <<< "$WORKSPACES"

for workspace in "${WORKSPACE_LIST[@]}"; do
  wmill workspace switch "$workspace"
  wmill sync push --skip-variables <<< "Y"
done
