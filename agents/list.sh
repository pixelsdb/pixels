#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for agent_dir in "$ROOT_DIR"/*; do
  [ -d "$agent_dir" ] || continue

  agent_name="$(basename "$agent_dir")"
  case "$agent_name" in
    scripts|skills)
      continue
      ;;
  esac

  if [ -f "$agent_dir/agent.yaml" ]; then
    printf '%s\n' "$agent_name"
  fi
done
