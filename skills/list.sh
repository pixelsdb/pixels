#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for skill_dir in "$ROOT_DIR"/*; do
  [ -d "$skill_dir" ] || continue

  skill_name="$(basename "$skill_dir")"
  case "$skill_name" in
    codex|claude)
      continue
      ;;
  esac

  if [ -f "$skill_dir/skill.yaml" ]; then
    printf '%s\n' "$skill_name"
  fi
done
