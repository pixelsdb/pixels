#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

available_skills() {
  local dir found name
  found=false

  for dir in "$ROOT_DIR"/*; do
    [ -d "$dir" ] || continue
    [ -f "$dir/skill.yaml" ] || continue
    name="$(basename "$dir")"
    printf '  %s\n' "$name"
    found=true
  done

  if [ "$found" = false ]; then
    printf '  (none)\n'
  fi
}

usage() {
  cat <<EOF
Usage:
  $0 --skill <name> --tool <codex|claude> --scope <project|global>

Required arguments:
  --skill <name>          Skill directory name under skills/.
                           --agent is accepted as a deprecated alias.
  --tool <codex|claude>   Target AI tool.
  --scope <project|global>
                           project: install into this repository.
                           global: install into the current user's tool config.

Examples:
  $0 --skill pixels-install --tool codex --scope project
  $0 --skill pixels-install --tool codex --scope global
  $0 --skill pixels-install --tool claude --scope project

Available skills:
$(available_skills)
EOF
}

fail_with_usage() {
  local message

  for message in "$@"; do
    echo "Error: $message" >&2
  done
  echo >&2
  usage >&2
  exit 1
}

validate_skill_name() {
  if [[ "$SKILL" == */* || "$SKILL" == *".."* || ! "$SKILL" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
    ERRORS+=("--skill must be a simple directory name (letters, numbers, '.', '_', '-'), got: $SKILL")
  fi
}

parse_args() {
  SKILL=""
  TOOL=""
  SCOPE=""
  ERRORS=()

  while [ "$#" -gt 0 ]; do
    case "$1" in
      --skill|--agent)
        if [ "$#" -lt 2 ] || [[ "$2" == --* ]]; then
          ERRORS+=("$1 requires a value")
          shift
        else
          SKILL="$2"
          shift 2
        fi
        ;;
      --tool)
        if [ "$#" -lt 2 ] || [[ "$2" == --* ]]; then
          ERRORS+=("--tool requires a value")
          shift
        else
          TOOL="$2"
          shift 2
        fi
        ;;
      --scope)
        if [ "$#" -lt 2 ] || [[ "$2" == --* ]]; then
          ERRORS+=("--scope requires a value")
          shift
        else
          SCOPE="$2"
          shift 2
        fi
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        ERRORS+=("unknown argument: $1")
        shift
        ;;
    esac
  done

  [ -n "$SKILL" ] || ERRORS+=("missing --skill <name>")
  [ -n "$TOOL" ] || ERRORS+=("missing --tool <codex|claude>")
  [ -n "$SCOPE" ] || ERRORS+=("missing --scope <project|global>")

  if [ -n "$SKILL" ]; then
    validate_skill_name
  fi

  if [ -n "$TOOL" ]; then
    case "$TOOL" in
      codex|claude) ;;
      *) ERRORS+=("--tool must be codex or claude") ;;
    esac
  fi

  if [ -n "$SCOPE" ]; then
    case "$SCOPE" in
      project|global) ;;
      *) ERRORS+=("--scope must be project or global") ;;
    esac
  fi

  if [ "${#ERRORS[@]}" -gt 0 ]; then
    fail_with_usage "${ERRORS[@]}"
  fi
}

project_root() {
  cd "$ROOT_DIR/.." && pwd
}

skill_dir() {
  printf '%s/%s\n' "$ROOT_DIR" "$SKILL"
}

ensure_skill_exists() {
  local dir
  dir="$(skill_dir)"

  [ -d "$dir" ] || fail_with_usage "skill directory not found: $dir"
  [ -f "$dir/skill.yaml" ] || fail_with_usage "skill.yaml not found: $dir/skill.yaml"
}

copy_dir_if_exists() {
  local source_dir="$1"
  local target_dir="$2"

  [ -d "$source_dir" ] || return 0

  rm -rf "$target_dir"
  mkdir -p "$(dirname "$target_dir")"
  cp -R "$source_dir" "$target_dir"
}

copy_common_assets() {
  local source_dir="$1"
  local target_dir="$2"

  mkdir -p "$target_dir"
  cp "$source_dir/skill.yaml" "$target_dir/skill.yaml"
  copy_dir_if_exists "$source_dir/scripts" "$target_dir/scripts"
  copy_dir_if_exists "$source_dir/shared-scripts" "$target_dir/shared-scripts"
  copy_dir_if_exists "$source_dir/references" "$target_dir/references"
  copy_dir_if_exists "$source_dir/assets" "$target_dir/assets"
}

codex_target_dir() {
  if [ "$SCOPE" = "project" ]; then
    printf '%s/.agents/skills/%s\n' "$(project_root)" "$SKILL"
  else
    printf '%s/.agents/skills/%s\n' "$HOME" "$SKILL"
  fi
}

claude_agent_target() {
  if [ "$SCOPE" = "project" ]; then
    printf '%s/.claude/agents/%s.md\n' "$(project_root)" "$SKILL"
  else
    printf '%s/.claude/agents/%s.md\n' "$HOME" "$SKILL"
  fi
}

claude_assets_target() {
  if [ "$SCOPE" = "project" ]; then
    printf '%s/.claude/agent-assets/%s\n' "$(project_root)" "$SKILL"
  else
    printf '%s/.claude/agent-assets/%s\n' "$HOME" "$SKILL"
  fi
}

install_codex() {
  local source_dir target_dir source_skill
  source_dir="$(skill_dir)"
  source_skill="$source_dir/codex/SKILL.md"
  target_dir="$(codex_target_dir)"

  [ -f "$source_skill" ] || fail_with_usage "Codex SKILL.md not found: $source_skill"

  rm -rf "$target_dir"
  mkdir -p "$target_dir"
  cp "$source_skill" "$target_dir/SKILL.md"
  copy_common_assets "$source_dir" "$target_dir"

  echo "Installed Codex skill '$SKILL' to $target_dir"
}

install_claude() {
  local source_dir source_agent target_agent target_assets
  source_dir="$(skill_dir)"
  source_agent="$source_dir/claude/agent.md"
  target_agent="$(claude_agent_target)"
  target_assets="$(claude_assets_target)"

  [ -f "$source_agent" ] || fail_with_usage "Claude agent source not found: $source_agent"
  command -v python3 >/dev/null 2>&1 || fail_with_usage "python3 is required to install Claude agents"

  rm -rf "$target_assets"
  mkdir -p "$(dirname "$target_agent")" "$target_assets"
  copy_common_assets "$source_dir" "$target_assets"

  python3 - "$source_agent" "$target_agent" "$target_assets" "$(project_root)" <<'PY'
import sys
from pathlib import Path

source = Path(sys.argv[1])
target = Path(sys.argv[2])
assets = sys.argv[3]
repo = sys.argv[4]

text = source.read_text(encoding="utf-8").strip()
note = f"""## Installed Runtime Assets

- Installed from Pixels repository: `{repo}`
- Companion runtime assets: `{assets}`
- Helper scripts: `{assets}/scripts`
- Shared helper scripts: `{assets}/shared-scripts`
- Pass `REPO_ROOT=<Pixels repo root>` to helper scripts when running outside a Pixels checkout.
"""

lines = text.splitlines()
if lines and lines[0] == "---":
    end = None
    for index in range(1, len(lines)):
        if lines[index] == "---":
            end = index
            break
    if end is not None:
        text = "\n".join(lines[:end + 1]) + "\n\n" + note + "\n" + "\n".join(lines[end + 1:]).lstrip()
    else:
        text = note + "\n" + text
else:
    text = note + "\n" + text

target.write_text(text.rstrip() + "\n", encoding="utf-8")
PY

  echo "Installed Claude agent '$SKILL' to $target_agent"
  echo "Installed Claude companion assets to $target_assets"
}

parse_args "$@"
ensure_skill_exists

case "$TOOL" in
  codex) install_codex ;;
  claude) install_claude ;;
esac
