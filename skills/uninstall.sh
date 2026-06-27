#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<EOF
Usage:
  $0 --skill <name> --tool <codex|claude> --scope <project|global>

Required arguments:
  --skill <name>          Skill name to remove.
                           --agent is accepted as a deprecated alias.
  --tool <codex|claude>   Target AI tool.
  --scope <project|global>
                           project: remove from this repository.
                           global: remove from the current user's tool config.

Examples:
  $0 --skill pixels-install --tool codex --scope project
  $0 --skill pixels-install --tool claude --scope global
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

remove_path() {
  local label="$1"
  local path="$2"

  if [ -e "$path" ]; then
    rm -rf "$path"
    echo "Removed $label: $path"
  else
    echo "$label not found: $path"
  fi
}

uninstall_codex() {
  remove_path "Codex skill '$SKILL'" "$(codex_target_dir)"
}

uninstall_claude() {
  remove_path "Claude agent '$SKILL'" "$(claude_agent_target)"
  remove_path "Claude companion assets for '$SKILL'" "$(claude_assets_target)"
}

parse_args "$@"

case "$TOOL" in
  codex) uninstall_codex ;;
  claude) uninstall_claude ;;
esac
