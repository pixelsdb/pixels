#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<EOF
Usage: $0 --agent <name> --tool <claude|codex> --scope <project|global>
EOF
}

fail() {
  echo "Error: $*" >&2
  exit 1
}

parse_args() {
  AGENT=""
  TOOL=""
  SCOPE=""

  while [ "$#" -gt 0 ]; do
    case "$1" in
      --agent)
        [ "$#" -ge 2 ] || fail "--agent requires a value"
        AGENT="$2"
        shift 2
        ;;
      --tool)
        [ "$#" -ge 2 ] || fail "--tool requires a value"
        TOOL="$2"
        shift 2
        ;;
      --scope)
        [ "$#" -ge 2 ] || fail "--scope requires a value"
        SCOPE="$2"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        fail "unknown argument: $1"
        ;;
    esac
  done

  [ -n "$AGENT" ] || fail "missing --agent <name>"
  [ -n "$TOOL" ] || fail "missing --tool <claude|codex>"
  [ -n "$SCOPE" ] || fail "missing --scope <project|global>"

  case "$TOOL" in
    claude|codex) ;;
    *) fail "--tool must be claude or codex" ;;
  esac

  case "$SCOPE" in
    project|global) ;;
    *) fail "--scope must be project or global" ;;
  esac
}

project_root() {
  cd "$ROOT_DIR/.." && pwd
}

claude_target() {
  if [ "$SCOPE" = "project" ]; then
    printf '%s/.claude/agents/%s.md\n' "$(project_root)" "$AGENT"
  else
    printf '%s/.claude/agents/%s.md\n' "$HOME" "$AGENT"
  fi
}

codex_target() {
  if [ "$SCOPE" = "project" ]; then
    printf '%s/AGENTS.md\n' "$(project_root)"
  else
    printf '%s/.codex/AGENTS.md\n' "$HOME"
  fi
}

parse_args "$@"

uninstall_claude() {
  local target_file
  target_file="$(claude_target)"

  if [ -f "$target_file" ]; then
    rm -f "$target_file"
    echo "Removed Claude agent '$AGENT' from $target_file"
  else
    echo "Claude target not found: $target_file"
  fi
}

uninstall_codex() {
  local target_file
  target_file="$(codex_target)"

  if [ ! -f "$target_file" ]; then
    echo "Codex target not found: $target_file"
    return 0
  fi

  python3 - "$target_file" "$AGENT" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
agent = sys.argv[2]
begin = f"<!-- BEGIN AGENT: {agent} -->"
end = f"<!-- END AGENT: {agent} -->"

text = path.read_text(encoding="utf-8")
start = text.find(begin)

if start < 0:
    print(f"Codex block not found for agent '{agent}' in {path}")
    raise SystemExit(0)

stop = text.find(end, start)
if stop < 0:
    print(f"Error: found {begin} without matching {end} in {path}", file=sys.stderr)
    raise SystemExit(1)

stop += len(end)
new_text = text[:start].rstrip() + "\n" + text[stop:].lstrip("\n")
path.write_text(new_text.rstrip() + ("\n" if new_text.strip() else ""), encoding="utf-8")
print(f"Removed Codex block for agent '{agent}' from {path}")
PY
}

case "$TOOL" in
  claude)
    uninstall_claude
    ;;
  codex)
    uninstall_codex
    ;;
esac
