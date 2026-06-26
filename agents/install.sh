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

agent_dir() {
  printf '%s/%s\n' "$ROOT_DIR" "$AGENT"
}

ensure_agent_exists() {
  local dir
  dir="$(agent_dir)"

  [ -d "$dir" ] || fail "agent directory not found: $dir"
  [ -f "$dir/agent.yaml" ] || fail "agent.yaml not found: $dir/agent.yaml"
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
ensure_agent_exists

AGENT_DIR="$(agent_dir)"

install_claude() {
  local source_file target_file
  source_file="$AGENT_DIR/claude.md"
  target_file="$(claude_target)"

  [ -f "$source_file" ] || fail "Claude source not found: $source_file"

  mkdir -p "$(dirname "$target_file")"
  cp "$source_file" "$target_file"
  echo "Installed Claude agent '$AGENT' to $target_file"
}

install_codex() {
  local source_file target_file
  source_file="$AGENT_DIR/codex.md"
  target_file="$(codex_target)"

  [ -f "$source_file" ] || fail "Codex source not found: $source_file"

  mkdir -p "$(dirname "$target_file")"

  python3 - "$target_file" "$AGENT" "$source_file" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
agent = sys.argv[2]
source_path = Path(sys.argv[3])
content = source_path.read_text(encoding="utf-8").strip()
block = f"<!-- BEGIN AGENT: {agent} -->\n{content}\n<!-- END AGENT: {agent} -->\n"
begin = f"<!-- BEGIN AGENT: {agent} -->"
end = f"<!-- END AGENT: {agent} -->"

text = path.read_text(encoding="utf-8") if path.exists() else ""
start = text.find(begin)

if start >= 0:
    stop = text.find(end, start)
    if stop < 0:
        print(f"Error: found {begin} without matching {end} in {path}", file=sys.stderr)
        raise SystemExit(1)
    stop += len(end)
    new_text = text[:start].rstrip() + "\n\n" + block + text[stop:].lstrip("\n")
else:
    prefix = text.rstrip()
    new_text = (prefix + "\n\n" if prefix else "") + block

path.write_text(new_text, encoding="utf-8")
PY

  echo "Installed Codex agent '$AGENT' to $target_file"
}

case "$TOOL" in
  claude)
    install_claude
    ;;
  codex)
    install_codex
    ;;
esac
