#!/usr/bin/env bash
set -euo pipefail

# Thin CLI over the phase-checkpoint helpers in lib/shell_env.sh. A full
# install walks the phases listed in agent.yaml across many separate tool
# calls; if the session is interrupted partway through (dropped SSH
# session, context compaction, the user stepping away), resuming without
# this would mean either re-reading the whole conversation to guess what
# already happened, or blindly re-running steps that already succeeded.
#
# The agent should run `progress.sh mark <phase>` right after a phase from
# agent.yaml's `phases` list actually succeeds, and `progress.sh show` at
# the start of a session (or after a long gap) to see what is already done
# before deciding what to do next. This script does not run any install
# step itself - it only records/reports state.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/shell_env.sh
source "$SCRIPT_DIR/lib/shell_env.sh"

usage() {
  cat <<EOF
Usage:
  $0 mark <phase> [note]   Record <phase> (one of agent.yaml's phases) as done.
  $0 show                  Print every phase recorded as done so far.
  $0 is-done <phase>       Exit 0 if <phase> is already recorded as done, 1 otherwise.
  $0 reset                 Remove the progress file (start a fresh install run).

The progress file lives at: $(progress_file)
It is generated/user-specific and must never be committed.
EOF
}

cmd_mark() {
  local phase="${1:-}"
  local note="${2:-}"
  [[ -n "$phase" ]] || { usage >&2; exit 1; }

  mark_phase_done "$phase" "$note"
  printf 'marked done: %s\n' "$phase"
}

cmd_show() {
  print_progress
}

cmd_is_done() {
  local phase="${1:-}"
  [[ -n "$phase" ]] || { usage >&2; exit 1; }

  if phase_is_done "$phase"; then
    printf '%s: done\n' "$phase"
    exit 0
  else
    printf '%s: not done\n' "$phase"
    exit 1
  fi
}

cmd_reset() {
  local file
  file="$(progress_file)"
  rm -f "$file"
  printf 'progress file removed: %s\n' "$file"
}

main() {
  local sub="${1:-}"
  [[ -n "$sub" ]] || { usage >&2; exit 1; }
  shift

  case "$sub" in
    mark) cmd_mark "$@" ;;
    show) cmd_show "$@" ;;
    is-done) cmd_is_done "$@" ;;
    reset) cmd_reset "$@" ;;
    -h|--help) usage ;;
    *)
      printf 'ERROR: unknown subcommand: %s\n' "$sub" >&2
      usage >&2
      exit 1
      ;;
  esac
}

main "$@"
