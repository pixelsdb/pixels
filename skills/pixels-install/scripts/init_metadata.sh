#!/usr/bin/env bash
if [ -z "${BASH_VERSION:-}" ]; then
  printf 'ERROR: pixels-install scripts must be executed by Bash; do not run this script with zsh.\n' >&2
  exit 1
fi
if [ "${BASH_SOURCE[0]}" != "$0" ]; then
  printf 'ERROR: do not source pixels-install installer scripts; execute this script directly with Bash.\n' >&2
  return 1 2>/dev/null || exit 1
fi
set -euo pipefail

# Default metadata setup: run pixels-cli INIT-META against the database
# configured in PIXELS_HOME/etc/pixels.properties. Derby is the default
# backend (configure_pixels.sh writes a jdbc:derby URL). MySQL is an
# optional alternative: run install_mysql.sh and configure_pixels.sh with
# METADATA_DB_TYPE=mysql first.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/shell_env.sh
source "$SCRIPT_DIR/lib/shell_env.sh"
load_toolchain_env

SKILL_DIR="${SKILL_DIR:-$(skill_dir)}"
STATE_DIR="${STATE_DIR:-$(state_dir)}"
DEPLOYMENT_FILE="${DEPLOYMENT_FILE:-$STATE_DIR/deployment.env}"
if [[ -f "$DEPLOYMENT_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$DEPLOYMENT_FILE"
  set +a
fi
PIXELS_HOME="${PIXELS_HOME:-$HOME/opt/pixels}"
PIXELS_HOME="${PIXELS_HOME%/}"
CONFIG_FILE="${PIXELS_CONFIG_FILE:-${PIXELS_CONFIG:-$PIXELS_HOME/etc/pixels.properties}}"

log() { printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"; }
fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

prop() {
  grep -E "^[[:space:]]*$1=" "$CONFIG_FILE" | tail -n 1 | cut -d= -f2-
}

[[ -f "$CONFIG_FILE" ]] || fail "missing Pixels config file: $CONFIG_FILE"
CLI_JAR=$(ls -1 "$PIXELS_HOME"/sbin/pixels-cli-*-full.jar 2>/dev/null | head -n 1 || true)
[[ -n "$CLI_JAR" ]] || fail "pixels-cli fat jar not found under $PIXELS_HOME/sbin"

DRIVER="$(prop metadata.db.driver)"
URL="$(prop metadata.db.url)"
[[ -n "$DRIVER" && -n "$URL" ]] || fail "metadata.db.driver / metadata.db.url not set in $CONFIG_FILE"

mkdir -p "$PIXELS_HOME/var"

log "PIXELS_HOME=$PIXELS_HOME"
log "config=$CONFIG_FILE"
log "driver=$DRIVER"
log "url=$URL"
log "running INIT-META"

CLI_OUT="$(mktemp "${TMPDIR:-/tmp}/pixels-init-meta.XXXXXX.out")"
trap 'rm -f "$CLI_OUT"' EXIT
set +e
printf 'INIT-META\nexit\n' | PIXELS_HOME="$PIXELS_HOME" PIXELS_CONFIG="$CONFIG_FILE" \
  java -jar "$CLI_JAR" >"$CLI_OUT" 2>&1
CLI_RC=$?
set -e
cat "$CLI_OUT"

grep -q "INIT-META finished" "$CLI_OUT" || fail "INIT-META did not finish successfully"
[[ "$CLI_RC" -eq 0 ]] || fail "pixels-cli exited with $CLI_RC"

if grep -qi "DERBY" "$CLI_OUT"; then
  log "INIT-META created Derby metadata tables"
elif grep -qi "MYSQL" "$CLI_OUT"; then
  log "INIT-META created MySQL metadata tables"
else
  log "INIT-META finished"
fi
