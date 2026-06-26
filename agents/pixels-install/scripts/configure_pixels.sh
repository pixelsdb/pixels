#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/shell_env.sh
source "$SCRIPT_DIR/lib/shell_env.sh"
# Picks up PIXELS_HOME from an earlier, separately invoked
# build_install_pixels.sh run, in case it was customized away from the
# default.
load_toolchain_env

# Picks up METADATA_DB_USER/METADATA_DB_PASSWORD/METADATA_DB_NAME written by
# install_mysql.sh, so the MySQL credentials confirmed interactively there
# are the same ones written into pixels.properties below. Explicit env vars
# passed to this script still take precedence over the secrets file.
AGENT_DIR="${AGENT_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
DEPLOYMENT_FILE="${DEPLOYMENT_FILE:-$AGENT_DIR/deployment.env}"
USE_DEPLOYMENT_FILE="${USE_DEPLOYMENT_FILE:-true}"
if [[ "$USE_DEPLOYMENT_FILE" == "true" && -f "$DEPLOYMENT_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$DEPLOYMENT_FILE"
  set +a
fi
PIXELS_HOME="${PIXELS_HOME:-$HOME/opt/pixels}"
CONFIG_FILE="${PIXELS_CONFIG_FILE:-$PIXELS_HOME/etc/pixels.properties}"
BACKUP_FILE="${BACKUP_FILE:-$CONFIG_FILE.bak.$(date '+%Y%m%d%H%M%S')}"
SECRETS_FILE="${SECRETS_FILE:-$AGENT_DIR/deployment.secrets.env}"
METADATA_DB_PASSWORD_SET_BEFORE_SECRETS=false
[[ -n "${METADATA_DB_PASSWORD+x}" ]] && METADATA_DB_PASSWORD_SET_BEFORE_SECRETS=true
if [[ -f "$SECRETS_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$SECRETS_FILE"
  set +a
fi

PIXELS_VAR_DIR="${PIXELS_VAR_DIR:-$PIXELS_HOME/var/}"
METADATA_DB_DRIVER="${METADATA_DB_DRIVER:-com.mysql.cj.jdbc.Driver}"
METADATA_DB_USER="${METADATA_DB_USER:-pixels}"
ALLOW_DEFAULT_METADATA_PASSWORD="${ALLOW_DEFAULT_METADATA_PASSWORD:-false}"
if [[ -z "${METADATA_DB_PASSWORD:-}" && "$ALLOW_DEFAULT_METADATA_PASSWORD" == "true" ]]; then
  METADATA_DB_PASSWORD="password"
fi
METADATA_DB_PASSWORD="${METADATA_DB_PASSWORD:-}"
METADATA_DB_HOST="${METADATA_DB_HOST:-localhost}"
METADATA_DB_PORT="${METADATA_DB_PORT:-3306}"
METADATA_DB_NAME="${METADATA_DB_NAME:-pixels_metadata}"
METADATA_DB_URL="${METADATA_DB_URL:-jdbc:mysql://$METADATA_DB_HOST:$METADATA_DB_PORT/$METADATA_DB_NAME?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull}"
METADATA_SERVER_HOST="${METADATA_SERVER_HOST:-localhost}"
METADATA_SERVER_PORT="${METADATA_SERVER_PORT:-18888}"
TRANS_SERVER_HOST="${TRANS_SERVER_HOST:-localhost}"
TRANS_SERVER_PORT="${TRANS_SERVER_PORT:-18889}"
QUERY_SCHEDULE_SERVER_HOST="${QUERY_SCHEDULE_SERVER_HOST:-localhost}"
QUERY_SCHEDULE_SERVER_PORT="${QUERY_SCHEDULE_SERVER_PORT:-18893}"
ETCD_HOSTS="${ETCD_HOSTS:-localhost}"
ETCD_PORT="${ETCD_PORT:-2379}"
METRICS_NODE_TEXT_DIR="${METRICS_NODE_TEXT_DIR:-$HOME/opt/node_exporter/text/}"
PRESTO_PIXELS_JDBC_URL="${PRESTO_PIXELS_JDBC_URL:-jdbc:trino://localhost:8080/pixels/tpch}"
CACHE_ENABLED="${CACHE_ENABLED:-false}"
CONFIGURE_PIXELS_WORKERS_FILE="${CONFIGURE_PIXELS_WORKERS_FILE:-true}"

log() {
  printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"
}

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

set_property() {
  local key="$1"
  local value="$2"
  local escaped_key
  local escaped_value

  escaped_key="$(printf '%s' "$key" | sed 's/[.[\\*^$()+?{}|]/\\&/g')"
  escaped_value="$(printf '%s' "$value" | sed 's/[\\&]/\\&/g')"

  if grep -qE "^[[:space:]]*${escaped_key}=" "$CONFIG_FILE"; then
    sed -i -E "s|^[[:space:]]*${escaped_key}=.*|${key}=${escaped_value}|" "$CONFIG_FILE"
  else
    printf '%s=%s\n' "$key" "$value" >> "$CONFIG_FILE"
  fi
}

validate_boolean() {
  case "$CACHE_ENABLED" in
    true|false) ;;
    *) fail "CACHE_ENABLED must be true or false" ;;
  esac
  case "$CONFIGURE_PIXELS_WORKERS_FILE" in
    true|false) ;;
    *) fail "CONFIGURE_PIXELS_WORKERS_FILE must be true or false" ;;
  esac
}

validate_metadata_password() {
  if [[ -n "$METADATA_DB_PASSWORD" ]]; then
    return
  fi

  if [[ "$METADATA_DB_PASSWORD_SET_BEFORE_SECRETS" == "true" ]]; then
    fail "METADATA_DB_PASSWORD was set before sourcing secrets but resolved to empty"
  fi

  fail "METADATA_DB_PASSWORD is not set; run install_mysql.sh first, export METADATA_DB_PASSWORD explicitly, or set ALLOW_DEFAULT_METADATA_PASSWORD=true to write the documented default password"
}

validate_config_file() {
  [[ -f "$CONFIG_FILE" ]] || fail "missing Pixels config file: $CONFIG_FILE"
  [[ -w "$CONFIG_FILE" ]] || fail "Pixels config file is not writable: $CONFIG_FILE"
}

configure_pixels() {
  cp "$CONFIG_FILE" "$BACKUP_FILE"
  log "backup created: $BACKUP_FILE"

  set_property pixels.var.dir "$PIXELS_VAR_DIR"
  set_property metadata.db.driver "$METADATA_DB_DRIVER"
  set_property metadata.db.user "$METADATA_DB_USER"
  set_property metadata.db.password "$METADATA_DB_PASSWORD"
  set_property metadata.db.url "$METADATA_DB_URL"
  set_property metadata.server.port "$METADATA_SERVER_PORT"
  set_property metadata.server.host "$METADATA_SERVER_HOST"
  set_property trans.server.port "$TRANS_SERVER_PORT"
  set_property trans.server.host "$TRANS_SERVER_HOST"
  set_property query.schedule.server.port "$QUERY_SCHEDULE_SERVER_PORT"
  set_property query.schedule.server.host "$QUERY_SCHEDULE_SERVER_HOST"
  set_property etcd.hosts "$ETCD_HOSTS"
  set_property etcd.port "$ETCD_PORT"
  set_property metrics.node.text.dir "$METRICS_NODE_TEXT_DIR"
  set_property presto.pixels.jdbc.url "$PRESTO_PIXELS_JDBC_URL"
  set_property cache.enabled "$CACHE_ENABLED"
}

configure_workers_file() {
  local workers_file="$PIXELS_HOME/etc/workers"
  local worker

  [[ "$CONFIGURE_PIXELS_WORKERS_FILE" == "true" ]] || return 0
  [[ -n "${PIXELS_WORKERS:-}" ]] || return 0

  mkdir -p "$(dirname "$workers_file")"
  if [[ -f "$workers_file" ]]; then
    cp "$workers_file" "$workers_file.bak.$(date '+%Y%m%d%H%M%S')"
  fi

  : > "$workers_file"
  for worker in $PIXELS_WORKERS; do
    printf '%s %s\n' "$worker" "$PIXELS_HOME" >> "$workers_file"
  done
  log "Pixels workers file updated: $workers_file"
}

verify_property() {
  local key="$1"
  local expected="$2"
  local actual

  actual="$(grep -E "^[[:space:]]*${key}=" "$CONFIG_FILE" | tail -n 1 | cut -d= -f2-)"
  [[ "$actual" == "$expected" ]] || fail "$key expected '$expected' but found '$actual'"
}

verify_config() {
  verify_property pixels.var.dir "$PIXELS_VAR_DIR"
  verify_property metadata.db.driver "$METADATA_DB_DRIVER"
  verify_property metadata.db.user "$METADATA_DB_USER"
  verify_property metadata.db.password "$METADATA_DB_PASSWORD"
  verify_property metadata.db.url "$METADATA_DB_URL"
  verify_property metadata.server.port "$METADATA_SERVER_PORT"
  verify_property metadata.server.host "$METADATA_SERVER_HOST"
  verify_property trans.server.port "$TRANS_SERVER_PORT"
  verify_property trans.server.host "$TRANS_SERVER_HOST"
  verify_property query.schedule.server.port "$QUERY_SCHEDULE_SERVER_PORT"
  verify_property query.schedule.server.host "$QUERY_SCHEDULE_SERVER_HOST"
  verify_property etcd.hosts "$ETCD_HOSTS"
  verify_property etcd.port "$ETCD_PORT"
  verify_property metrics.node.text.dir "$METRICS_NODE_TEXT_DIR"
  verify_property presto.pixels.jdbc.url "$PRESTO_PIXELS_JDBC_URL"
  verify_property cache.enabled "$CACHE_ENABLED"

  log "Pixels config updated: $CONFIG_FILE"
}

main() {
  validate_boolean
  validate_metadata_password
  validate_config_file
  configure_pixels
  configure_workers_file
  verify_config
}

main "$@"
