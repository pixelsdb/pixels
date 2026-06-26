#!/usr/bin/env bash
set -euo pipefail

PIXELS_HOME="${PIXELS_HOME:-$HOME/opt/pixels}"
CONFIG_FILE="${PIXELS_CONFIG_FILE:-$PIXELS_HOME/etc/pixels.properties}"
BACKUP_FILE="${BACKUP_FILE:-$CONFIG_FILE.bak.$(date '+%Y%m%d%H%M%S')}"

PIXELS_VAR_DIR="${PIXELS_VAR_DIR:-$PIXELS_HOME/var/}"
METADATA_DB_DRIVER="${METADATA_DB_DRIVER:-com.mysql.cj.jdbc.Driver}"
METADATA_DB_USER="${METADATA_DB_USER:-pixels}"
METADATA_DB_PASSWORD="${METADATA_DB_PASSWORD:-password}"
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
  validate_config_file
  configure_pixels
  verify_config
}

main "$@"
