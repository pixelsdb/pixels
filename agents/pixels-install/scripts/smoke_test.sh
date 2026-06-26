#!/usr/bin/env bash
set -euo pipefail

PIXELS_HOME="${PIXELS_HOME:-$HOME/opt/pixels}"
CONFIG_FILE="${PIXELS_CONFIG_FILE:-$PIXELS_HOME/etc/pixels.properties}"
SMOKE_TIMEOUT_SECONDS="${SMOKE_TIMEOUT_SECONDS:-30}"

METADATA_SERVER_PORT="${METADATA_SERVER_PORT:-18888}"
QUERY_SCHEDULE_SERVER_PORT="${QUERY_SCHEDULE_SERVER_PORT:-18893}"
TRANS_SERVER_PORT="${TRANS_SERVER_PORT:-18889}"
ETCD_PORT="${ETCD_PORT:-2379}"
MYSQL_PORT="${MYSQL_PORT:-3306}"

CHECK_TRANS_SERVER="${CHECK_TRANS_SERVER:-false}"
CHECK_ETCD="${CHECK_ETCD:-true}"
CHECK_MYSQL="${CHECK_MYSQL:-true}"
CHECK_PIXELS_CLI="${CHECK_PIXELS_CLI:-true}"

log() {
  printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"
}

warn() {
  printf 'WARN: %s\n' "$*" >&2
}

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "$1 command not found"
}

property_value() {
  local key="$1"

  awk -F= -v key="$key" '
    $1 == key {
      value = $0
      sub(/^[^=]*=/, "", value)
      result = value
    }
    END {
      if (result != "") {
        print result
      }
    }
  ' "$CONFIG_FILE"
}

property_or_default() {
  local key="$1"
  local default_value="$2"
  local value

  value="$(property_value "$key")"
  if [[ -n "$value" ]]; then
    printf '%s\n' "$value"
  else
    printf '%s\n' "$default_value"
  fi
}

first_glob_match() {
  local pattern="$1"
  local matches

  matches="$(compgen -G "$pattern" || true)"
  [[ -n "$matches" ]] || return 1
  printf '%s\n' "$matches" | head -n 1
}

port_is_listening() {
  local host="$1"
  local port="$2"

  if command -v ss >/dev/null 2>&1; then
    if [[ "$host" == "localhost" || "$host" == "127.0.0.1" || "$host" == "0.0.0.0" ]]; then
      ss -ltn | awk '{print $4}' | grep -Eq "(^|:)${port}$"
      return
    fi
  fi

  require_command nc
  nc -z "$host" "$port" >/dev/null 2>&1
}

wait_for_port() {
  local name="$1"
  local host="$2"
  local port="$3"
  local deadline

  deadline=$((SECONDS + SMOKE_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    if port_is_listening "$host" "$port"; then
      log "$name is reachable at $host:$port"
      return
    fi
    sleep 2
  done

  fail "$name is not reachable at $host:$port within ${SMOKE_TIMEOUT_SECONDS}s"
}

verify_filesystem() {
  [[ -d "$PIXELS_HOME" ]] || fail "PIXELS_HOME does not exist: $PIXELS_HOME"
  [[ -d "$PIXELS_HOME/bin" ]] || fail "missing directory: $PIXELS_HOME/bin"
  [[ -d "$PIXELS_HOME/sbin" ]] || fail "missing directory: $PIXELS_HOME/sbin"
  [[ -d "$PIXELS_HOME/etc" ]] || fail "missing directory: $PIXELS_HOME/etc"
  [[ -f "$CONFIG_FILE" ]] || fail "missing Pixels config file: $CONFIG_FILE"

  first_glob_match "$PIXELS_HOME/bin/pixels-daemon-*-full.jar" >/dev/null || fail "missing pixels-daemon full jar in $PIXELS_HOME/bin"
  first_glob_match "$PIXELS_HOME/sbin/pixels-cli-*-full.jar" >/dev/null || fail "missing pixels-cli full jar in $PIXELS_HOME/sbin"

  log "Pixels installation layout looks valid: $PIXELS_HOME"
}

verify_config_property() {
  local key="$1"
  local value

  value="$(property_value "$key")"
  [[ -n "$value" ]] || fail "missing or empty config property: $key"
  log "config property found: $key=$value"
}

verify_config() {
  verify_config_property pixels.var.dir
  verify_config_property metadata.db.driver
  verify_config_property metadata.db.user
  verify_config_property metadata.db.password
  verify_config_property metadata.db.url
  verify_config_property metadata.server.host
  verify_config_property metadata.server.port
  verify_config_property query.schedule.server.host
  verify_config_property query.schedule.server.port
  verify_config_property etcd.hosts
  verify_config_property etcd.port
  verify_config_property cache.enabled
}

verify_java() {
  require_command java
  java -version >/dev/null 2>&1 || fail "java is installed but cannot run"
  log "Java runtime is available"
}

verify_core_services() {
  local metadata_host
  local query_schedule_host
  local trans_host
  local metadata_port
  local query_schedule_port
  local trans_port

  metadata_host="$(property_or_default metadata.server.host localhost)"
  query_schedule_host="$(property_or_default query.schedule.server.host localhost)"
  trans_host="$(property_or_default trans.server.host localhost)"
  metadata_port="$(property_or_default metadata.server.port "$METADATA_SERVER_PORT")"
  query_schedule_port="$(property_or_default query.schedule.server.port "$QUERY_SCHEDULE_SERVER_PORT")"
  trans_port="$(property_or_default trans.server.port "$TRANS_SERVER_PORT")"

  wait_for_port "metadata server" "$metadata_host" "$metadata_port"
  wait_for_port "query schedule server" "$query_schedule_host" "$query_schedule_port"

  if [[ "$CHECK_TRANS_SERVER" == "true" ]]; then
    wait_for_port "transaction server" "$trans_host" "$trans_port"
  else
    log "transaction server port check skipped; set CHECK_TRANS_SERVER=true to enable it"
  fi
}

verify_etcd() {
  local etcd_hosts
  local etcd_host
  local etcd_port

  if [[ "$CHECK_ETCD" != "true" ]]; then
    log "etcd check skipped"
    return
  fi

  etcd_hosts="$(property_or_default etcd.hosts localhost)"
  etcd_host="${etcd_hosts%%,*}"
  etcd_host="${etcd_host%%;*}"
  etcd_host="${etcd_host%%:*}"
  etcd_port="$(property_or_default etcd.port "$ETCD_PORT")"

  wait_for_port "etcd" "$etcd_host" "$etcd_port"
}

verify_mysql() {
  local metadata_db_url
  local mysql_host
  local mysql_port

  if [[ "$CHECK_MYSQL" != "true" ]]; then
    log "MySQL check skipped"
    return
  fi

  metadata_db_url="$(property_value metadata.db.url)"
  mysql_host="$(printf '%s\n' "$metadata_db_url" | sed -nE 's#^jdbc:mysql://([^:/?]+).*#\1#p')"
  mysql_port="$(printf '%s\n' "$metadata_db_url" | sed -nE 's#^jdbc:mysql://[^:/?]+:([0-9]+).*#\1#p')"
  mysql_host="${mysql_host:-localhost}"
  mysql_port="${mysql_port:-$MYSQL_PORT}"

  wait_for_port "MySQL" "$mysql_host" "$mysql_port"
}

verify_pixels_cli() {
  local cli_jar

  if [[ "$CHECK_PIXELS_CLI" != "true" ]]; then
    log "Pixels CLI check skipped"
    return
  fi

  cli_jar="$(first_glob_match "$PIXELS_HOME/sbin/pixels-cli-*-full.jar")"
  timeout 10s java -jar "$cli_jar" --help >/dev/null 2>&1 || {
    warn "pixels-cli --help did not complete successfully; jar exists but interactive CLI may not support --help"
    return
  }

  log "Pixels CLI jar can be executed"
}

main() {
  export PIXELS_HOME
  verify_filesystem
  verify_config
  verify_java
  verify_core_services
  verify_etcd
  verify_mysql
  verify_pixels_cli
  log "Pixels smoke test passed"
}

main "$@"
