#!/usr/bin/env bash
set -uo pipefail

# Verifies the installed layout, configuration, metadata access, etcd
# health, and basic CLI behavior when applicable. Runs every check and
# reports a structured summary at the end instead of stopping at the first
# failure (no `set -e`/hard `exit` inside individual checks), so the skill
# can see every problem in one pass - e.g. "etcd is down AND the metadata
# DB url is missing" - instead of fixing one, re-running, and discovering
# the next.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/shell_env.sh
source "$SCRIPT_DIR/lib/shell_env.sh"
load_toolchain_env

SKILL_DIR="${SKILL_DIR:-$(skill_dir)}"
STATE_DIR="${STATE_DIR:-$(state_dir)}"
DEPLOYMENT_FILE="${DEPLOYMENT_FILE:-$STATE_DIR/deployment.env}"
TRINO_DEPLOYMENT_FILE="${TRINO_DEPLOYMENT_FILE:-$STATE_DIR/trino-deployment.env}"
if [[ -f "$DEPLOYMENT_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$DEPLOYMENT_FILE"
  set +a
fi
if [[ -f "$TRINO_DEPLOYMENT_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$TRINO_DEPLOYMENT_FILE"
  set +a
fi

PIXELS_HOME="${PIXELS_HOME:-$HOME/opt/pixels}"
CONFIG_FILE="${PIXELS_CONFIG_FILE:-$PIXELS_HOME/etc/pixels.properties}"
SMOKE_TIMEOUT_SECONDS="${SMOKE_TIMEOUT_SECONDS:-30}"
TRINO_HOME_LINK="${TRINO_HOME_LINK:-$HOME/opt/trino-server}"

METADATA_SERVER_PORT="${METADATA_SERVER_PORT:-18888}"
QUERY_SCHEDULE_SERVER_PORT="${QUERY_SCHEDULE_SERVER_PORT:-18893}"
TRANS_SERVER_PORT="${TRANS_SERVER_PORT:-18889}"
ETCD_PORT="${ETCD_PORT:-2379}"
MYSQL_PORT="${MYSQL_PORT:-3306}"

CHECK_TRANS_SERVER="${CHECK_TRANS_SERVER:-false}"
CHECK_ETCD="${CHECK_ETCD:-true}"
CHECK_MYSQL="${CHECK_MYSQL:-true}"
CHECK_PIXELS_CLI="${CHECK_PIXELS_CLI:-true}"
CHECK_TRINO="${CHECK_TRINO:-false}"

require_command() {
  command -v "$1" >/dev/null 2>&1 || { printf 'ERROR: %s command not found\n' "$1" >&2; exit 1; }
}

property_value() {
  local key="$1"

  [[ -f "$CONFIG_FILE" ]] || return 1

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

# Polls for a listening port and records ok/fail - never exits the script,
# so one unreachable service doesn't hide whether the others are up too.
check_port_reachable() {
  local check_name="$1"
  local display_name="$2"
  local host="$3"
  local port="$4"
  local deadline

  deadline=$((SECONDS + SMOKE_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    if port_is_listening "$host" "$port"; then
      result_record "$check_name" ok "$display_name reachable at $host:$port"
      return
    fi
    sleep 2
  done

  result_record "$check_name" fail "$display_name not reachable at $host:$port within ${SMOKE_TIMEOUT_SECONDS}s"
}

verify_filesystem() {
  if [[ ! -d "$PIXELS_HOME" ]]; then
    result_record fs:pixels_home fail "PIXELS_HOME does not exist: $PIXELS_HOME"
    return
  fi
  result_record fs:pixels_home ok "$PIXELS_HOME"

  local dir
  for dir in bin sbin etc; do
    if [[ -d "$PIXELS_HOME/$dir" ]]; then
      result_record "fs:$dir" ok "present"
    else
      result_record "fs:$dir" fail "missing directory: $PIXELS_HOME/$dir"
    fi
  done

  if [[ -f "$CONFIG_FILE" ]]; then
    result_record fs:config ok "$CONFIG_FILE"
  else
    result_record fs:config fail "missing Pixels config file: $CONFIG_FILE"
  fi

  if first_glob_match "$PIXELS_HOME/bin/pixels-daemon-*-full.jar" >/dev/null; then
    result_record fs:daemon_jar ok "present"
  else
    result_record fs:daemon_jar fail "missing pixels-daemon full jar in $PIXELS_HOME/bin"
  fi

  if first_glob_match "$PIXELS_HOME/sbin/pixels-cli-*-full.jar" >/dev/null; then
    result_record fs:cli_jar ok "present"
  else
    result_record fs:cli_jar fail "missing pixels-cli full jar in $PIXELS_HOME/sbin"
  fi
}

verify_pixels_topology() {
  local worker workers_file missing=0 worker_count=0

  if [[ ! -f "$DEPLOYMENT_FILE" ]]; then
    result_record pixels_topology skip "deployment.env not found; run prepare_deployment.sh to record single-node/cluster topology"
    return
  fi

  result_record pixels_topology ok "mode=${PIXELS_DEPLOYMENT_MODE:-unknown} coordinator=${PIXELS_COORDINATOR_NAME:-${COORDINATOR_HOST:-unknown}} workers=${PIXELS_WORKERS:-unknown}"

  workers_file="$PIXELS_HOME/etc/workers"
  if [[ ! -f "$workers_file" ]]; then
    result_record pixels_workers_file fail "missing workers file: $workers_file"
    return
  fi

  for worker in ${PIXELS_WORKERS:-}; do
    ((worker_count += 1))
    if ! awk -v worker="$worker" '$1 == worker { found = 1 } END { exit(found ? 0 : 1) }' "$workers_file"; then
      result_record "pixels_worker:$worker" fail "not listed in $workers_file"
      missing=1
    fi
  done

  if (( worker_count == 0 )); then
    result_record pixels_workers_file fail "PIXELS_WORKERS is empty in $DEPLOYMENT_FILE"
  elif (( missing == 0 )); then
    result_record pixels_workers_file ok "$workers_file contains $worker_count worker(s)"
  fi
}

verify_config_property() {
  local key="$1"
  local value

  value="$(property_value "$key")"
  if [[ -n "$value" ]]; then
    result_record "config:$key" ok "$value"
  else
    result_record "config:$key" fail "missing or empty config property"
  fi
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
  if ! command -v java >/dev/null 2>&1; then
    result_record java fail "java command not found"
    return
  fi
  if java -version >/dev/null 2>&1; then
    result_record java ok "Java runtime is available"
  else
    result_record java fail "java is installed but cannot run"
  fi
}

verify_core_services() {
  local metadata_host query_schedule_host trans_host
  local metadata_port query_schedule_port trans_port

  metadata_host="$(property_or_default metadata.server.host localhost)"
  query_schedule_host="$(property_or_default query.schedule.server.host localhost)"
  trans_host="$(property_or_default trans.server.host localhost)"
  metadata_port="$(property_or_default metadata.server.port "$METADATA_SERVER_PORT")"
  query_schedule_port="$(property_or_default query.schedule.server.port "$QUERY_SCHEDULE_SERVER_PORT")"
  trans_port="$(property_or_default trans.server.port "$TRANS_SERVER_PORT")"

  check_port_reachable service:metadata "metadata server" "$metadata_host" "$metadata_port"
  check_port_reachable service:query_schedule "query schedule server" "$query_schedule_host" "$query_schedule_port"

  if [[ "$CHECK_TRANS_SERVER" == "true" ]]; then
    check_port_reachable service:trans "transaction server" "$trans_host" "$trans_port"
  else
    result_record service:trans skip "set CHECK_TRANS_SERVER=true to enable"
  fi
}

verify_etcd() {
  if [[ "$CHECK_ETCD" != "true" ]]; then
    result_record etcd skip "set CHECK_ETCD=true to enable"
    return
  fi

  local etcd_hosts etcd_host etcd_port
  etcd_hosts="$(property_or_default etcd.hosts localhost)"
  etcd_host="${etcd_hosts%%,*}"
  etcd_host="${etcd_host%%;*}"
  etcd_host="${etcd_host%%:*}"
  etcd_port="$(property_or_default etcd.port "$ETCD_PORT")"

  check_port_reachable etcd "etcd" "$etcd_host" "$etcd_port"
}

verify_mysql() {
  if [[ "$CHECK_MYSQL" != "true" ]]; then
    result_record mysql skip "set CHECK_MYSQL=true to enable"
    return
  fi

  local metadata_db_url mysql_host mysql_port
  metadata_db_url="$(property_value metadata.db.url)"
  mysql_host="$(printf '%s\n' "$metadata_db_url" | sed -nE 's#^jdbc:mysql://([^:/?]+).*#\1#p')"
  mysql_port="$(printf '%s\n' "$metadata_db_url" | sed -nE 's#^jdbc:mysql://[^:/?]+:([0-9]+).*#\1#p')"
  mysql_host="${mysql_host:-localhost}"
  mysql_port="${mysql_port:-$MYSQL_PORT}"

  check_port_reachable mysql "MySQL" "$mysql_host" "$mysql_port"
}

verify_pixels_cli() {
  if [[ "$CHECK_PIXELS_CLI" != "true" ]]; then
    result_record pixels_cli skip "set CHECK_PIXELS_CLI=true to enable"
    return
  fi

  local cli_jar
  cli_jar="$(first_glob_match "$PIXELS_HOME/sbin/pixels-cli-*-full.jar")"
  if [[ -z "$cli_jar" ]]; then
    result_record pixels_cli fail "no pixels-cli jar found under $PIXELS_HOME/sbin"
    return
  fi

  if timeout 10s java -jar "$cli_jar" --help >/dev/null 2>&1; then
    result_record pixels_cli ok "jar can be executed"
  else
    result_record pixels_cli warn "pixels-cli --help did not complete successfully; jar exists but interactive CLI may not support --help"
  fi
}

verify_trino() {
  if [[ "$CHECK_TRINO" != "true" ]]; then
    result_record trino skip "set CHECK_TRINO=true to enable"
    return
  fi

  if [[ ! -d "$TRINO_HOME_LINK" ]]; then
    result_record trino_home fail "TRINO_HOME_LINK does not exist: $TRINO_HOME_LINK"
    return
  fi
  result_record trino_home ok "$TRINO_HOME_LINK"

  if [[ -x "$TRINO_HOME_LINK/bin/launcher" ]]; then
    result_record trino_launcher ok "present"
  else
    result_record trino_launcher fail "missing launcher under $TRINO_HOME_LINK/bin"
  fi

  if [[ -f "$TRINO_HOME_LINK/etc/config.properties" ]]; then
    result_record trino_config ok "$TRINO_HOME_LINK/etc/config.properties"
  else
    result_record trino_config fail "missing $TRINO_HOME_LINK/etc/config.properties"
  fi

  if [[ -f "$TRINO_HOME_LINK/etc/catalog/pixels.properties" ]]; then
    result_record trino_pixels_catalog ok "$TRINO_HOME_LINK/etc/catalog/pixels.properties"
  else
    result_record trino_pixels_catalog fail "missing Trino pixels catalog config"
  fi
}

main() {
  export PIXELS_HOME
  result_reset

  verify_filesystem
  verify_pixels_topology
  verify_config
  verify_java
  verify_core_services
  verify_etcd
  verify_mysql
  verify_pixels_cli
  verify_trino

  result_emit_summary smoke_test
}

main "$@"
