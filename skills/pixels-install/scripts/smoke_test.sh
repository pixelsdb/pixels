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
CONFIG_FILE="${PIXELS_CONFIG_FILE:-${PIXELS_CONFIG:-$PIXELS_HOME/etc/pixels.properties}}"
SMOKE_TIMEOUT_SECONDS="${SMOKE_TIMEOUT_SECONDS:-30}"
TRINO_HOME_LINK="${TRINO_HOME_LINK:-$HOME/opt/trino-server}"
TRINO_HTTP_PORT="${TRINO_HTTP_PORT:-8080}"
TRINO_PIXELS_HOME="${TRINO_PIXELS_HOME:-${PIXELS_HOME:-$HOME/opt/pixels}}"
TRINO_PIXELS_CONFIG="${TRINO_PIXELS_CONFIG:-${PIXELS_CONFIG:-$TRINO_PIXELS_HOME/etc/pixels.properties}}"
TRINO_PIXELS_ENV_FILE="${TRINO_PIXELS_ENV_FILE:-$HOME/.pixels-trino-env.sh}"
PIXELS_LOG_DIR="${PIXELS_LOG_DIR:-$PIXELS_HOME/logs}"
PIXELS_LOG_ERROR_PATTERN="${PIXELS_LOG_ERROR_PATTERN:-ERROR|FATAL|Exception|OutOfMemoryError}"
PIXELS_LOG_IGNORE_PATTERN="${PIXELS_LOG_IGNORE_PATTERN:-}"
PIXELS_LOG_MAX_MATCHES="${PIXELS_LOG_MAX_MATCHES:-20}"
TRINO_CATALOG="${TRINO_CATALOG:-pixels}"
TRINO_CLI_QUERY="${TRINO_CLI_QUERY:-SHOW SCHEMAS}"
TRINO_CLI_TIMEOUT_SECONDS="${TRINO_CLI_TIMEOUT_SECONDS:-30}"
TRINO_CLI_OUTPUT_FILE="${TRINO_CLI_OUTPUT_FILE:-$STATE_DIR/logs/trino_show_schemas.out}"
TRINO_CLI_ERROR_FILE="${TRINO_CLI_ERROR_FILE:-$STATE_DIR/logs/trino_show_schemas.err}"
SSH_USER="${SSH_USER:-${TRINO_SSH_USER:-root}}"
SSH_PORT="${SSH_PORT:-${TRINO_SSH_PORT:-}}"

METADATA_SERVER_PORT="${METADATA_SERVER_PORT:-18888}"
QUERY_SCHEDULE_SERVER_PORT="${QUERY_SCHEDULE_SERVER_PORT:-18893}"
TRANS_SERVER_PORT="${TRANS_SERVER_PORT:-18889}"
ETCD_PORT="${ETCD_PORT:-2379}"
MYSQL_PORT="${MYSQL_PORT:-3306}"

CHECK_TRANS_SERVER="${CHECK_TRANS_SERVER:-false}"
CHECK_CORE_SERVICES="${CHECK_CORE_SERVICES:-true}"
CHECK_JAVA="${CHECK_JAVA:-true}"
CHECK_ETCD="${CHECK_ETCD:-true}"
CHECK_MYSQL="${CHECK_MYSQL:-true}"
CHECK_PIXELS_CLI="${CHECK_PIXELS_CLI:-true}"
CHECK_TRINO="${CHECK_TRINO:-false}"
CHECK_PIXELS_LAYOUT="${CHECK_PIXELS_LAYOUT:-true}"
CHECK_TRINO_PIXELS_CLIENT="${CHECK_TRINO_PIXELS_CLIENT:-$CHECK_TRINO}"
CHECK_PIXELS_LOGS="${CHECK_PIXELS_LOGS:-true}"
CHECK_TRINO_CLUSTER_STATUS="${CHECK_TRINO_CLUSTER_STATUS:-$CHECK_TRINO}"
CHECK_TRINO_CLI="${CHECK_TRINO_CLI:-$CHECK_TRINO}"

require_command() {
  command -v "$1" >/dev/null 2>&1 || { printf 'ERROR: %s command not found\n' "$1" >&2; exit 1; }
}

property_value() {
  local key="$1"

  property_value_from_file "$CONFIG_FILE" "$key"
}

property_value_from_file() {
  local file="$1"
  local key="$2"

  [[ -f "$file" ]] || return 1

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
  ' "$file"
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

remote_spec() {
  local target="$1"
  if [[ -n "$SSH_USER" && "$target" != *@* ]]; then
    printf '%s@%s' "$SSH_USER" "$target"
  else
    printf '%s' "$target"
  fi
}

ssh_opts() {
  local -a opts=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
  [[ -n "$SSH_PORT" ]] && opts+=(-p "$SSH_PORT")
  printf '%s\n' "${opts[@]}"
}

verify_filesystem() {
  if [[ "$CHECK_PIXELS_LAYOUT" != "true" ]]; then
    result_record fs:pixels_layout skip "set CHECK_PIXELS_LAYOUT=true to require a full Pixels runtime layout"
    return
  fi

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

  if [[ "$CHECK_PIXELS_LAYOUT" != "true" ]]; then
    result_record pixels_topology skip "Trino-side Pixels client mode; full Pixels topology layout not required"
    return
  fi

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
  verify_config_property trans.server.host
  verify_config_property trans.server.port
  verify_config_property query.schedule.server.host
  verify_config_property query.schedule.server.port
  verify_config_property etcd.hosts
  verify_config_property etcd.port
  verify_config_property cache.enabled
}

verify_java() {
  if [[ "$CHECK_JAVA" != "true" ]]; then
    result_record java skip "set CHECK_JAVA=true to enable"
    return
  fi

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
  if [[ "$CHECK_CORE_SERVICES" != "true" ]]; then
    result_record service:core skip "set CHECK_CORE_SERVICES=true to enable"
    return
  fi

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
  if [[ "$CHECK_PIXELS_LAYOUT" != "true" ]]; then
    result_record pixels_cli skip "Trino-side Pixels client mode; pixels-cli jar is not required"
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

verify_pixels_logs() {
  if [[ "$CHECK_PIXELS_LOGS" != "true" ]]; then
    result_record pixels_logs skip "set CHECK_PIXELS_LOGS=true to enable"
    return
  fi
  if [[ "$CHECK_PIXELS_LAYOUT" != "true" ]]; then
    result_record pixels_logs skip "Trino-side Pixels client mode; full Pixels logs are not required"
    return
  fi

  if [[ ! -d "$PIXELS_LOG_DIR" ]]; then
    result_record pixels_logs warn "logs directory not found: $PIXELS_LOG_DIR"
    return
  fi

  local matches filtered
  matches="$(
    find "$PIXELS_LOG_DIR" -maxdepth 3 -type f -print0 2>/dev/null |
      xargs -0 grep -IEn "$PIXELS_LOG_ERROR_PATTERN" 2>/dev/null |
      head -n "$PIXELS_LOG_MAX_MATCHES" || true
  )"

  if [[ -n "$matches" && -n "$PIXELS_LOG_IGNORE_PATTERN" ]]; then
    filtered="$(printf '%s\n' "$matches" | grep -Ev "$PIXELS_LOG_IGNORE_PATTERN" || true)"
  else
    filtered="$matches"
  fi

  if [[ -n "$filtered" ]]; then
    result_record pixels_logs fail "error pattern found in $PIXELS_LOG_DIR (first matches: $(printf '%s\n' "$filtered" | paste -sd ' ' -))"
  else
    result_record pixels_logs ok "no matches for /$PIXELS_LOG_ERROR_PATTERN/ under $PIXELS_LOG_DIR"
  fi
}

verify_trino_pixels_client() {
  if [[ "$CHECK_TRINO_PIXELS_CLIENT" != "true" ]]; then
    result_record trino_pixels_client skip "set CHECK_TRINO_PIXELS_CLIENT=true to enable"
    return
  fi

  local key value
  local -a required_keys=(
    metadata.server.host
    metadata.server.port
    trans.server.host
    trans.server.port
    query.schedule.server.host
    query.schedule.server.port
    etcd.hosts
    etcd.port
  )

  case "$TRINO_PIXELS_HOME" in
    */etc|*/etc/)
      result_record trino_pixels_home fail "TRINO_PIXELS_HOME/PIXELS_HOME must be the parent directory (for example ~/opt/pixels), not the etc directory: $TRINO_PIXELS_HOME"
      ;;
    *)
      if [[ -d "$TRINO_PIXELS_HOME" ]]; then
        result_record trino_pixels_home ok "$TRINO_PIXELS_HOME"
      else
        result_record trino_pixels_home fail "missing Trino-side Pixels home directory: $TRINO_PIXELS_HOME"
      fi
      ;;
  esac

  if [[ -f "$TRINO_PIXELS_CONFIG" ]]; then
    result_record trino_pixels_config ok "$TRINO_PIXELS_CONFIG"
  else
    result_record trino_pixels_config fail "missing Trino-side Pixels client config: $TRINO_PIXELS_CONFIG"
    return
  fi

  if [[ -f "$TRINO_PIXELS_ENV_FILE" ]]; then
    result_record trino_pixels_env ok "$TRINO_PIXELS_ENV_FILE"
    if grep -qE '^export PIXELS_HOME=' "$TRINO_PIXELS_ENV_FILE" &&
       grep -qE '^export PIXELS_CONFIG=' "$TRINO_PIXELS_ENV_FILE"; then
      result_record trino_pixels_env_exports ok "PIXELS_HOME and PIXELS_CONFIG are exported"
    else
      result_record trino_pixels_env_exports fail "missing PIXELS_HOME or PIXELS_CONFIG export in $TRINO_PIXELS_ENV_FILE"
    fi
  else
    result_record trino_pixels_env warn "missing $TRINO_PIXELS_ENV_FILE; direct launcher use may not inherit PIXELS_HOME/PIXELS_CONFIG"
  fi

  for key in "${required_keys[@]}"; do
    value="$(property_value_from_file "$TRINO_PIXELS_CONFIG" "$key")"
    if [[ -n "$value" ]]; then
      result_record "trino_pixels_config:$key" ok "$value"
      case "$key" in
        metadata.server.host|trans.server.host|query.schedule.server.host|etcd.hosts)
          if [[ "$value" == "localhost" || "$value" == "127.0.0.1" ]]; then
            result_record "trino_pixels_config:${key}:localhost" warn "$key is $value; this is only valid when the Pixels service runs on the same Trino node"
          fi
          ;;
      esac
    else
      result_record "trino_pixels_config:$key" fail "missing or empty config property"
    fi
  done
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
    if grep -qE '^connector\.name=pixels$' "$TRINO_HOME_LINK/etc/catalog/pixels.properties"; then
      result_record trino_pixels_catalog_connector ok "connector.name=pixels"
    else
      result_record trino_pixels_catalog_connector fail "missing connector.name=pixels in Trino catalog"
    fi
    if grep -qE '^(metadata|trans|etcd|query\.schedule)\.' "$TRINO_HOME_LINK/etc/catalog/pixels.properties"; then
      result_record trino_pixels_catalog_scope fail "Trino catalog contains Pixels runtime properties; etc/catalog/pixels.properties is not PIXELS_HOME/etc/pixels.properties"
    else
      result_record trino_pixels_catalog_scope ok "catalog config is separate from Pixels runtime config"
    fi
  else
    result_record trino_pixels_catalog fail "missing Trino pixels catalog config"
  fi

  local connector_dir listener_dir
  connector_dir="$(find "$TRINO_HOME_LINK/plugin" -mindepth 1 -maxdepth 1 -type d -name 'pixels-trino-connector*' -print -quit 2>/dev/null || true)"
  listener_dir="$(find "$TRINO_HOME_LINK/plugin" -mindepth 1 -maxdepth 1 -type d -name 'pixels-trino-listener*' -print -quit 2>/dev/null || true)"

  if [[ -n "$connector_dir" ]]; then
    result_record trino_pixels_connector ok "$connector_dir"
  else
    result_record trino_pixels_connector fail "missing pixels-trino connector plugin"
  fi

  if [[ -n "$listener_dir" ]]; then
    result_record trino_pixels_listener ok "$listener_dir"
  else
    result_record trino_pixels_listener fail "missing pixels-trino listener plugin"
  fi

  if [[ -f "$TRINO_HOME_LINK/etc/event-listener.properties" ]]; then
    result_record trino_event_listener_config ok "$TRINO_HOME_LINK/etc/event-listener.properties"
  else
    result_record trino_event_listener_config fail "missing Trino pixels event listener config"
  fi
}

verify_trino_cluster_status() {
  if [[ "$CHECK_TRINO_CLUSTER_STATUS" != "true" ]]; then
    result_record trino_cluster_status skip "set CHECK_TRINO_CLUSTER_STATUS=true to enable"
    return
  fi

  if [[ ! -x "$TRINO_HOME_LINK/bin/launcher" ]]; then
    result_record trino_status:${TRINO_COORDINATOR_NAME:-coordinator} fail "missing local launcher: $TRINO_HOME_LINK/bin/launcher"
  elif "$TRINO_HOME_LINK/bin/launcher" status >/dev/null 2>&1; then
    result_record trino_status:${TRINO_COORDINATOR_NAME:-coordinator} ok "local launcher reports running"
  else
    result_record trino_status:${TRINO_COORDINATOR_NAME:-coordinator} fail "local launcher status failed"
  fi

  local -a targets=()
  local -a names=()
  local -a ssh_args=()
  local i target name output

  read -r -a targets <<< "${TRINO_WORKER_SSH_TARGETS:-}"
  read -r -a names <<< "${TRINO_WORKER_NAMES:-}"
  if [[ "${#targets[@]}" -eq 0 ]]; then
    result_record trino_worker_status skip "no Trino workers recorded in trino-deployment.env"
    return
  fi

  while IFS= read -r opt; do ssh_args+=("$opt"); done < <(ssh_opts)
  for ((i = 0; i < ${#targets[@]}; i++)); do
    target="${targets[$i]}"
    name="${names[$i]:-worker-$i}"
    if output="$(ssh "${ssh_args[@]}" "$(remote_spec "$target")" "[ -f ~/.pixels-trino-env.sh ] && . ~/.pixels-trino-env.sh; '$TRINO_HOME_LINK/bin/launcher' status" 2>&1)"; then
      result_record "trino_status:$name" ok "remote launcher reports running"
    else
      result_record "trino_status:$name" fail "remote launcher status failed on $target: $(printf '%s' "$output" | tail -n 1)"
    fi
  done
}

verify_trino_cli_show_schemas() {
  if [[ "$CHECK_TRINO_CLI" != "true" ]]; then
    result_record trino_cli skip "set CHECK_TRINO_CLI=true to enable"
    return
  fi

  if [[ ! -x "$TRINO_HOME_LINK/bin/trino" ]]; then
    result_record trino_cli fail "missing trino CLI: $TRINO_HOME_LINK/bin/trino"
    return
  fi

  mkdir -p "$(dirname "$TRINO_CLI_OUTPUT_FILE")"
  if (
    [[ -f "$TRINO_PIXELS_ENV_FILE" ]] && source "$TRINO_PIXELS_ENV_FILE"
    if command -v timeout >/dev/null 2>&1; then
      timeout "${TRINO_CLI_TIMEOUT_SECONDS}s" "$TRINO_HOME_LINK/bin/trino" \
        --server "localhost:$TRINO_HTTP_PORT" \
        --catalog "$TRINO_CATALOG" \
        --execute "$TRINO_CLI_QUERY"
    else
      "$TRINO_HOME_LINK/bin/trino" \
        --server "localhost:$TRINO_HTTP_PORT" \
        --catalog "$TRINO_CATALOG" \
        --execute "$TRINO_CLI_QUERY"
    fi
  ) >"$TRINO_CLI_OUTPUT_FILE" 2>"$TRINO_CLI_ERROR_FILE"; then
    result_record trino_cli_show_schemas ok "$TRINO_CLI_QUERY succeeded via catalog $TRINO_CATALOG (output: $TRINO_CLI_OUTPUT_FILE)"
  else
    result_record trino_cli_show_schemas fail "$TRINO_CLI_QUERY failed (stderr: $(tail -n 1 "$TRINO_CLI_ERROR_FILE" 2>/dev/null))"
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
  verify_pixels_logs
  verify_trino_pixels_client
  verify_trino
  verify_trino_cluster_status
  verify_trino_cli_show_schemas

  result_emit_summary smoke_test
}

main "$@"
