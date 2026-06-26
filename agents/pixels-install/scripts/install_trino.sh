#!/usr/bin/env bash
set -euo pipefail

# Installs Trino (per https://trino.io/docs/466/installation/deployment.html)
# on the current node and installs the pixels-trino connector/listener
# (per https://github.com/pixelsdb/pixels-trino) into it. Run this once per
# node in the cluster (coordinator and every worker) - this script does not
# SSH into other nodes itself; prepare_trino_cluster.sh only sets up
# passwordless SSH so the optional install_trino_shell_helpers.sh can later
# drive start/stop/restart across the cluster.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/shell_env.sh
source "$SCRIPT_DIR/lib/shell_env.sh"
load_toolchain_env

AGENT_DIR="${AGENT_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
REPO_ROOT="${REPO_ROOT:-$(cd "$AGENT_DIR/../.." && pwd)}"

# Picks up the cluster topology written by prepare_trino_cluster.sh, if any.
# Explicit env vars passed to this script still take precedence.
TRINO_DEPLOYMENT_FILE="${TRINO_DEPLOYMENT_FILE:-$AGENT_DIR/trino-deployment.env}"
if [[ -f "$TRINO_DEPLOYMENT_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$TRINO_DEPLOYMENT_FILE"
  set +a
fi

TRINO_VERSION="${TRINO_VERSION:-466}"
TRINO_INSTALL_PARENT="${TRINO_INSTALL_PARENT:-$HOME/opt}"
TRINO_INSTALL_DIR="${TRINO_INSTALL_DIR:-$TRINO_INSTALL_PARENT/trino-server-$TRINO_VERSION}"
TRINO_HOME_LINK="${TRINO_HOME_LINK:-$TRINO_INSTALL_PARENT/trino-server}"
TRINO_DOWNLOAD_URL="${TRINO_DOWNLOAD_URL:-https://repo1.maven.org/maven2/io/trino/trino-server/$TRINO_VERSION/trino-server-$TRINO_VERSION.tar.gz}"
TMP_DIR="${TMP_DIR:-/tmp}"

# node.data-dir lives under the install *parent* (~/opt), not inside the
# versioned/symlinked trino-server-<version> directory, so it survives
# `change_trino_version`-style symlink swaps across upgrades - matching the
# official recommendation to keep the data dir outside the install dir.
TRINO_DATA_DIR="${TRINO_DATA_DIR:-$TRINO_INSTALL_PARENT/var/trino/data}"
TRINO_ENVIRONMENT="${TRINO_ENVIRONMENT:-production}"
TRINO_HTTP_PORT="${TRINO_HTTP_PORT:-8080}"

# Which role this node plays. Left empty, we try to infer it by matching
# this host's IP addresses against TRINO_COORDINATOR_HOST; otherwise set
# TRINO_ROLE=coordinator|worker explicitly.
TRINO_ROLE="${TRINO_ROLE:-}"
TRINO_COORDINATOR_HOST="${TRINO_COORDINATOR_HOST:-}"
TRINO_COORDINATOR_IS_WORKER="${TRINO_COORDINATOR_IS_WORKER:-false}"

PIXELS_HOME="${PIXELS_HOME:-$HOME/opt/pixels}"
PIXELS_TRINO_DIR="${PIXELS_TRINO_DIR:-$HOME/pixels-trino}"
PIXELS_TRINO_REPO_URL="${PIXELS_TRINO_REPO_URL:-https://github.com/pixelsdb/pixels-trino.git}"
INSTALL_PIXELS_TRINO_PLUGIN="${INSTALL_PIXELS_TRINO_PLUGIN:-true}"
# cloud.function.switch in etc/catalog/pixels.properties: off/on/auto/session.
# Pixels-Turbo (serverless pushdown) is out of this agent's scope, so this
# stays "off" unless the user explicitly asks for Pixels Turbo.
CLOUD_FUNCTION_SWITCH="${CLOUD_FUNCTION_SWITCH:-off}"
# The event listener is explicitly marked optional ("*") in pixels-trino's
# README; default it off.
INSTALL_PIXELS_TRINO_LISTENER="${INSTALL_PIXELS_TRINO_LISTENER:-false}"
INSTALL_TRINO_CLI="${INSTALL_TRINO_CLI:-true}"
TRINO_CLI_DOWNLOAD_URL="${TRINO_CLI_DOWNLOAD_URL:-https://repo1.maven.org/maven2/io/trino/trino-cli/$TRINO_VERSION/trino-cli-$TRINO_VERSION-executable.jar}"
ASSUME_YES="${ASSUME_YES:-false}"
CONFIRM_TRINO_INSTALL="${CONFIRM_TRINO_INSTALL:-}"

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

require_java() {
  local derived_home

  if [[ -z "${JAVA_HOME:-}" || ! -x "${JAVA_HOME:-}/bin/java" ]]; then
    derived_home="$(locate_existing_java_home || true)"
    [[ -n "$derived_home" ]] && export JAVA_HOME="$derived_home"
  fi

  [[ -n "${JAVA_HOME:-}" && -x "$JAVA_HOME/bin/java" ]] ||
    fail "JAVA_HOME is not set to a valid JDK; run install_jdk.sh first (Trino $TRINO_VERSION requires Java 23+)"
}

# Sets one `key=value` line in a Java .properties file, adding it if missing.
set_properties_property() {
  local file="$1"
  local key="$2"
  local value="$3"
  local escaped_key escaped_value

  mkdir -p "$(dirname "$file")"
  touch "$file"
  escaped_key="$(printf '%s' "$key" | sed 's/[.[\\*^$()+?{}|]/\\&/g')"
  escaped_value="$(printf '%s' "$value" | sed 's/[\\&]/\\&/g')"

  if grep -qE "^[[:space:]]*${escaped_key}=" "$file"; then
    sed -i -E "s|^[[:space:]]*${escaped_key}=.*|${key}=${escaped_value}|" "$file"
  else
    printf '%s=%s\n' "$key" "$value" >> "$file"
  fi
}

# Best-effort: matches this host's own IP addresses against
# TRINO_COORDINATOR_HOST to decide whether this node is the coordinator.
# Always overridable with an explicit TRINO_ROLE.
infer_role() {
  local local_ips ip

  if [[ -n "$TRINO_ROLE" ]]; then
    printf '%s\n' "$TRINO_ROLE"
    return
  fi

  [[ -n "$TRINO_COORDINATOR_HOST" ]] || fail "TRINO_ROLE is not set and TRINO_COORDINATOR_HOST is unknown; set TRINO_ROLE=coordinator|worker explicitly (or run prepare_trino_cluster.sh first)"

  if [[ "$TRINO_COORDINATOR_HOST" == "localhost" || "$TRINO_COORDINATOR_HOST" == "127.0.0.1" ]]; then
    printf 'coordinator\n'
    return
  fi

  local_ips="$(hostname -I 2>/dev/null || true)"
  for ip in $local_ips; do
    if [[ "$ip" == "$TRINO_COORDINATOR_HOST" ]]; then
      printf 'coordinator\n'
      return
    fi
  done

  printf 'worker\n'
}

validate_trino_target() {
  case "$TRINO_ROLE" in
    ""|coordinator|worker) ;;
    *) fail "TRINO_ROLE must be coordinator or worker, got: $TRINO_ROLE" ;;
  esac

  [[ -n "$TRINO_COORDINATOR_HOST" ]] ||
    fail "TRINO_COORDINATOR_HOST is required; run prepare_trino_cluster.sh first or set it explicitly"
}

confirm_trino_install() {
  local reply role

  case "$CONFIRM_TRINO_INSTALL" in
    true) return ;;
    false) fail "CONFIRM_TRINO_INSTALL=false" ;;
  esac
  [[ "$ASSUME_YES" == "true" ]] && return

  role="$(infer_role)"
  printf '\nTrino install summary:\n'
  printf '  version: %s\n' "$TRINO_VERSION"
  printf '  role: %s\n' "$role"
  printf '  coordinator_host: %s\n' "$TRINO_COORDINATOR_HOST"
  printf '  install_dir: %s\n' "$TRINO_INSTALL_DIR"
  printf '  home_link: %s\n' "$TRINO_HOME_LINK"
  printf '  data_dir: %s\n' "$TRINO_DATA_DIR"
  printf '  pixels_trino_source: %s\n\n' "$PIXELS_TRINO_DIR"

  [[ -t 0 ]] || fail "Trino install paths and role must be confirmed; set CONFIRM_TRINO_INSTALL=true after reviewing them"
  read -r -p "Install/configure Trino with these settings on this node? [y/N]: " reply
  [[ "$reply" =~ ^[Yy]$ ]] || fail "aborted Trino install"
}

download_trino_server() {
  local archive

  if [[ -d "$TRINO_INSTALL_DIR" ]]; then
    log "Trino install directory already exists: $TRINO_INSTALL_DIR"
  else
    require_command curl
    require_command tar
    mkdir -p "$TRINO_INSTALL_PARENT"
    archive="$TMP_DIR/trino-server-$TRINO_VERSION.tar.gz"

    log "downloading Trino $TRINO_VERSION from Maven Central"
    curl -fsSL "$TRINO_DOWNLOAD_URL" -o "$archive"

    log "extracting Trino to $TRINO_INSTALL_DIR"
    tar -xzf "$archive" -C "$TRINO_INSTALL_PARENT"
  fi

  ln -sfn "$TRINO_INSTALL_DIR" "$TRINO_HOME_LINK"
}

write_node_properties() {
  local file="$TRINO_HOME_LINK/etc/node.properties"
  local node_id

  mkdir -p "$TRINO_HOME_LINK/etc" "$TRINO_DATA_DIR"

  # node.id "should remain consistent across reboots or upgrades", so keep
  # whatever is already there instead of regenerating it every run.
  node_id="$(grep -E '^node\.id=' "$file" 2>/dev/null | cut -d= -f2-)"
  if [[ -z "$node_id" ]]; then
    node_id="$(cat /proc/sys/kernel/random/uuid 2>/dev/null || true)"
    [[ -n "$node_id" ]] || node_id="$(python3 -c 'import uuid; print(uuid.uuid4())' 2>/dev/null || true)"
    [[ -n "$node_id" ]] || fail "could not generate a node.id (no /proc/sys/kernel/random/uuid and no python3)"
    log "generated new node.id: $node_id"
  else
    log "reusing existing node.id: $node_id"
  fi

  set_properties_property "$file" node.environment "$TRINO_ENVIRONMENT"
  set_properties_property "$file" node.id "$node_id"
  set_properties_property "$file" node.data-dir "$TRINO_DATA_DIR"
}

write_jvm_config() {
  local file="$TRINO_HOME_LINK/etc/jvm.config"

  if [[ -f "$file" ]]; then
    log "jvm.config already exists, leaving its memory/GC settings as-is: $file"
  else
    log "writing default jvm.config: $file"
    cat > "$file" <<'EOF'
-server
-Xmx16G
-XX:InitialRAMPercentage=80
-XX:MaxRAMPercentage=80
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
-XX:+HeapDumpOnOutOfMemoryError
-XX:-OmitStackTraceInFastThrow
-XX:ReservedCodeCacheSize=512M
-XX:PerMethodRecompilationCutoff=10000
-XX:PerBytecodeRecompilationCutoff=10000
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000
-Dfile.encoding=UTF-8
-XX:+EnableDynamicAgentLoading
EOF
  fi

  # Required by the pixels-trino connector (Java 9+ reflection access),
  # per https://github.com/pixelsdb/pixels-trino's README.
  grep -qxF '--add-opens=java.base/sun.nio.ch=ALL-UNNAMED' "$file" ||
    printf '%s\n' '--add-opens=java.base/sun.nio.ch=ALL-UNNAMED' >> "$file"
  grep -qxF '--add-opens=java.base/java.nio=ALL-UNNAMED' "$file" ||
    printf '%s\n' '--add-opens=java.base/java.nio=ALL-UNNAMED' >> "$file"
}

write_config_properties() {
  local file="$TRINO_HOME_LINK/etc/config.properties"
  local role discovery_uri

  role="$(infer_role)"
  log "this node's Trino role: $role (TRINO_COORDINATOR_HOST=$TRINO_COORDINATOR_HOST)"

  # discovery.uri must point at the coordinator's real, reachable address -
  # never localhost - so workers (and the coordinator itself) can find it.
  discovery_uri="http://$TRINO_COORDINATOR_HOST:$TRINO_HTTP_PORT"

  set_properties_property "$file" http-server.http.port "$TRINO_HTTP_PORT"
  set_properties_property "$file" discovery.uri "$discovery_uri"

  if [[ "$role" == "coordinator" ]]; then
    set_properties_property "$file" coordinator true
    set_properties_property "$file" node-scheduler.include-coordinator "$TRINO_COORDINATOR_IS_WORKER"
  else
    set_properties_property "$file" coordinator false
    # node-scheduler.include-coordinator only applies to the coordinator;
    # remove it here in case this file was previously written for the
    # coordinator role.
    sed -i -E '/^node-scheduler\.include-coordinator=/d' "$file"
  fi
}

write_log_properties() {
  local file="$TRINO_HOME_LINK/etc/log.properties"

  set_properties_property "$file" io.trino INFO
}

build_pixels_trino_plugin() {
  local connector_zip listener_zip plugin_dir="$TRINO_HOME_LINK/plugin"

  [[ "$INSTALL_PIXELS_TRINO_PLUGIN" == "true" ]] || {
    log "INSTALL_PIXELS_TRINO_PLUGIN=false; skipping the pixels-trino connector/listener"
    return
  }

  require_java
  require_command mvn

  if [[ ! -d "$PIXELS_TRINO_DIR/.git" ]]; then
    require_command git
    log "cloning pixels-trino into $PIXELS_TRINO_DIR"
    git clone "$PIXELS_TRINO_REPO_URL" "$PIXELS_TRINO_DIR"
  fi

  log "building pixels-trino (requires Pixels itself to already be 'mvn install'ed locally)"
  (cd "$PIXELS_TRINO_DIR" && mvn clean install) || fail "pixels-trino build failed"

  connector_zip="$(find "$PIXELS_TRINO_DIR/connector/target" -maxdepth 1 -name 'pixels-trino-connector-*.zip' -print -quit 2>/dev/null || true)"
  listener_zip="$(find "$PIXELS_TRINO_DIR/listener/target" -maxdepth 1 -name 'pixels-trino-listener-*.zip' -print -quit 2>/dev/null || true)"
  [[ -n "$connector_zip" ]] || fail "pixels-trino-connector-*.zip not found under $PIXELS_TRINO_DIR/connector/target"

  require_command unzip
  mkdir -p "$plugin_dir"
  rm -rf "$plugin_dir/pixels-trino-connector"
  log "installing pixels-trino connector: $connector_zip"
  unzip -oq "$connector_zip" -d "$plugin_dir"

  set_properties_property "$TRINO_HOME_LINK/etc/catalog/pixels.properties" connector.name pixels
  set_properties_property "$TRINO_HOME_LINK/etc/catalog/pixels.properties" cloud.function.switch "$CLOUD_FUNCTION_SWITCH"
  set_properties_property "$TRINO_HOME_LINK/etc/catalog/pixels.properties" clean.intermediate.result true

  if [[ "$INSTALL_PIXELS_TRINO_LISTENER" == "true" ]]; then
    [[ -n "$listener_zip" ]] || fail "pixels-trino-listener-*.zip not found under $PIXELS_TRINO_DIR/listener/target"
    rm -rf "$plugin_dir/pixels-trino-listener"
    log "installing pixels-trino event listener: $listener_zip"
    unzip -oq "$listener_zip" -d "$plugin_dir"

    mkdir -p "$PIXELS_HOME/listener"
    set_properties_property "$TRINO_HOME_LINK/etc/event-listener.properties" event-listener.name pixels-event-listener
    set_properties_property "$TRINO_HOME_LINK/etc/event-listener.properties" enabled true
    set_properties_property "$TRINO_HOME_LINK/etc/event-listener.properties" listened.user.prefix none
    set_properties_property "$TRINO_HOME_LINK/etc/event-listener.properties" listened.schema pixels
    set_properties_property "$TRINO_HOME_LINK/etc/event-listener.properties" listened.query.type SELECT
    set_properties_property "$TRINO_HOME_LINK/etc/event-listener.properties" log.dir "$PIXELS_HOME/listener/"
  else
    log "INSTALL_PIXELS_TRINO_LISTENER=false; skipping the optional pixels-trino event listener"
  fi
}

install_trino_cli() {
  local cli_path="$TRINO_HOME_LINK/bin/trino"

  [[ "$INSTALL_TRINO_CLI" == "true" ]] || return 0
  [[ -x "$cli_path" ]] && { log "trino-cli already installed: $cli_path"; return; }

  require_command curl
  log "downloading trino-cli $TRINO_VERSION"
  if curl -fsSL "$TRINO_CLI_DOWNLOAD_URL" -o "$cli_path"; then
    chmod +x "$cli_path"
  else
    warn "could not download trino-cli from $TRINO_CLI_DOWNLOAD_URL; continuing without it (not fatal)"
    rm -f "$cli_path"
  fi
}

verify_install() {
  [[ -x "$TRINO_HOME_LINK/bin/launcher" ]] || fail "missing $TRINO_HOME_LINK/bin/launcher"
  [[ -f "$TRINO_HOME_LINK/etc/node.properties" ]] || fail "missing $TRINO_HOME_LINK/etc/node.properties"
  [[ -f "$TRINO_HOME_LINK/etc/config.properties" ]] || fail "missing $TRINO_HOME_LINK/etc/config.properties"
  [[ -f "$TRINO_HOME_LINK/etc/jvm.config" ]] || fail "missing $TRINO_HOME_LINK/etc/jvm.config"
  [[ -d "$TRINO_DATA_DIR" ]] || fail "missing data dir $TRINO_DATA_DIR"

  if [[ "$INSTALL_PIXELS_TRINO_PLUGIN" == "true" ]]; then
    [[ -d "$TRINO_HOME_LINK/plugin/pixels-trino-connector" ]] || fail "pixels-trino connector not installed under $TRINO_HOME_LINK/plugin"
    [[ -f "$TRINO_HOME_LINK/etc/catalog/pixels.properties" ]] || fail "pixels catalog config not written under $TRINO_HOME_LINK/etc/catalog"
  fi

  log "Trino installation verified at $TRINO_HOME_LINK ($TRINO_INSTALL_DIR)"
}

main() {
  validate_trino_target
  confirm_trino_install
  require_java
  download_trino_server
  write_node_properties
  write_jvm_config
  write_config_properties
  write_log_properties
  build_pixels_trino_plugin
  install_trino_cli
  verify_install
}

main "$@"
