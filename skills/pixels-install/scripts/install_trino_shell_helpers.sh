#!/usr/bin/env bash
set -euo pipefail

# Optional convenience step (asks before doing anything): installs
# start_trino_cluster/stop_trino_cluster/restart_trino_cluster/trino_cli on
# the Trino coordinator recorded in trino-deployment.env. The generated
# functions operate the coordinator locally and every worker over SSH.
# Set TRINO_SHELL_HELPERS_TARGET=local only when intentionally installing on
# the current host.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/shell_env.sh
source "$SCRIPT_DIR/lib/shell_env.sh"
load_toolchain_env

SKILL_DIR="${SKILL_DIR:-$(skill_dir)}"
STATE_DIR="${STATE_DIR:-$(state_dir)}"
TRINO_DEPLOYMENT_FILE="${TRINO_DEPLOYMENT_FILE:-$STATE_DIR/trino-deployment.env}"
SSH_USER_WAS_SET=false
SSH_PORT_WAS_SET=false
[[ -n "${SSH_USER+x}" ]] && SSH_USER_WAS_SET=true
[[ -n "${SSH_PORT+x}" ]] && SSH_PORT_WAS_SET=true
if [[ -f "$TRINO_DEPLOYMENT_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$TRINO_DEPLOYMENT_FILE"
  set +a
fi

if [[ "$SSH_USER_WAS_SET" == "false" ]]; then
  SSH_USER="${TRINO_SSH_USER:-root}"
else
  SSH_USER="${SSH_USER:-}"
fi
if [[ "$SSH_PORT_WAS_SET" == "false" ]]; then
  SSH_PORT="${TRINO_SSH_PORT:-}"
else
  SSH_PORT="${SSH_PORT:-}"
fi
TRINO_HTTP_PORT="${TRINO_HTTP_PORT:-8080}"
TRINO_FUNCTIONS_FILE="${TRINO_FUNCTIONS_FILE:-}"
TRINO_PIXELS_HOME="${TRINO_PIXELS_HOME:-${PIXELS_HOME:-$HOME/opt/pixels}}"
TRINO_PIXELS_CONFIG="${TRINO_PIXELS_CONFIG:-${PIXELS_CONFIG:-$TRINO_PIXELS_HOME/etc/pixels.properties}}"
TRINO_SHELL_HELPERS_TARGET="${TRINO_SHELL_HELPERS_TARGET:-coordinator}"
REMOTE_STATE_DIR="${REMOTE_STATE_DIR:-.agents/state/pixels-install}"
REMOTE_SCRIPT_DIR="${REMOTE_SCRIPT_DIR:-.agents/skills/pixels-install/scripts}"
ASSUME_YES="${ASSUME_YES:-false}"
INSTALL_TRINO_SHELL_HELPERS="${INSTALL_TRINO_SHELL_HELPERS:-}"

log() {
  printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"
}

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

confirm_install() {
  local reply target

  case "$INSTALL_TRINO_SHELL_HELPERS" in
    true) return 0 ;;
    false) return 1 ;;
  esac

  [[ "$ASSUME_YES" == "true" ]] && return 0

  target="$(target_description)"
  if [[ ! -t 0 ]]; then
    log "not running interactively and INSTALL_TRINO_SHELL_HELPERS/ASSUME_YES not set; skipping the trino shell helpers for $target. Set INSTALL_TRINO_SHELL_HELPERS=true once the user has agreed."
    return 1
  fi

  read -r -p "Install start_trino_cluster/stop_trino_cluster/restart_trino_cluster/trino_cli shell functions on $target? [y/N]: " reply
  [[ "$reply" =~ ^[Yy]$ ]]
}

remote_spec() {
  local target="$1"
  if [[ -n "$SSH_USER" && "$target" != *@* ]]; then
    printf '%s@%s' "$SSH_USER" "$target"
  else
    printf '%s' "$target"
  fi
}

shell_quote() {
  printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\\\''/g")"
}

ssh_host_part() {
  local target="$1"
  target="${target#*@}"
  target="${target%%:*}"
  printf '%s\n' "$target"
}

current_host_tokens() {
  {
    hostname 2>/dev/null || true
    hostname -f 2>/dev/null || true
    hostname -s 2>/dev/null || true
    hostname -I 2>/dev/null | tr ' ' '\n' || true
    printf 'localhost\n127.0.0.1\n::1\n'
  } | awk 'NF && !seen[$0]++'
}

is_current_host() {
  local candidate token
  for candidate in "$@"; do
    [[ -n "${candidate:-}" ]] || continue
    candidate="$(ssh_host_part "$candidate")"
    while IFS= read -r token; do
      [[ "$candidate" == "$token" ]] && return 0
    done < <(current_host_tokens)
  done
  return 1
}

coordinator_target() {
  [[ -n "${TRINO_COORDINATOR_SSH_TARGET:-}" ]] ||
    fail "TRINO_COORDINATOR_SSH_TARGET is unknown; run prepare_trino_cluster.sh first, set TRINO_DEPLOYMENT_FILE, or set TRINO_COORDINATOR_SSH_TARGET"
  printf '%s\n' "$TRINO_COORDINATOR_SSH_TARGET"
}

target_is_local() {
  local target
  case "$TRINO_SHELL_HELPERS_TARGET" in
    local) return 0 ;;
    coordinator) ;;
    *) fail "TRINO_SHELL_HELPERS_TARGET must be coordinator or local, got: $TRINO_SHELL_HELPERS_TARGET" ;;
  esac

  target="$(coordinator_target)"
  is_current_host "$target" "${TRINO_COORDINATOR_HOST:-}" "${TRINO_COORDINATOR_NAME:-}"
}

target_description() {
  if [[ "$TRINO_SHELL_HELPERS_TARGET" == "local" ]]; then
    printf 'current host'
  else
    printf 'Trino coordinator %s' "$(coordinator_target)"
  fi
}

# Coordinator first (operated locally by the generated functions), then every
# worker (operated over SSH from the coordinator).
build_node_list() {
  local node

  coordinator_target >/dev/null

  TRINO_NODE_LIST=("$(remote_spec "$TRINO_COORDINATOR_SSH_TARGET")")
  for node in ${TRINO_WORKER_SSH_TARGETS:-}; do
    TRINO_NODE_LIST+=("$(remote_spec "$node")")
  done
}

local_functions_file() {
  printf '%s\n' "${TRINO_FUNCTIONS_FILE:-$HOME/.trino-shell-helpers.sh}"
}

write_functions_file() {
  local functions_file="$1"
  local nodes_literal node default_pixels_home default_pixels_config

  nodes_literal=""
  for node in "${TRINO_NODE_LIST[@]}"; do
    nodes_literal+="  \"$node\"
"
  done
  default_pixels_home="$(shell_quote "$TRINO_PIXELS_HOME")"
  default_pixels_config="$(shell_quote "$TRINO_PIXELS_CONFIG")"

  log "writing trino cluster shell functions to $functions_file (${#TRINO_NODE_LIST[@]} node(s), coordinator first)"
  mkdir -p "$(dirname "$functions_file")"

  cat > "$functions_file" <<EOF
# Generated by pixels-install/scripts/install_trino_shell_helpers.sh
# TRINO_NODES[0] is the coordinator and is always operated locally; the rest
# are workers, operated over the passwordless SSH set up by
# prepare_trino_cluster.sh. Re-run install_trino_shell_helpers.sh to
# regenerate this file after the cluster topology changes.

TRINO_NODES=(
$nodes_literal)

TRINO_REMOTE_HOME_LINK="\${TRINO_REMOTE_HOME_LINK:-${TRINO_HOME_LINK:-$HOME/opt/trino-server}}"
TRINO_DEFAULT_PIXELS_HOME=$default_pixels_home
TRINO_DEFAULT_PIXELS_CONFIG=$default_pixels_config

_trino_home() {
  printf '%s\n' "\${TRINO_HOME_LINK:-\$HOME/opt/trino-server}"
}

_trino_pixels_home() {
  printf '%s\n' "\${TRINO_PIXELS_HOME:-\$TRINO_DEFAULT_PIXELS_HOME}"
}

_trino_pixels_config() {
  local pixels_home
  pixels_home="\$(_trino_pixels_home)"
  if [[ -n "\${TRINO_PIXELS_CONFIG:-}" ]]; then
    printf '%s\n' "\$TRINO_PIXELS_CONFIG"
  elif [[ "\$TRINO_DEFAULT_PIXELS_CONFIG" == "\$TRINO_DEFAULT_PIXELS_HOME/etc/pixels.properties" ]]; then
    printf '%s\n' "\$pixels_home/etc/pixels.properties"
  else
    printf '%s\n' "\$TRINO_DEFAULT_PIXELS_CONFIG"
  fi
}

_export_trino_pixels_env() {
  export PIXELS_HOME="\$(_trino_pixels_home)"
  export PIXELS_CONFIG="\$(_trino_pixels_config)"
}

_trino_remote_launcher() {
  printf '%s/bin/launcher' "\$TRINO_REMOTE_HOME_LINK"
}

_trino_remote_run() {
  local node="\$1"
  local action="\$2"
  local pixels_home pixels_config
  pixels_home="\$(_trino_pixels_home)"
  pixels_config="\$(_trino_pixels_config)"
  ssh -n "\$node" "export PIXELS_HOME='\$pixels_home'; export PIXELS_CONFIG='\$pixels_config'; \\"\$(_trino_remote_launcher)\\" \$action"
}

start_trino_cluster() {
  local node first=true

  for node in "\${TRINO_NODES[@]}"; do
    if [[ "\$first" == "true" ]]; then
      echo "starting trino on \$node (coordinator, local)"
      _export_trino_pixels_env
      "\$(_trino_home)/bin/launcher" start || { echo "failed to start coordinator \$node" >&2; return 1; }
      first=false
    else
      echo "starting trino on \$node (worker, remote)"
      _trino_remote_run "\$node" start || { echo "failed to start worker \$node" >&2; return 1; }
    fi
  done

  echo "trino cluster started"
}

stop_trino_cluster() {
  local idx node

  for ((idx = \${#TRINO_NODES[@]} - 1; idx >= 0; idx--)); do
    node="\${TRINO_NODES[\$idx]}"
    if (( idx > 0 )); then
      echo "stopping trino on \$node (worker, remote)"
      _trino_remote_run "\$node" stop || { echo "failed to stop worker \$node" >&2; return 1; }
    else
      echo "stopping trino on \$node (coordinator, local)"
      _export_trino_pixels_env
      "\$(_trino_home)/bin/launcher" stop || { echo "failed to stop coordinator \$node" >&2; return 1; }
    fi
  done

  echo "trino cluster stopped"
}

restart_trino_cluster() {
  stop_trino_cluster || return 1
  start_trino_cluster || return 1
  echo "trino cluster restarted"
}

trino_cli() {
  local catalog="\${1:-pixels}"
  _export_trino_pixels_env
  "\$(_trino_home)/bin/trino" --server "localhost:\${TRINO_HTTP_PORT:-$TRINO_HTTP_PORT}" --catalog "\$catalog"
}
EOF

  chmod 644 "$functions_file"
}

persist_source_line() {
  local functions_file="$1"
  local profile_file line

  profile_file="$(detect_profile_file)"
  line="[ -f \"$functions_file\" ] && source \"$functions_file\""

  log "persisting source line for $functions_file in $profile_file"
  persist_line "$profile_file" "$line"
}

install_local() {
  local functions_file

  build_node_list
  functions_file="$(local_functions_file)"
  write_functions_file "$functions_file"
  bash -n "$functions_file" || fail "generated functions file has a syntax error: $functions_file"
  persist_source_line "$functions_file"

  log "trino shell helpers installed on current host: start_trino_cluster, stop_trino_cluster, restart_trino_cluster, trino_cli"
  log "open a new terminal (or run: source $(detect_profile_file)) to use them"
}

install_remote_coordinator() {
  local target remote remote_env_string
  local -a ssh_args scp_args remote_env

  target="$(coordinator_target)"
  remote="$(remote_spec "$target")"

  ssh_args=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
  scp_args=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
  if [[ -n "$SSH_PORT" ]]; then
    ssh_args+=(-p "$SSH_PORT")
    scp_args+=(-P "$SSH_PORT")
  fi

  log "installing Trino shell helpers on coordinator $remote"
  ssh "${ssh_args[@]}" "$remote" "mkdir -p $(shell_quote "$REMOTE_SCRIPT_DIR/lib") $(shell_quote "$REMOTE_STATE_DIR")"
  scp "${scp_args[@]}" "$0" "$remote:$REMOTE_SCRIPT_DIR/install_trino_shell_helpers.sh"
  scp "${scp_args[@]}" "$SCRIPT_DIR/lib/shell_env.sh" "$remote:$REMOTE_SCRIPT_DIR/lib/shell_env.sh"
  [[ -f "$TRINO_DEPLOYMENT_FILE" ]] ||
    fail "Trino deployment file not found: $TRINO_DEPLOYMENT_FILE"
  scp "${scp_args[@]}" "$TRINO_DEPLOYMENT_FILE" "$remote:$REMOTE_STATE_DIR/trino-deployment.env"

  remote_env=(
    "INSTALL_TRINO_SHELL_HELPERS=true"
    "TRINO_SHELL_HELPERS_TARGET=local"
    "STATE_DIR=$(shell_quote "$REMOTE_STATE_DIR")"
    "TRINO_PIXELS_HOME=$(shell_quote "$TRINO_PIXELS_HOME")"
    "TRINO_PIXELS_CONFIG=$(shell_quote "$TRINO_PIXELS_CONFIG")"
    "SSH_USER=$(shell_quote "$SSH_USER")"
    "SSH_PORT=$(shell_quote "$SSH_PORT")"
  )
  if [[ -n "$TRINO_FUNCTIONS_FILE" ]]; then
    remote_env+=("TRINO_FUNCTIONS_FILE=$(shell_quote "$TRINO_FUNCTIONS_FILE")")
  fi

  remote_env_string="$(printf '%s ' "${remote_env[@]}")"
  ssh "${ssh_args[@]}" "$remote" "chmod +x $(shell_quote "$REMOTE_SCRIPT_DIR/install_trino_shell_helpers.sh") && env $remote_env_string $(shell_quote "$REMOTE_SCRIPT_DIR/install_trino_shell_helpers.sh")"
}

main() {
  if ! confirm_install; then
    log "skipping trino shell helpers (not confirmed)"
    return 0
  fi

  if target_is_local; then
    install_local
  else
    install_remote_coordinator
  fi
}

main "$@"
