#!/usr/bin/env bash
set -euo pipefail

# Collects the Trino cluster topology (one coordinator, zero or more
# workers) the same way prepare_deployment.sh does for Pixels, writes it to
# trino-deployment.env for install_trino.sh and install_trino_shell_helpers.sh
# to read, and optionally delegates passwordless-SSH bootstrap across every
# node to the shared shared-scripts/setup_cluster.sh - per
# https://trino.io/docs/466/installation/deployment.html, every node in a
# Trino cluster needs to be reachable, and start_trino_cluster()-style
# helpers SSH from the coordinator into each worker.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/shell_env.sh
source "$SCRIPT_DIR/lib/shell_env.sh"
SKILL_DIR="${SKILL_DIR:-$(skill_dir)}"
STATE_DIR="${STATE_DIR:-$(state_dir)}"
SHARED_SETUP_CLUSTER="${SHARED_SETUP_CLUSTER:-$SKILL_DIR/shared-scripts/setup_cluster.sh}"
OUTPUT_FILE="${OUTPUT_FILE:-$STATE_DIR/trino-deployment.env}"

TRINO_VERSION="${TRINO_VERSION:-466}"
TRINO_HTTP_PORT="${TRINO_HTTP_PORT:-8080}"
TRINO_COORDINATOR_IS_WORKER="${TRINO_COORDINATOR_IS_WORKER:-false}"
TRINO_INSTALL_PARENT="${TRINO_INSTALL_PARENT:-$HOME/opt}"
TRINO_HOME_LINK="${TRINO_HOME_LINK:-$TRINO_INSTALL_PARENT/trino-server}"
TRINO_DATA_DIR="${TRINO_DATA_DIR:-$TRINO_INSTALL_PARENT/var/trino/data}"
SSH_USER="${SSH_USER:-root}"
SSH_PORT="${SSH_PORT:-}"
VERIFY_REMOTE_LOGIN="${VERIFY_REMOTE_LOGIN:-true}"
SETUP_SSH="${SETUP_SSH:-false}"
ASSUME_YES="${ASSUME_YES:-false}"
CONFIRM_TRINO_DEPLOYMENT="${CONFIRM_TRINO_DEPLOYMENT:-}"

COORDINATOR=""
WORKERS=()

usage() {
  cat <<EOF
Usage: $0 --coordinator <ssh_target,host_ip,host_name,host_user,host_port> [options]
       $0

With no node arguments, the script prompts interactively for Trino topology,
coordinator/worker roles, and install locations.

Options:
  --coordinator <ssh_target,host_ip,host_name,host_user,host_port>
      Required. The node that runs the Trino coordinator.

  --worker <ssh_target,host_ip,host_name,host_user,host_port>
      Add one worker node. Can be repeated. Omit entirely for a single-node
      deployment (pass --coordinator-is-worker true in that case).

  --coordinator-is-worker <true|false>
      Whether the coordinator also accepts scheduled work
      (node-scheduler.include-coordinator). Default: false, matching the
      official recommendation to dedicate the coordinator on larger
      clusters. Forced to true automatically if no --worker is given.

  --version <trino_version>
      Trino server version to install. Default: 466.

  --http-port <port>
      HTTP port shared by every node (also used in discovery.uri). Default: 8080.

  --install-parent <path>
      Directory that contains the versioned Trino install. Default: $HOME/opt

  --home-link <path>
      Symlink/current Trino home. Default: <install-parent>/trino-server

  --data-dir <path>
      Trino node.data-dir. Default: <install-parent>/var/trino/data

  --ssh-user <user>
      User used by the local machine when connecting to ssh_target values
      that do not already include user@host. Default: root

  --ssh-port <port>
      Shared SSH port used by the local machine. Leave empty for SSH defaults.

  --setup-ssh <true|false>
      Run shared-scripts/setup_cluster.sh after writing trino-deployment.env,
      so the coordinator and every worker can passwordlessly SSH into each
      other (required by start_trino_cluster()-style helpers). Default: false

  --verify-remote-login <true|false>
      Passed to setup_cluster.sh when --setup-ssh true. Default: true

  --output <file>
      Deployment env file path. Default: $STATE_DIR/trino-deployment.env

  -h, --help
      Show this help message.
EOF
}

log() {
  printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"
}

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

parse_bool() {
  case "$1" in
    true|false) printf '%s\n' "$1" ;;
    *) fail "expected true or false, got: $1" ;;
  esac
}

prompt_value() {
  local prompt="$1"
  local default_value="$2"
  local value

  read -r -p "$prompt [$default_value]: " value
  printf '%s\n' "${value:-$default_value}"
}

prompt_bool() {
  local prompt="$1"
  local default_value="$2"
  local reply suffix

  if [[ "$default_value" == "true" ]]; then
    suffix="Y/n"
  else
    suffix="y/N"
  fi

  read -r -p "$prompt [$suffix]: " reply
  if [[ -z "$reply" ]]; then
    printf '%s\n' "$default_value"
  elif [[ "$reply" =~ ^[Yy]$ ]]; then
    printf 'true\n'
  else
    printf 'false\n'
  fi
}

validate_node_spec() {
  local spec="$1"
  local ssh_target host_ip host_name host_user host_port extra

  IFS=, read -r ssh_target host_ip host_name host_user host_port extra <<< "$spec"
  [[ -z "${extra:-}" ]] || fail "node spec expects exactly 5 comma-separated fields: $spec"
  [[ -n "${ssh_target:-}" ]] || fail "node spec ssh_target is empty: $spec"
  [[ -n "${host_ip:-}" ]] || fail "node spec host_ip is empty: $spec"
  [[ -n "${host_name:-}" ]] || fail "node spec host_name is empty: $spec"
  [[ -n "${host_user:-}" ]] || fail "node spec host_user is empty: $spec"
  [[ "${host_port:-}" =~ ^[0-9]+$ ]] || fail "node spec host_port must be a number: $spec"
  (( host_port >= 1 && host_port <= 65535 )) || fail "node spec host_port must be between 1 and 65535: $spec"
}

parse_args() {
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --coordinator)
        [[ "$#" -ge 2 ]] || fail "--coordinator requires a value"
        validate_node_spec "$2"
        COORDINATOR="$2"
        shift 2
        ;;
      --worker)
        [[ "$#" -ge 2 ]] || fail "--worker requires a value"
        validate_node_spec "$2"
        WORKERS+=("$2")
        shift 2
        ;;
      --coordinator-is-worker)
        [[ "$#" -ge 2 ]] || fail "--coordinator-is-worker requires a value"
        TRINO_COORDINATOR_IS_WORKER="$(parse_bool "$2")"
        shift 2
        ;;
      --version)
        [[ "$#" -ge 2 ]] || fail "--version requires a value"
        TRINO_VERSION="$2"
        shift 2
        ;;
      --http-port)
        [[ "$#" -ge 2 ]] || fail "--http-port requires a value"
        TRINO_HTTP_PORT="$2"
        shift 2
        ;;
      --install-parent)
        [[ "$#" -ge 2 ]] || fail "--install-parent requires a value"
        TRINO_INSTALL_PARENT="$2"
        TRINO_HOME_LINK="$TRINO_INSTALL_PARENT/trino-server"
        TRINO_DATA_DIR="$TRINO_INSTALL_PARENT/var/trino/data"
        shift 2
        ;;
      --home-link)
        [[ "$#" -ge 2 ]] || fail "--home-link requires a value"
        TRINO_HOME_LINK="$2"
        shift 2
        ;;
      --data-dir)
        [[ "$#" -ge 2 ]] || fail "--data-dir requires a value"
        TRINO_DATA_DIR="$2"
        shift 2
        ;;
      --ssh-user)
        [[ "$#" -ge 2 ]] || fail "--ssh-user requires a value"
        SSH_USER="$2"
        shift 2
        ;;
      --ssh-port)
        [[ "$#" -ge 2 ]] || fail "--ssh-port requires a value"
        SSH_PORT="$2"
        shift 2
        ;;
      --setup-ssh)
        [[ "$#" -ge 2 ]] || fail "--setup-ssh requires a value"
        SETUP_SSH="$(parse_bool "$2")"
        shift 2
        ;;
      --verify-remote-login)
        [[ "$#" -ge 2 ]] || fail "--verify-remote-login requires a value"
        VERIFY_REMOTE_LOGIN="$(parse_bool "$2")"
        shift 2
        ;;
      --output)
        [[ "$#" -ge 2 ]] || fail "--output requires a value"
        OUTPUT_FILE="$2"
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

  if [[ "${#WORKERS[@]}" -eq 0 ]]; then
    [[ -n "$COORDINATOR" ]] || return 0
    log "no --worker given; treating this as a single-node deployment (forcing --coordinator-is-worker true)"
    TRINO_COORDINATOR_IS_WORKER=true
  fi
}

csv_field() {
  local spec="$1"
  local field_index="$2"

  awk -F, -v field_index="$field_index" '{ print $field_index }' <<< "$spec"
}

join_by_space() {
  local first=true
  local value

  for value in "$@"; do
    if [[ "$first" == "true" ]]; then
      printf '%s' "$value"
      first=false
    else
      printf ' %s' "$value"
    fi
  done
}

shell_quote() {
  printf '%q' "$1"
}

interactive_config_if_needed() {
  local mode default_user local_spec worker_count i worker_spec

  if [[ -n "$COORDINATOR" ]]; then
    return
  fi

  [[ -t 0 ]] || fail "no Trino topology was provided; pass --coordinator/--worker or run interactively"

  TRINO_VERSION="$(prompt_value "Trino version" "$TRINO_VERSION")"
  TRINO_HTTP_PORT="$(prompt_value "Trino HTTP port" "$TRINO_HTTP_PORT")"
  TRINO_INSTALL_PARENT="$(prompt_value "Trino install parent" "$TRINO_INSTALL_PARENT")"
  TRINO_HOME_LINK="$(prompt_value "Trino home symlink/current path" "$TRINO_INSTALL_PARENT/trino-server")"
  TRINO_DATA_DIR="$(prompt_value "Trino data dir" "$TRINO_INSTALL_PARENT/var/trino/data")"

  mode="$(prompt_value "Trino deployment mode (single|cluster)" "single")"
  case "$mode" in
    single|cluster) ;;
    *) fail "Trino deployment mode must be single or cluster" ;;
  esac

  if [[ "$mode" == "single" ]]; then
    default_user="$(id -un 2>/dev/null || printf '%s' "${USER:-root}")"
    local_spec="localhost,127.0.0.1,localhost,$default_user,22"
    COORDINATOR="$(prompt_value "Single-node Trino node spec" "$local_spec")"
    validate_node_spec "$COORDINATOR"
    TRINO_COORDINATOR_IS_WORKER=true
    return
  fi

  COORDINATOR="$(prompt_value "Trino coordinator node spec (ssh_target,host_ip,host_name,host_user,host_port)" "")"
  validate_node_spec "$COORDINATOR"
  TRINO_COORDINATOR_IS_WORKER="$(prompt_bool "Should the Trino coordinator also accept scheduled work?" "false")"

  worker_count="$(prompt_value "Number of Trino worker nodes" "1")"
  [[ "$worker_count" =~ ^[0-9]+$ ]] || fail "worker count must be a number"
  for ((i = 1; i <= worker_count; i++)); do
    worker_spec="$(prompt_value "Trino worker #$i node spec (ssh_target,host_ip,host_name,host_user,host_port)" "")"
    validate_node_spec "$worker_spec"
    WORKERS+=("$worker_spec")
  done
}

finalize_config() {
  [[ -n "$COORDINATOR" ]] || fail "Trino coordinator is not set"

  if [[ "${#WORKERS[@]}" -eq 0 ]]; then
    log "no Trino worker nodes configured; treating this as single-node Trino and forcing coordinator-is-worker true"
    TRINO_COORDINATOR_IS_WORKER=true
  fi
}

print_summary() {
  local node

  printf '\nTrino deployment summary:\n'
  printf '  version: %s\n' "$TRINO_VERSION"
  printf '  http_port: %s\n' "$TRINO_HTTP_PORT"
  printf '  install_parent: %s\n' "$TRINO_INSTALL_PARENT"
  printf '  home_link: %s\n' "$TRINO_HOME_LINK"
  printf '  data_dir: %s\n' "$TRINO_DATA_DIR"
  printf '  coordinator: %s\n' "$COORDINATOR"
  printf '  coordinator_is_worker: %s\n' "$TRINO_COORDINATOR_IS_WORKER"
  printf '  workers:\n'
  if [[ "${#WORKERS[@]}" -eq 0 ]]; then
    printf '    - (none; single-node coordinator/worker)\n'
  else
    for node in "${WORKERS[@]}"; do
      printf '    - %s\n' "$node"
    done
  fi
  printf '  output: %s\n\n' "$OUTPUT_FILE"
}

confirm_config() {
  local reply

  case "$CONFIRM_TRINO_DEPLOYMENT" in
    true) return ;;
    false) fail "CONFIRM_TRINO_DEPLOYMENT=false" ;;
  esac
  [[ "$ASSUME_YES" == "true" ]] && return

  print_summary
  [[ -t 0 ]] || fail "Trino deployment topology and install paths must be confirmed; set CONFIRM_TRINO_DEPLOYMENT=true after reviewing them"
  read -r -p "Write this Trino deployment configuration? [y/N]: " reply
  [[ "$reply" =~ ^[Yy]$ ]] || fail "aborted Trino deployment configuration"
}

write_deployment_env() {
  local coordinator_ssh_target coordinator_host coordinator_name
  local worker_ssh_targets=() worker_hosts=() worker_names=() all_names=()
  local node host_name host_ip ssh_target

  coordinator_ssh_target="$(csv_field "$COORDINATOR" 1)"
  coordinator_host="$(csv_field "$COORDINATOR" 2)"
  coordinator_name="$(csv_field "$COORDINATOR" 3)"
  all_names+=("$coordinator_name")

  for node in "${WORKERS[@]}"; do
    ssh_target="$(csv_field "$node" 1)"
    host_ip="$(csv_field "$node" 2)"
    host_name="$(csv_field "$node" 3)"
    worker_ssh_targets+=("$ssh_target")
    worker_hosts+=("$host_ip")
    worker_names+=("$host_name")
    all_names+=("$host_name")
  done

  mkdir -p "$(dirname "$OUTPUT_FILE")"
  cat > "$OUTPUT_FILE" <<EOF
# Generated by pixels-install/scripts/prepare_trino_cluster.sh
TRINO_VERSION=$(shell_quote "$TRINO_VERSION")
TRINO_HTTP_PORT=$(shell_quote "$TRINO_HTTP_PORT")
TRINO_INSTALL_PARENT=$(shell_quote "$TRINO_INSTALL_PARENT")
TRINO_HOME_LINK=$(shell_quote "$TRINO_HOME_LINK")
TRINO_DATA_DIR=$(shell_quote "$TRINO_DATA_DIR")
TRINO_COORDINATOR_IS_WORKER=$(shell_quote "$TRINO_COORDINATOR_IS_WORKER")
TRINO_COORDINATOR_SSH_TARGET=$(shell_quote "$coordinator_ssh_target")
TRINO_COORDINATOR_HOST=$(shell_quote "$coordinator_host")
TRINO_COORDINATOR_NAME=$(shell_quote "$coordinator_name")
TRINO_WORKER_SSH_TARGETS=$(shell_quote "$(join_by_space "${worker_ssh_targets[@]}")")
TRINO_WORKER_HOSTS=$(shell_quote "$(join_by_space "${worker_hosts[@]}")")
TRINO_WORKER_NAMES=$(shell_quote "$(join_by_space "${worker_names[@]}")")
TRINO_ALL_NODE_NAMES=$(shell_quote "$(join_by_space "${all_names[@]}")")
EOF

  log "trino deployment config written: $OUTPUT_FILE"
}

run_setup_cluster() {
  local args=()
  local node

  [[ "$SETUP_SSH" == "true" ]] || return 0
  [[ -x "$SHARED_SETUP_CLUSTER" ]] || fail "setup_cluster.sh not found or not executable: $SHARED_SETUP_CLUSTER"

  args+=(--ssh-user "$SSH_USER")
  if [[ -n "$SSH_PORT" ]]; then
    args+=(--ssh-port "$SSH_PORT")
  fi
  args+=(--verify-remote-login "$VERIFY_REMOTE_LOGIN")
  args+=(--node "$COORDINATOR")

  for node in "${WORKERS[@]}"; do
    args+=(--node "$node")
  done

  log "setting up passwordless SSH across the Trino cluster (coordinator + ${#WORKERS[@]} worker(s))"
  "$SHARED_SETUP_CLUSTER" "${args[@]}"
}

main() {
  parse_args "$@"
  interactive_config_if_needed
  finalize_config
  confirm_config
  write_deployment_env
  run_setup_cluster
}

main "$@"
