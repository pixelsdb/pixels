#!/usr/bin/env bash
set -euo pipefail

AGENT_DIR="${AGENT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
REPO_ROOT="${REPO_ROOT:-$(cd "$AGENT_DIR/../.." && pwd)}"
SHARED_SETUP_CLUSTER="${SHARED_SETUP_CLUSTER:-$REPO_ROOT/agents/scripts/setup_cluster.sh}"
OUTPUT_FILE="${OUTPUT_FILE:-$AGENT_DIR/deployment.env}"
PIXELS_HOME="${PIXELS_HOME:-$HOME/opt/pixels}"
PIXELS_DEPLOYMENT_MODE="${PIXELS_DEPLOYMENT_MODE:-}"
PIXELS_COORDINATOR_IS_WORKER="${PIXELS_COORDINATOR_IS_WORKER:-true}"
SSH_USER="${SSH_USER:-root}"
SSH_PORT="${SSH_PORT:-}"
VERIFY_REMOTE_LOGIN="${VERIFY_REMOTE_LOGIN:-true}"
SETUP_SSH="${SETUP_SSH:-false}"
ASSUME_YES="${ASSUME_YES:-false}"
CONFIRM_PIXELS_DEPLOYMENT="${CONFIRM_PIXELS_DEPLOYMENT:-}"

NODES=()
COORDINATOR=""
WORKERS=()

usage() {
  cat <<EOF
Usage: $0 [--coordinator <ssh_target,host_ip,host_name,host_user,host_port> --worker <...>] [options]
       $0 --node <ssh_target,host_ip,host_name,host_user,host_port> [options]
       $0 --nodes-file <file> [options]
       $0

With no node arguments, the script prompts interactively for Pixels topology,
coordinator/worker roles, and PIXELS_HOME.

Options:
  --coordinator <ssh_target,host_ip,host_name,host_user,host_port>
      Pixels coordinator node. Required for an explicit cluster topology.

  --worker <ssh_target,host_ip,host_name,host_user,host_port>
      Pixels worker node. Can be repeated. For a single-node deployment, use the
      same node as coordinator and set --coordinator-is-worker true.

  --coordinator-is-worker <true|false>
      Whether the coordinator also runs a worker daemon. Default: true.

  --node <ssh_target,host_ip,host_name,host_user,host_port>
      Legacy shorthand. Can be repeated. The first node is treated as the
      coordinator; all nodes are written to PIXELS_WORKERS.

  --nodes-file <file>
      Read deployment nodes from a CSV file. Each non-empty, non-comment line
      must use: ssh_target,host_ip,host_name,host_user,host_port.

  --pixels-home <path>
      Pixels home on each node. Default: $HOME/opt/pixels

  --ssh-user <user>
      User used by the local machine when connecting to ssh_target values that
      do not already include user@host. Default: root

  --ssh-port <port>
      Shared SSH port used by the local machine. Leave empty for SSH defaults.

  --setup-ssh <true|false>
      Run agents/scripts/setup_cluster.sh after writing deployment.env.
      Default: false

  --verify-remote-login <true|false>
      Passed to setup_cluster.sh when --setup-ssh true. Default: true

  --output <file>
      Deployment env file path. Default: $AGENT_DIR/deployment.env

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
  [[ -z "${extra:-}" ]] || fail "--node expects exactly 5 comma-separated fields"
  [[ -n "${ssh_target:-}" ]] || fail "--node ssh_target is empty"
  [[ -n "${host_ip:-}" ]] || fail "--node host_ip is empty"
  [[ -n "${host_name:-}" ]] || fail "--node host_name is empty"
  [[ -n "${host_user:-}" ]] || fail "--node host_user is empty"
  [[ "${host_port:-}" =~ ^[0-9]+$ ]] || fail "--node host_port must be a number"
  (( host_port >= 1 && host_port <= 65535 )) || fail "--node host_port must be between 1 and 65535"
}

add_node_arg() {
  validate_node_spec "$1"
  NODES+=("$1")
}

set_coordinator_arg() {
  validate_node_spec "$1"
  COORDINATOR="$1"
}

add_worker_arg() {
  validate_node_spec "$1"
  WORKERS+=("$1")
}

add_nodes_file_arg() {
  local file="$1"
  local line
  local node_count=0

  [[ -f "$file" ]] || fail "--nodes-file not found: $file"

  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    line="$(printf '%s' "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -n "$line" ]] || continue
    [[ "${line:0:1}" != "#" ]] || continue

    add_node_arg "$line"
    ((node_count += 1))
  done < "$file"

  [[ "$node_count" -gt 0 ]] || fail "--nodes-file has no nodes: $file"
}

parse_args() {
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --node)
        [[ "$#" -ge 2 ]] || fail "--node requires a value"
        add_node_arg "$2"
        shift 2
        ;;
      --coordinator)
        [[ "$#" -ge 2 ]] || fail "--coordinator requires a value"
        set_coordinator_arg "$2"
        shift 2
        ;;
      --worker)
        [[ "$#" -ge 2 ]] || fail "--worker requires a value"
        add_worker_arg "$2"
        shift 2
        ;;
      --coordinator-is-worker)
        [[ "$#" -ge 2 ]] || fail "--coordinator-is-worker requires a value"
        PIXELS_COORDINATOR_IS_WORKER="$(parse_bool "$2")"
        shift 2
        ;;
      --nodes-file)
        [[ "$#" -ge 2 ]] || fail "--nodes-file requires a value"
        add_nodes_file_arg "$2"
        shift 2
        ;;
      --pixels-home)
        [[ "$#" -ge 2 ]] || fail "--pixels-home requires a value"
        PIXELS_HOME="$2"
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

  if [[ "${#NODES[@]}" -gt 0 ]]; then
    [[ -z "$COORDINATOR" ]] || fail "--node cannot be mixed with --coordinator"
    [[ "${#WORKERS[@]}" -eq 0 ]] || fail "--node cannot be mixed with --worker"
    COORDINATOR="${NODES[0]}"
    WORKERS=("${NODES[@]}")
    PIXELS_COORDINATOR_IS_WORKER=true
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

  [[ -t 0 ]] || fail "no Pixels topology was provided; pass --coordinator/--worker or run interactively"

  mode="$(prompt_value "Pixels deployment mode (single|cluster)" "${PIXELS_DEPLOYMENT_MODE:-single}")"
  case "$mode" in
    single|cluster) ;;
    *) fail "Pixels deployment mode must be single or cluster" ;;
  esac
  PIXELS_DEPLOYMENT_MODE="$mode"
  PIXELS_HOME="$(prompt_value "PIXELS_HOME on each node" "$PIXELS_HOME")"

  if [[ "$mode" == "single" ]]; then
    default_user="$(id -un 2>/dev/null || printf '%s' "${USER:-root}")"
    local_spec="localhost,127.0.0.1,localhost,$default_user,22"
    COORDINATOR="$(prompt_value "Single-node Pixels node spec" "$local_spec")"
    validate_node_spec "$COORDINATOR"
    PIXELS_COORDINATOR_IS_WORKER=true
    WORKERS=("$COORDINATOR")
    return
  fi

  COORDINATOR="$(prompt_value "Pixels coordinator node spec (ssh_target,host_ip,host_name,host_user,host_port)" "")"
  validate_node_spec "$COORDINATOR"
  PIXELS_COORDINATOR_IS_WORKER="$(prompt_bool "Should the Pixels coordinator also run a worker daemon?" "false")"
  if [[ "$PIXELS_COORDINATOR_IS_WORKER" == "true" ]]; then
    WORKERS+=("$COORDINATOR")
  fi

  worker_count="$(prompt_value "Number of additional Pixels worker nodes" "1")"
  [[ "$worker_count" =~ ^[0-9]+$ ]] || fail "worker count must be a number"
  for ((i = 1; i <= worker_count; i++)); do
    worker_spec="$(prompt_value "Pixels worker #$i node spec (ssh_target,host_ip,host_name,host_user,host_port)" "")"
    validate_node_spec "$worker_spec"
    WORKERS+=("$worker_spec")
  done
}

finalize_config() {
  local node coordinator_in_workers=false

  if [[ -z "$COORDINATOR" ]]; then
    fail "Pixels coordinator is not set"
  fi
  for node in "${WORKERS[@]}"; do
    if [[ "$node" == "$COORDINATOR" ]]; then
      coordinator_in_workers=true
      break
    fi
  done
  if [[ "$PIXELS_COORDINATOR_IS_WORKER" == "true" && "$coordinator_in_workers" == "false" ]]; then
    WORKERS=("$COORDINATOR" "${WORKERS[@]}")
  fi
  if [[ "${#WORKERS[@]}" -eq 0 ]]; then
    fail "at least one Pixels worker is required; set --worker or --coordinator-is-worker true for single-node deployments"
  fi
  if [[ -z "$PIXELS_DEPLOYMENT_MODE" ]]; then
    if [[ "${#WORKERS[@]}" -eq 1 && "${WORKERS[0]}" == "$COORDINATOR" ]]; then
      PIXELS_DEPLOYMENT_MODE=single
    else
      PIXELS_DEPLOYMENT_MODE=cluster
    fi
  fi
}

print_summary() {
  local node

  printf '\nPixels deployment summary:\n'
  printf '  mode: %s\n' "$PIXELS_DEPLOYMENT_MODE"
  printf '  PIXELS_HOME: %s\n' "$PIXELS_HOME"
  printf '  coordinator: %s\n' "$COORDINATOR"
  printf '  coordinator_is_worker: %s\n' "$PIXELS_COORDINATOR_IS_WORKER"
  printf '  workers:\n'
  for node in "${WORKERS[@]}"; do
    printf '    - %s\n' "$node"
  done
  printf '  output: %s\n\n' "$OUTPUT_FILE"
}

confirm_config() {
  local reply

  case "$CONFIRM_PIXELS_DEPLOYMENT" in
    true) return ;;
    false) fail "CONFIRM_PIXELS_DEPLOYMENT=false" ;;
  esac
  [[ "$ASSUME_YES" == "true" ]] && return

  print_summary
  [[ -t 0 ]] || fail "Pixels deployment topology must be confirmed; set CONFIRM_PIXELS_DEPLOYMENT=true after reviewing it"
  read -r -p "Write this Pixels deployment configuration? [y/N]: " reply
  [[ "$reply" =~ ^[Yy]$ ]] || fail "aborted Pixels deployment configuration"
}

write_deployment_env() {
  local coordinator_host coordinator_name
  local coordinator_ssh_target
  local metadata_host
  local workers=()
  local workers_value
  local node host_name

  coordinator_ssh_target="$(csv_field "$COORDINATOR" 1)"
  coordinator_host="$(csv_field "$COORDINATOR" 2)"
  coordinator_name="$(csv_field "$COORDINATOR" 3)"
  metadata_host="$coordinator_name"

  for node in "${WORKERS[@]}"; do
    host_name="$(csv_field "$node" 3)"
    workers+=("$host_name")
  done
  workers_value="$(join_by_space "${workers[@]}")"

  mkdir -p "$(dirname "$OUTPUT_FILE")"
  cat > "$OUTPUT_FILE" <<EOF
# Generated by pixels-install/scripts/prepare_deployment.sh
PIXELS_DEPLOYMENT_MODE=$(shell_quote "$PIXELS_DEPLOYMENT_MODE")
PIXELS_HOME=$(shell_quote "$PIXELS_HOME")
PIXELS_WORKERS=$(shell_quote "$workers_value")
PIXELS_COORDINATOR_IS_WORKER=$(shell_quote "$PIXELS_COORDINATOR_IS_WORKER")
PIXELS_COORDINATOR_SSH_TARGET=$(shell_quote "$coordinator_ssh_target")
PIXELS_COORDINATOR_HOST=$(shell_quote "$coordinator_host")
PIXELS_COORDINATOR_NAME=$(shell_quote "$coordinator_name")
COORDINATOR_SSH_TARGET=$(shell_quote "$coordinator_ssh_target")
COORDINATOR_HOST=$(shell_quote "$coordinator_name")
METADATA_SERVER_HOST=$(shell_quote "$metadata_host")
TRANS_SERVER_HOST=$(shell_quote "$metadata_host")
QUERY_SCHEDULE_SERVER_HOST=$(shell_quote "$metadata_host")
ETCD_HOSTS=$(shell_quote "$metadata_host")
METADATA_DB_HOST=$(shell_quote "$metadata_host")
EOF

  log "deployment config written: $OUTPUT_FILE"
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
    [[ "$node" == "$COORDINATOR" ]] && continue
    args+=(--node "$node")
  done

  log "setting up cluster passwordless SSH"
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
