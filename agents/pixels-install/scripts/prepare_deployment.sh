#!/usr/bin/env bash
set -euo pipefail

AGENT_DIR="${AGENT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
REPO_ROOT="${REPO_ROOT:-$(cd "$AGENT_DIR/../.." && pwd)}"
SHARED_SETUP_CLUSTER="${SHARED_SETUP_CLUSTER:-$REPO_ROOT/agents/scripts/setup_cluster.sh}"
OUTPUT_FILE="${OUTPUT_FILE:-$AGENT_DIR/deployment.env}"
PIXELS_HOME="${PIXELS_HOME:-$HOME/opt/pixels}"
SSH_USER="${SSH_USER:-root}"
SSH_PORT="${SSH_PORT:-}"
VERIFY_REMOTE_LOGIN="${VERIFY_REMOTE_LOGIN:-true}"
SETUP_SSH="${SETUP_SSH:-false}"

NODES=()

usage() {
  cat <<EOF
Usage: $0 --node <ssh_target,host_ip,host_name,host_user,host_port> [options]
       $0 --nodes-file <file> [options]

Options:
  --node <ssh_target,host_ip,host_name,host_user,host_port>
      Add one deployment node. Can be repeated. The first node is treated as the
      coordinator by default; all nodes are written to PIXELS_WORKERS.

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

  [[ "${#NODES[@]}" -gt 0 ]] || fail "at least one --node or --nodes-file entry is required"
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

write_deployment_env() {
  local coordinator_host
  local coordinator_ssh_target
  local metadata_host
  local workers=()
  local workers_value
  local node host_name

  coordinator_ssh_target="$(csv_field "${NODES[0]}" 1)"
  coordinator_host="$(csv_field "${NODES[0]}" 3)"
  metadata_host="$coordinator_host"

  for node in "${NODES[@]}"; do
    host_name="$(csv_field "$node" 3)"
    workers+=("$host_name")
  done
  workers_value="$(join_by_space "${workers[@]}")"

  mkdir -p "$(dirname "$OUTPUT_FILE")"
  cat > "$OUTPUT_FILE" <<EOF
# Generated by pixels-install/scripts/prepare_deployment.sh
PIXELS_HOME=$(shell_quote "$PIXELS_HOME")
PIXELS_WORKERS=$(shell_quote "$workers_value")
COORDINATOR_SSH_TARGET=$(shell_quote "$coordinator_ssh_target")
COORDINATOR_HOST=$(shell_quote "$coordinator_host")
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

  [[ "$SETUP_SSH" == "true" ]] || return
  [[ -x "$SHARED_SETUP_CLUSTER" ]] || fail "setup_cluster.sh not found or not executable: $SHARED_SETUP_CLUSTER"

  args+=(--ssh-user "$SSH_USER")
  if [[ -n "$SSH_PORT" ]]; then
    args+=(--ssh-port "$SSH_PORT")
  fi
  args+=(--verify-remote-login "$VERIFY_REMOTE_LOGIN")

  for node in "${NODES[@]}"; do
    args+=(--node "$node")
  done

  log "setting up cluster passwordless SSH"
  "$SHARED_SETUP_CLUSTER" "${args[@]}"
}

main() {
  parse_args "$@"
  write_deployment_env
  run_setup_cluster
}

main "$@"
