#!/usr/bin/env bash
set -euo pipefail

# Run this script on your local machine. It logs in to each remote server,
# ensures an SSH key exists, updates /etc/hosts, and distributes public keys.

SSH_USER="${SSH_USER:-root}"      # Set to empty if SSH_TARGETS already contains user@host or uses ssh config User.
SSH_PORT="${SSH_PORT:-}"          # Leave empty to use ssh config / default port 22.
VERIFY_REMOTE_LOGIN="${VERIFY_REMOTE_LOGIN:-true}"

# SSH_TARGETS:
#   Used only by this local script to connect to each server. Each item can be
#   an IP, DNS name, Host alias from ~/.ssh/config, or user@host.
SSH_TARGETS=(
  "10.0.0.11"
  "10.0.0.12"
  "10.0.0.13"
)

# HOSTS_IPS:
#   Written into every remote server's /etc/hosts. Usually these are private
#   or internal IPs. Keep the order aligned with SSH_TARGETS and HOST_NAMES.
HOSTS_IPS=(
  "10.0.0.11"
  "10.0.0.12"
  "10.0.0.13"
)

# HOST_NAMES:
#   Written into /etc/hosts and used when verifying remote-to-remote SSH login.
#   Keep the order aligned with SSH_TARGETS and HOSTS_IPS.
HOST_NAMES=(
  "trino-coordinator"
  "trino-worker-1"
  "trino-worker-2"
)

# HOST_USERS:
#   Usernames used when cluster hosts SSH to each other. Keep the order aligned
#   with SSH_TARGETS, HOSTS_IPS, and HOST_NAMES.
HOST_USERS=(
  "root"
  "root"
  "root"
)

# HOST_PORTS:
#   SSH ports used when cluster hosts SSH to each other. Keep the order aligned
#   with SSH_TARGETS, HOSTS_IPS, HOST_NAMES, and HOST_USERS.
HOST_PORTS=(
  "22"
  "22"
  "22"
)

MANAGED_BEGIN="# BEGIN managed by setup_cluster_ssh.sh"
MANAGED_END="# END managed by setup_cluster_ssh.sh"

SSH_COMMON_OPTS=(
  -o BatchMode=yes
  -o ConnectTimeout=10
  -o StrictHostKeyChecking=accept-new
)

WORKDIR=""
CUSTOM_NODE_CONFIG_ACTIVE=false
NODE_INPUT_MODE=""

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --node <ssh_target,host_ip,host_name,host_user,host_port>
      Add one cluster node. Can be repeated. When at least one custom node input
      option is used, the built-in SSH_TARGETS, HOSTS_IPS, HOST_NAMES,
      HOST_USERS, and HOST_PORTS defaults are replaced.

  --nodes-file <file>
      Read cluster nodes from a CSV file. Each non-empty, non-comment line must
      use: ssh_target,host_ip,host_name,host_user,host_port.

  --ssh-targets <values>
      Set SSH_TARGETS. Values may be space- or comma-separated.

  --hosts-ips <values>
      Set HOSTS_IPS. Values may be space- or comma-separated.

  --host-names <values>
      Set HOST_NAMES. Values may be space- or comma-separated.

  --host-users <values>
      Set HOST_USERS. Values may be space- or comma-separated.

  --host-ports <values>
      Set HOST_PORTS. Values may be space- or comma-separated.

  --ssh-user <user>
      User used by this local script when SSH_TARGETS do not include user@host.
      Use an empty value to rely on SSH config or explicit user@host targets.

  --ssh-port <port>
      Port used by this local script when connecting to every SSH_TARGET.
      Leave unset to use SSH config or default port 22.

  --verify-remote-login <true|false>
      Whether to verify passwordless SSH from each cluster node to the others.

  -h, --help
      Show this help message.

Examples:
  $0 \
    --ssh-user root \
    --node 10.0.0.11,10.0.0.11,trino-coordinator,root,22 \
    --node 10.0.0.12,10.0.0.12,trino-worker-1,root,22

  $0 \
    --ssh-targets "10.0.0.11 10.0.0.12" \
    --hosts-ips "10.0.0.11 10.0.0.12" \
    --host-names "trino-coordinator trino-worker-1" \
    --host-users "root root" \
    --host-ports "22 22"
EOF
}

parse_bool() {
  case "$1" in
    true|false) printf '%s\n' "$1" ;;
    *) die "expected true or false, got: $1" ;;
  esac
}

clear_node_config() {
  SSH_TARGETS=()
  HOSTS_IPS=()
  HOST_NAMES=()
  HOST_USERS=()
  HOST_PORTS=()
}

activate_custom_node_config() {
  if [[ "$CUSTOM_NODE_CONFIG_ACTIVE" == "false" ]]; then
    clear_node_config
    CUSTOM_NODE_CONFIG_ACTIVE=true
  fi
}

use_node_input_mode() {
  local mode="$1"
  local option_name="$2"

  if [[ -n "$NODE_INPUT_MODE" && "$NODE_INPUT_MODE" != "$mode" ]]; then
    die "$option_name cannot be mixed with $NODE_INPUT_MODE-based node options"
  fi

  NODE_INPUT_MODE="$mode"
  activate_custom_node_config
}

set_array_arg() {
  local array_name="$1"
  local option_name="$2"
  local raw_value="$3"
  local normalized
  local -a parsed=()
  local -n values_ref="$array_name"

  normalized="${raw_value//,/ }"
  read -r -a parsed <<< "$normalized"
  [[ "${#parsed[@]}" -gt 0 ]] || die "$option_name requires at least one value"

  values_ref=("${parsed[@]}")
}

add_node_arg() {
  local spec="$1"
  local ssh_target host_ip host_name host_user host_port extra

  IFS=, read -r ssh_target host_ip host_name host_user host_port extra <<< "$spec"
  [[ -z "${extra:-}" ]] || die "--node expects exactly 5 comma-separated fields"
  [[ -n "${ssh_target:-}" ]] || die "--node ssh_target is empty"
  [[ -n "${host_ip:-}" ]] || die "--node host_ip is empty"
  [[ -n "${host_name:-}" ]] || die "--node host_name is empty"
  [[ -n "${host_user:-}" ]] || die "--node host_user is empty"
  [[ -n "${host_port:-}" ]] || die "--node host_port is empty"

  SSH_TARGETS+=("$ssh_target")
  HOSTS_IPS+=("$host_ip")
  HOST_NAMES+=("$host_name")
  HOST_USERS+=("$host_user")
  HOST_PORTS+=("$host_port")
}

add_nodes_file_arg() {
  local file="$1"
  local line
  local node_count=0

  [[ -f "$file" ]] || die "--nodes-file not found: $file"

  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    line="$(printf '%s' "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -n "$line" ]] || continue
    [[ "${line:0:1}" != "#" ]] || continue

    add_node_arg "$line"
    ((node_count += 1))
  done < "$file"

  [[ "$node_count" -gt 0 ]] || die "--nodes-file has no nodes: $file"
}

parse_args() {
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      --node)
        [[ "$#" -ge 2 ]] || die "--node requires a value"
        use_node_input_mode "node" "--node"
        add_node_arg "$2"
        shift 2
        ;;
      --nodes-file)
        [[ "$#" -ge 2 ]] || die "--nodes-file requires a value"
        use_node_input_mode "node" "--nodes-file"
        add_nodes_file_arg "$2"
        shift 2
        ;;
      --ssh-targets)
        [[ "$#" -ge 2 ]] || die "--ssh-targets requires a value"
        use_node_input_mode "array" "--ssh-targets"
        set_array_arg SSH_TARGETS "--ssh-targets" "$2"
        shift 2
        ;;
      --hosts-ips)
        [[ "$#" -ge 2 ]] || die "--hosts-ips requires a value"
        use_node_input_mode "array" "--hosts-ips"
        set_array_arg HOSTS_IPS "--hosts-ips" "$2"
        shift 2
        ;;
      --host-names)
        [[ "$#" -ge 2 ]] || die "--host-names requires a value"
        use_node_input_mode "array" "--host-names"
        set_array_arg HOST_NAMES "--host-names" "$2"
        shift 2
        ;;
      --host-users)
        [[ "$#" -ge 2 ]] || die "--host-users requires a value"
        use_node_input_mode "array" "--host-users"
        set_array_arg HOST_USERS "--host-users" "$2"
        shift 2
        ;;
      --host-ports)
        [[ "$#" -ge 2 ]] || die "--host-ports requires a value"
        use_node_input_mode "array" "--host-ports"
        set_array_arg HOST_PORTS "--host-ports" "$2"
        shift 2
        ;;
      --ssh-user)
        [[ "$#" -ge 2 ]] || die "--ssh-user requires a value"
        SSH_USER="$2"
        shift 2
        ;;
      --ssh-port)
        [[ "$#" -ge 2 ]] || die "--ssh-port requires a value"
        SSH_PORT="$2"
        shift 2
        ;;
      --verify-remote-login)
        [[ "$#" -ge 2 ]] || die "--verify-remote-login requires a value"
        VERIFY_REMOTE_LOGIN="$(parse_bool "$2")"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "unknown argument: $1"
        ;;
    esac
  done
}

cleanup() {
  if [[ -n "${WORKDIR:-}" && -d "$WORKDIR" ]]; then
    rm -rf "$WORKDIR"
  fi
}

log() {
  printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*" >&2
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

remote_spec() {
  local target="$1"
  if [[ -n "$SSH_USER" && "$target" != *@* ]]; then
    printf '%s@%s' "$SSH_USER" "$target"
  else
    printf '%s' "$target"
  fi
}

run_ssh() {
  local target="$1"
  shift
  local -a args=("${SSH_COMMON_OPTS[@]}")
  if [[ -n "$SSH_PORT" ]]; then
    args+=(-p "$SSH_PORT")
  fi
  ssh "${args[@]}" "$(remote_spec "$target")" "$@"
}

run_scp_to_remote() {
  local source_file="$1"
  local target="$2"
  local remote_file="$3"
  local -a args=("${SSH_COMMON_OPTS[@]}")
  if [[ -n "$SSH_PORT" ]]; then
    args+=(-P "$SSH_PORT")
  fi
  scp "${args[@]}" "$source_file" "$(remote_spec "$target"):$remote_file"
}

validate_config() {
  local count="${#SSH_TARGETS[@]}"
  local i

  [[ "$count" -gt 0 ]] || die "SSH_TARGETS is empty"
  [[ "${#HOSTS_IPS[@]}" -eq "$count" ]] || die "HOSTS_IPS count must match SSH_TARGETS count"
  [[ "${#HOST_NAMES[@]}" -eq "$count" ]] || die "HOST_NAMES count must match SSH_TARGETS count"
  [[ "${#HOST_USERS[@]}" -eq "$count" ]] || die "HOST_USERS count must match SSH_TARGETS count"
  [[ "${#HOST_PORTS[@]}" -eq "$count" ]] || die "HOST_PORTS count must match SSH_TARGETS count"

  for ((i = 0; i < count; i++)); do
    [[ -n "${SSH_TARGETS[$i]:-}" ]] || die "SSH_TARGETS[$i] is empty"
    [[ -n "${HOSTS_IPS[$i]:-}" ]] || die "HOSTS_IPS[$i] is empty"
    [[ -n "${HOST_NAMES[$i]:-}" ]] || die "HOST_NAMES[$i] is empty"
    [[ -n "${HOST_USERS[$i]:-}" ]] || die "HOST_USERS[$i] is empty"
    [[ "${HOST_PORTS[$i]:-}" =~ ^[0-9]+$ ]] || die "HOST_PORTS[$i] must be a number"
    (( HOST_PORTS[$i] >= 1 && HOST_PORTS[$i] <= 65535 )) || die "HOST_PORTS[$i] must be between 1 and 65535"
  done
}

check_local_connectivity() {
  local count="${#SSH_TARGETS[@]}"
  local i target hostname
  local -a failed_servers=()

  log "checking local SSH connectivity"
  for ((i = 0; i < count; i++)); do
    target="${SSH_TARGETS[$i]}"
    hostname="${HOST_NAMES[$i]}"

    if run_ssh "$target" true >/dev/null 2>&1; then
      log "local SSH OK: $hostname"
    else
      failed_servers+=("$hostname (target: $target, ssh: $(remote_spec "$target"))")
    fi
  done

  if [[ "${#failed_servers[@]}" -gt 0 ]]; then
    printf '\nLocal SSH connection failed for these servers:\n' >&2
    for item in "${failed_servers[@]}"; do
      printf '  - %s\n' "$item" >&2
    done
    return 1
  fi
}

ensure_remote_key_and_print_pubkey() {
  local target="$1"
  local remote_user="$2"

  run_ssh "$target" 'bash -s' -- "$remote_user" <<'REMOTE'
set -euo pipefail

remote_user="$1"

if [[ "$(id -un)" == "$remote_user" ]]; then
  SUDO=()
elif [[ "$(id -u)" -eq 0 ]]; then
  SUDO=()
elif command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
  SUDO=(sudo -n)
else
  echo "Need root or passwordless sudo to manage SSH files for user '$remote_user' on $(hostname)" >&2
  exit 1
fi

home_dir="$(awk -F: -v user="$remote_user" '$1 == user { print $6; exit }' /etc/passwd)"
if [[ -z "$home_dir" ]]; then
  echo "User '$remote_user' does not exist on $(hostname)" >&2
  exit 1
fi

run_as_remote_user() {
  if [[ "$(id -un)" == "$remote_user" || "$(id -u)" -eq 0 ]]; then
    "$@"
  else
    "${SUDO[@]}" -u "$remote_user" "$@"
  fi
}

"${SUDO[@]}" mkdir -p "$home_dir/.ssh"
"${SUDO[@]}" chmod 700 "$home_dir/.ssh"
"${SUDO[@]}" chown "$remote_user" "$home_dir/.ssh" 2>/dev/null || true

pubkey=""
if [[ -s "$home_dir/.ssh/id_ed25519" && -s "$home_dir/.ssh/id_ed25519.pub" ]]; then
  pubkey="$home_dir/.ssh/id_ed25519.pub"
elif [[ -s "$home_dir/.ssh/id_rsa" && -s "$home_dir/.ssh/id_rsa.pub" ]]; then
  pubkey="$home_dir/.ssh/id_rsa.pub"
else
  command -v ssh-keygen >/dev/null 2>&1 || {
    echo "ssh-keygen not found on $(hostname)" >&2
    exit 1
  }

  echo "creating ed25519 key for $remote_user on $(hostname)" >&2
  run_as_remote_user ssh-keygen -q -t ed25519 -N "" -f "$home_dir/.ssh/id_ed25519" -C "$remote_user@$(hostname)"
  pubkey="$home_dir/.ssh/id_ed25519.pub"
fi

"${SUDO[@]}" chmod 600 "$home_dir/.ssh/id_ed25519" "$home_dir/.ssh/id_rsa" 2>/dev/null || true
"${SUDO[@]}" chmod 644 "$home_dir/.ssh/id_ed25519.pub" "$home_dir/.ssh/id_rsa.pub" 2>/dev/null || true
"${SUDO[@]}" chown "$remote_user" "$home_dir/.ssh/id_ed25519" "$home_dir/.ssh/id_ed25519.pub" "$home_dir/.ssh/id_rsa" "$home_dir/.ssh/id_rsa.pub" 2>/dev/null || true

"${SUDO[@]}" cat "$pubkey"
REMOTE
}

install_hosts_keys_and_known_hosts() {
  local target="$1"
  local local_hosts_file="$2"
  local local_pubkeys_file="$3"
  local local_ssh_config_file="$4"
  local remote_user="$5"
  local remote_hosts_file="/tmp/setup_cluster_hosts.$$"
  local remote_pubkeys_file="/tmp/setup_cluster_pubkeys.$$"
  local remote_ssh_config_file="/tmp/setup_cluster_ssh_config.$$"

  run_scp_to_remote "$local_hosts_file" "$target" "$remote_hosts_file" >/dev/null
  run_scp_to_remote "$local_pubkeys_file" "$target" "$remote_pubkeys_file" >/dev/null
  run_scp_to_remote "$local_ssh_config_file" "$target" "$remote_ssh_config_file" >/dev/null

  run_ssh "$target" 'bash -s' -- "$remote_hosts_file" "$remote_pubkeys_file" "$remote_ssh_config_file" "$remote_user" <<'REMOTE'
set -euo pipefail

remote_hosts_file="$1"
remote_pubkeys_file="$2"
remote_ssh_config_file="$3"
remote_user="$4"
managed_begin="# BEGIN managed by setup_cluster_ssh.sh"
managed_end="# END managed by setup_cluster_ssh.sh"

if [[ "$(id -u)" -eq 0 ]]; then
  SUDO=()
elif command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
  SUDO=(sudo -n)
else
  echo "Need root or passwordless sudo to update /etc/hosts and manage SSH files for user '$remote_user' on $(hostname)" >&2
  exit 1
fi

home_dir="$(awk -F: -v user="$remote_user" '$1 == user { print $6; exit }' /etc/passwd)"
if [[ -z "$home_dir" ]]; then
  echo "User '$remote_user' does not exist on $(hostname)" >&2
  exit 1
fi

"${SUDO[@]}" mkdir -p "$home_dir/.ssh"
"${SUDO[@]}" chmod 700 "$home_dir/.ssh"
"${SUDO[@]}" chown "$remote_user" "$home_dir/.ssh" 2>/dev/null || true
"${SUDO[@]}" touch "$home_dir/.ssh/authorized_keys"
"${SUDO[@]}" chmod 600 "$home_dir/.ssh/authorized_keys"
"${SUDO[@]}" chown "$remote_user" "$home_dir/.ssh/authorized_keys" 2>/dev/null || true

while IFS= read -r key; do
  [[ -n "$key" ]] || continue
  case "$key" in
    ssh-ed25519\ *|ssh-rsa\ *) ;;
    *) continue ;;
  esac
  "${SUDO[@]}" grep -qxF "$key" "$home_dir/.ssh/authorized_keys" ||
    printf '%s\n' "$key" | "${SUDO[@]}" tee -a "$home_dir/.ssh/authorized_keys" >/dev/null
done < "$remote_pubkeys_file"
"${SUDO[@]}" chmod 600 "$home_dir/.ssh/authorized_keys"
"${SUDO[@]}" chown "$remote_user" "$home_dir/.ssh/authorized_keys" 2>/dev/null || true

ssh_config_path="$home_dir/.ssh/config"
ssh_config_tmp="$(mktemp /tmp/ssh_config.XXXXXX)"
ssh_config_new="${ssh_config_tmp}.new"

if [[ -f "$ssh_config_path" ]]; then
  "${SUDO[@]}" cp "$ssh_config_path" "$ssh_config_tmp"
else
  : > "$ssh_config_tmp"
fi

awk -v begin="$managed_begin" -v end="$managed_end" '
  $0 == begin { skip = 1; next }
  $0 == end { skip = 0; next }
  !skip { print }
' "$ssh_config_tmp" > "$ssh_config_new"

printf '\n' >> "$ssh_config_new"
cat "$remote_ssh_config_file" >> "$ssh_config_new"

if [[ "$(id -u)" -eq 0 ]]; then
  cat "$ssh_config_new" > "$ssh_config_path"
else
  "${SUDO[@]}" cp "$ssh_config_new" "$ssh_config_path"
fi
"${SUDO[@]}" chmod 600 "$ssh_config_path"
"${SUDO[@]}" chown "$remote_user" "$ssh_config_path" 2>/dev/null || true

hosts_tmp="$(mktemp /tmp/etc_hosts.XXXXXX)"
hosts_new="${hosts_tmp}.new"

if [[ -f /etc/hosts ]]; then
  "${SUDO[@]}" cp /etc/hosts "$hosts_tmp"
else
  : > "$hosts_tmp"
fi

awk -v begin="$managed_begin" -v end="$managed_end" '
  $0 == begin { skip = 1; next }
  $0 == end { skip = 0; next }
  !skip { print }
' "$hosts_tmp" > "$hosts_new"

printf '\n' >> "$hosts_new"
cat "$remote_hosts_file" >> "$hosts_new"

if [[ "$(id -u)" -eq 0 ]]; then
  cat "$hosts_new" > /etc/hosts
else
  "${SUDO[@]}" cp "$hosts_new" /etc/hosts
fi
"${SUDO[@]}" chmod 644 /etc/hosts 2>/dev/null || true

if command -v ssh-keyscan >/dev/null 2>&1; then
  known_tmp="$(mktemp /tmp/known_hosts.XXXXXX)"
  awk '
    $1 == "Host" { host = $2; ip = ""; port = "" }
    $1 == "HostName" { ip = $2 }
    $1 == "Port" {
      port = $2
      if (host != "" && ip != "" && port != "") {
        print ip, host, port
      }
    }
  ' "$remote_ssh_config_file" |
    while read -r ip host port; do
      ssh-keyscan -T 5 -p "$port" "$host" "$ip" >> "$known_tmp" 2>/dev/null || true
    done

  if [[ -s "$known_tmp" ]]; then
    "${SUDO[@]}" touch "$home_dir/.ssh/known_hosts"
    "${SUDO[@]}" chmod 644 "$home_dir/.ssh/known_hosts"
    known_merged="$(mktemp /tmp/known_hosts_merged.XXXXXX)"
    "${SUDO[@]}" cat "$home_dir/.ssh/known_hosts" "$known_tmp" | sort -u > "$known_merged"
    if [[ "$(id -u)" -eq 0 ]]; then
      cat "$known_merged" > "$home_dir/.ssh/known_hosts"
    else
      "${SUDO[@]}" cp "$known_merged" "$home_dir/.ssh/known_hosts"
    fi
    "${SUDO[@]}" chmod 644 "$home_dir/.ssh/known_hosts"
    "${SUDO[@]}" chown "$remote_user" "$home_dir/.ssh/known_hosts" 2>/dev/null || true
    rm -f "$known_merged"
  fi
  rm -f "$known_tmp"
else
  echo "ssh-keyscan not found on $(hostname), skipped known_hosts update" >&2
fi

rm -f "$remote_hosts_file" "$remote_pubkeys_file" "$remote_ssh_config_file" "$hosts_tmp" "$hosts_new" "$ssh_config_tmp" "$ssh_config_new"
REMOTE
}

verify_remote_login() {
  local target="$1"
  local source_hostname="$2"
  local source_user="$3"
  local host
  shift 3

  if ! run_ssh "$target" 'bash -s' -- "$source_hostname" "$source_user" "$@" <<'REMOTE'
set -euo pipefail

source_hostname="$1"
source_user="$2"
shift 2

if [[ "$(id -un)" == "$source_user" ]]; then
  RUN_AS=()
elif command -v sudo >/dev/null 2>&1 && sudo -n -u "$source_user" true 2>/dev/null; then
  RUN_AS=(sudo -n -u "$source_user")
elif [[ "$(id -u)" -eq 0 && -x "$(command -v runuser 2>/dev/null)" ]]; then
  RUN_AS=(runuser -u "$source_user" --)
else
  for host in "$@"; do
    [[ "$host" != "$source_hostname" ]] || continue
    printf '%s -> %s (cannot run ssh as user %s)\n' "$source_hostname" "$host" "$source_user"
  done
  exit 0
fi

for host in "$@"; do
  [[ "$host" != "$source_hostname" ]] || continue
  if "${RUN_AS[@]}" ssh \
    -o BatchMode=yes \
    -o PasswordAuthentication=no \
    -o ConnectTimeout=8 \
    -o StrictHostKeyChecking=yes \
    "$host" true 2>/dev/null; then
    echo "OK: $source_hostname -> $host" >&2
  else
    printf '%s -> %s\n' "$source_hostname" "$host"
  fi
done

exit 0
REMOTE
  then
    for host in "$@"; do
      [[ "$host" != "$source_hostname" ]] || continue
      printf '%s -> %s (could not run verification on source host)\n' "$source_hostname" "$host"
    done
  fi
}

main() {
  validate_config
  check_local_connectivity

  local server_count="${#SSH_TARGETS[@]}"

  WORKDIR="$(mktemp -d /tmp/setup_cluster_ssh.XXXXXX)"
  trap cleanup EXIT

  local hosts_file="$WORKDIR/hosts_block"
  local ssh_config_file="$WORKDIR/ssh_config_block"
  local pubkeys_file="$WORKDIR/public_keys"
  local -a hostnames=()
  local -a remote_login_failures=()
  local i target hostname user pubkey failure

  {
    printf '%s\n' "$MANAGED_BEGIN"
    for ((i = 0; i < server_count; i++)); do
      printf '%s %s\n' "${HOSTS_IPS[$i]}" "${HOST_NAMES[$i]}"
    done
    printf '%s\n' "$MANAGED_END"
  } > "$hosts_file"

  {
    printf '%s\n' "$MANAGED_BEGIN"
    for ((i = 0; i < server_count; i++)); do
      printf 'Host %s\n' "${HOST_NAMES[$i]}"
      printf '  HostName %s\n' "${HOSTS_IPS[$i]}"
      printf '  User %s\n' "${HOST_USERS[$i]}"
      printf '  Port %s\n' "${HOST_PORTS[$i]}"
      printf '\n'
    done
    printf '%s\n' "$MANAGED_END"
  } > "$ssh_config_file"

  for ((i = 0; i < server_count; i++)); do
    hostnames+=("${HOST_NAMES[$i]}")
  done

  : > "$pubkeys_file"

  log "checking/generating remote SSH keys"
  for ((i = 0; i < server_count; i++)); do
    target="${SSH_TARGETS[$i]}"
    hostname="${HOST_NAMES[$i]}"
    user="${HOST_USERS[$i]}"
    log "key check: $target ($hostname, user $user)"
    pubkey="$(ensure_remote_key_and_print_pubkey "$target" "$user" | awk '/^ssh-(ed25519|rsa) / { key = $0 } END { if (key) print key }')"
    [[ -n "$pubkey" ]] || die "Could not read public key for user $user from $target"
    printf '%s\n' "$pubkey" >> "$pubkeys_file"
  done

  sort -u "$pubkeys_file" -o "$pubkeys_file"

  log "updating /etc/hosts, authorized_keys, and known_hosts on every server"
  for ((i = 0; i < server_count; i++)); do
    target="${SSH_TARGETS[$i]}"
    hostname="${HOST_NAMES[$i]}"
    user="${HOST_USERS[$i]}"
    log "install: $target ($hostname, user $user)"
    install_hosts_keys_and_known_hosts "$target" "$hosts_file" "$pubkeys_file" "$ssh_config_file" "$user"
  done

  if [[ "$VERIFY_REMOTE_LOGIN" == "true" ]]; then
    log "verifying remote-to-remote SSH login"
    for ((i = 0; i < server_count; i++)); do
      target="${SSH_TARGETS[$i]}"
      hostname="${HOST_NAMES[$i]}"
      user="${HOST_USERS[$i]}"
      log "verify from: $hostname as $user"
      while IFS= read -r failure; do
        [[ -n "$failure" ]] || continue
        remote_login_failures+=("$failure")
      done < <(verify_remote_login "$target" "$hostname" "$user" "${hostnames[@]}")
    done

    if [[ "${#remote_login_failures[@]}" -gt 0 ]]; then
      printf '\nRemote passwordless SSH failed for these hostname pairs:\n' >&2
      for failure in "${remote_login_failures[@]}"; do
        printf '  - %s\n' "$failure" >&2
      done
      exit 1
    fi

    log "remote-to-remote SSH verification passed"
  fi

  log "done"
}

parse_args "$@"
main