#!/usr/bin/env bash
set -uo pipefail

# Validates OS, architecture, memory, disk, ports, host resolution,
# privilege, and optional SSH reachability. Runs every check and reports a
# structured summary at the end instead of stopping at the first failure
# (no `set -e`/hard `exit` inside individual checks), so the skill can see
# every problem in one pass and decide what to fix, instead of re-running
# this script once per failure to discover them one at a time.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/shell_env.sh
source "$SCRIPT_DIR/lib/shell_env.sh"

MIN_MEMORY_MB="${MIN_MEMORY_MB:-4096}"
MIN_DISK_GB="${MIN_DISK_GB:-20}"
CHECK_PORTS="${CHECK_PORTS:-18888 18889 18893 2379 2380 3306}"
CHECK_HOSTS="${CHECK_HOSTS:-}"
CHECK_SSH_HOSTS="${CHECK_SSH_HOSTS:-}"
SSH_USER="${SSH_USER:-}"
SSH_PORT="${SSH_PORT:-}"

log() {
  printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"
}

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

check_os() {
  if [[ ! -r /etc/os-release ]]; then
    result_record os fail "/etc/os-release not found"
    return
  fi
  . /etc/os-release

  if [[ "${ID:-}" != "ubuntu" ]]; then
    result_record os warn "expected Ubuntu, found '${PRETTY_NAME:-unknown}'"
  else
    result_record os ok "${PRETTY_NAME:-Ubuntu}"
  fi

  local arch
  arch="$(uname -m)"
  case "$arch" in
    x86_64|amd64)
      result_record arch ok "$arch"
      ;;
    aarch64|arm64)
      result_record arch warn "$arch (install_jdk.sh supports aarch64, but the documented Pixels + Trino path in docs/INSTALL.md is only verified on x86_64)"
      ;;
    *)
      result_record arch fail "expected x86_64 or aarch64 architecture, found '$arch'"
      ;;
  esac
}

check_memory() {
  local mem_mb
  mem_mb="$(awk '/MemTotal/ { print int($2 / 1024) }' /proc/meminfo)"
  if [[ -z "$mem_mb" ]]; then
    result_record memory fail "could not read system memory"
    return
  fi

  if (( mem_mb < MIN_MEMORY_MB )); then
    result_record memory fail "${mem_mb}MB, expected at least ${MIN_MEMORY_MB}MB"
  else
    result_record memory ok "${mem_mb}MB"
  fi
}

check_disk() {
  local disk_gb
  disk_gb="$(df -BG / | awk 'NR == 2 { gsub(/G/, "", $4); print $4 }')"
  if [[ -z "$disk_gb" ]]; then
    result_record disk fail "could not read free disk space for /"
    return
  fi

  if (( disk_gb < MIN_DISK_GB )); then
    result_record disk fail "${disk_gb}GB free on /, expected at least ${MIN_DISK_GB}GB"
  else
    result_record disk ok "${disk_gb}GB free on /"
  fi
}

check_privilege() {
  if [[ "$(id -u)" -eq 0 ]]; then
    result_record privilege ok "running as root"
    return
  fi

  if command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
    result_record privilege ok "passwordless sudo is available"
    return
  fi

  result_record privilege fail "not root and passwordless sudo is not configured for this user"
}

check_ports() {
  local port

  if ! command -v ss >/dev/null 2>&1; then
    result_record ports fail "ss command not found, cannot check port availability"
    return
  fi

  for port in $CHECK_PORTS; do
    if [[ ! "$port" =~ ^[0-9]+$ ]]; then
      result_record "port:$port" fail "invalid port in CHECK_PORTS: $port"
      continue
    fi
    if ss -ltn "sport = :$port" | awk 'NR > 1 { found = 1 } END { exit(found ? 0 : 1) }'; then
      result_record "port:$port" fail "already listening"
    else
      result_record "port:$port" ok "available"
    fi
  done
}

check_hosts() {
  local host

  if [[ -z "$CHECK_HOSTS" ]]; then
    return
  fi
  if ! command -v getent >/dev/null 2>&1; then
    result_record hosts fail "getent command not found, cannot resolve CHECK_HOSTS"
    return
  fi

  for host in $CHECK_HOSTS; do
    if getent hosts "$host" >/dev/null; then
      result_record "host:$host" ok "resolved"
    else
      result_record "host:$host" fail "cannot be resolved"
    fi
  done
}

ssh_target() {
  local host="$1"
  if [[ -n "$SSH_USER" && "$host" != *@* ]]; then
    printf '%s@%s' "$SSH_USER" "$host"
  else
    printf '%s' "$host"
  fi
}

check_ssh_hosts() {
  local host
  local -a ssh_args

  if [[ -z "$CHECK_SSH_HOSTS" ]]; then
    return
  fi
  if ! command -v ssh >/dev/null 2>&1; then
    result_record ssh fail "ssh command not found, cannot check CHECK_SSH_HOSTS"
    return
  fi

  ssh_args=(-o BatchMode=yes -o ConnectTimeout=8 -o StrictHostKeyChecking=accept-new)
  if [[ -n "$SSH_PORT" ]]; then
    ssh_args+=(-p "$SSH_PORT")
  fi

  for host in $CHECK_SSH_HOSTS; do
    if ssh "${ssh_args[@]}" "$(ssh_target "$host")" true >/dev/null 2>&1; then
      result_record "ssh:$host" ok "reachable"
    else
      result_record "ssh:$host" fail "SSH check failed"
    fi
  done
}

main() {
  require_command awk
  require_command df
  require_command uname

  result_reset
  log "checking prerequisites"

  check_os
  check_memory
  check_disk
  check_privilege
  check_ports
  check_hosts
  check_ssh_hosts

  result_emit_summary check_prerequisites
}

main "$@"
