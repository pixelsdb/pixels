#!/usr/bin/env bash
set -euo pipefail

MIN_MEMORY_MB="${MIN_MEMORY_MB:-4096}"
MIN_DISK_GB="${MIN_DISK_GB:-20}"
CHECK_PORTS="${CHECK_PORTS:-18888 18889 18893 2379 3306}"
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

warn() {
  printf 'WARN: %s\n' "$*" >&2
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

check_os() {
  [[ -r /etc/os-release ]] || fail "/etc/os-release not found"
  . /etc/os-release

  if [[ "${ID:-}" != "ubuntu" ]]; then
    warn "expected Ubuntu, found '${PRETTY_NAME:-unknown}'"
  else
    log "OS: ${PRETTY_NAME:-Ubuntu}"
  fi

  arch="$(uname -m)"
  if [[ "$arch" != "x86_64" && "$arch" != "amd64" ]]; then
    warn "expected x86_64 architecture, found '$arch'"
  else
    log "architecture: $arch"
  fi
}

check_memory() {
  mem_mb="$(awk '/MemTotal/ { print int($2 / 1024) }' /proc/meminfo)"
  [[ -n "$mem_mb" ]] || fail "could not read system memory"

  if (( mem_mb < MIN_MEMORY_MB )); then
    fail "memory is ${mem_mb}MB, expected at least ${MIN_MEMORY_MB}MB"
  fi

  log "memory: ${mem_mb}MB"
}

check_disk() {
  disk_gb="$(df -BG / | awk 'NR == 2 { gsub(/G/, "", $4); print $4 }')"
  [[ -n "$disk_gb" ]] || fail "could not read free disk space for /"

  if (( disk_gb < MIN_DISK_GB )); then
    fail "free disk on / is ${disk_gb}GB, expected at least ${MIN_DISK_GB}GB"
  fi

  log "free disk on /: ${disk_gb}GB"
}

check_privilege() {
  if [[ "$(id -u)" -eq 0 ]]; then
    log "privilege: running as root"
    return
  fi

  if command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
    log "privilege: passwordless sudo is available"
    return
  fi

  fail "run as root or configure passwordless sudo for this user"
}

check_ports() {
  local port

  require_command ss
  for port in $CHECK_PORTS; do
    [[ "$port" =~ ^[0-9]+$ ]] || fail "invalid port in CHECK_PORTS: $port"
    if ss -ltn "sport = :$port" | awk 'NR > 1 { found = 1 } END { exit(found ? 0 : 1) }'; then
      fail "port $port is already listening"
    fi
    log "port available: $port"
  done
}

check_hosts() {
  local host

  [[ -z "$CHECK_HOSTS" ]] && return
  require_command getent

  for host in $CHECK_HOSTS; do
    getent hosts "$host" >/dev/null || fail "host cannot be resolved: $host"
    log "host resolved: $host"
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

  [[ -z "$CHECK_SSH_HOSTS" ]] && return
  require_command ssh

  ssh_args=(-o BatchMode=yes -o ConnectTimeout=8 -o StrictHostKeyChecking=accept-new)
  if [[ -n "$SSH_PORT" ]]; then
    ssh_args+=(-p "$SSH_PORT")
  fi

  for host in $CHECK_SSH_HOSTS; do
    ssh "${ssh_args[@]}" "$(ssh_target "$host")" true >/dev/null 2>&1 || fail "SSH check failed: $host"
    log "SSH ok: $host"
  done
}

main() {
  log "checking prerequisites"
  require_command awk
  require_command df
  require_command uname

  check_os
  check_memory
  check_disk
  check_privilege
  check_ports
  check_hosts
  check_ssh_hosts

  log "prerequisite checks passed"
}

main "$@"
