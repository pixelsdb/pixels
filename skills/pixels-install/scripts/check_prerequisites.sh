#!/usr/bin/env bash
if [ -z "${BASH_VERSION:-}" ]; then
  printf 'ERROR: pixels-install scripts must be executed by Bash; do not run this script with zsh.\n' >&2
  exit 1
fi
if [ "${BASH_SOURCE[0]}" != "$0" ]; then
  printf 'ERROR: do not source pixels-install installer scripts; execute this script directly with Bash.\n' >&2
  return 1 2>/dev/null || exit 1
fi
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
CHECK_PORTS="${CHECK_PORTS:-18888 18889 18893 2379 2380}"
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

check_shell() {
  local shell_path shell_name

  shell_path="$(login_shell_path 2>/dev/null || true)"
  if [[ -z "$shell_path" ]]; then
    result_record shell fail "could not determine the current account's login shell"
    return
  fi

  shell_name="$(login_shell_name 2>/dev/null || true)"
  if [[ -z "$shell_name" ]]; then
    result_record shell fail "unsupported login shell: $shell_path (only bash and zsh are supported)"
    return
  fi

  if ! command -v bash >/dev/null 2>&1; then
    result_record shell fail "Bash is required to run pixels-install scripts but was not found on PATH"
    return
  fi

  result_record shell ok "$shell_name login shell detected at $shell_path; Bash runtime is available"
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
    if ssh "${ssh_args[@]}" "$(ssh_target "$host")" 'bash -s' >/dev/null 2>&1 <<'REMOTE'
set -euo pipefail

user="$(id -un)"
shell_path=""
if command -v getent >/dev/null 2>&1; then
  shell_path="$(getent passwd "$user" 2>/dev/null | awk -F: 'NR == 1 { print $7; exit }')"
fi
if [[ -z "$shell_path" && -r /etc/passwd ]]; then
  shell_path="$(awk -F: -v account="$user" '$1 == account { print $7; exit }' /etc/passwd)"
fi

[[ -n "$shell_path" ]] || exit 1
case "$(basename "$shell_path")" in
  bash|zsh)
    ;;
  *)
    exit 1
    ;;
esac
command -v "$(basename "$shell_path")" >/dev/null 2>&1
command -v bash >/dev/null 2>&1
REMOTE
    then
      result_record "ssh:$host" ok "reachable; remote login shell is bash or zsh and Bash is available"
    else
      result_record "ssh:$host" fail "SSH or remote shell validation failed"
    fi
  done
}

main() {
  require_command awk
  require_command df
  require_command uname

  result_reset
  log "checking prerequisites"

  check_shell
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
