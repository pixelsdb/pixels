#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
ETCD_VERSION="${ETCD_VERSION:-3.3.4}"
ETCD_ARCHIVE="${ETCD_ARCHIVE:-$REPO_ROOT/scripts/tars/etcd-v$ETCD_VERSION-linux-amd64.tar.xz}"
ETCD_INSTALL_PARENT="${ETCD_INSTALL_PARENT:-$HOME/opt}"
ETCD_INSTALL_DIR="${ETCD_INSTALL_DIR:-$ETCD_INSTALL_PARENT/etcd-v$ETCD_VERSION-linux-amd64-bin}"
ETCD_HOME_LINK="${ETCD_HOME_LINK:-$ETCD_INSTALL_PARENT/etcd}"
PROFILE_FILE="${PROFILE_FILE:-$HOME/.bashrc}"
START_ETCD="${START_ETCD:-true}"
ETCD_ENDPOINT="${ETCD_ENDPOINT:-http://127.0.0.1:2379}"

log() {
  printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"
}

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

sudo_cmd() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
  elif command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
    sudo -n "$@"
  else
    fail "run as root or configure passwordless sudo for this user"
  fi
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "$1 command not found"
}

install_go_if_missing() {
  if command -v go >/dev/null 2>&1; then
    log "Go is already installed: $(go version)"
    return
  fi

  require_command apt-get
  log "installing golang"
  sudo_cmd apt-get update
  sudo_cmd env DEBIAN_FRONTEND=noninteractive apt-get install -y golang
}

install_etcd() {
  require_command tar
  [[ -f "$ETCD_ARCHIVE" ]] || fail "etcd archive not found: $ETCD_ARCHIVE"

  mkdir -p "$ETCD_INSTALL_PARENT"

  if [[ -d "$ETCD_INSTALL_DIR" ]]; then
    log "etcd install directory already exists: $ETCD_INSTALL_DIR"
  else
    log "extracting etcd archive to $ETCD_INSTALL_PARENT"
    tar -xJf "$ETCD_ARCHIVE" -C "$ETCD_INSTALL_PARENT"
  fi

  [[ -x "$ETCD_INSTALL_DIR/etcd" ]] || fail "etcd binary not found: $ETCD_INSTALL_DIR/etcd"
  [[ -x "$ETCD_INSTALL_DIR/etcdctl" ]] || fail "etcdctl binary not found: $ETCD_INSTALL_DIR/etcdctl"

  ln -sfn "$ETCD_INSTALL_DIR" "$ETCD_HOME_LINK"
}

persist_environment() {
  local line_api line_home line_path

  line_api='export ETCDCTL_API=3'
  line_home="export ETCD=$ETCD_INSTALL_DIR"
  line_path='export PATH=$PATH:$ETCD'

  touch "$PROFILE_FILE"

  if ! grep -qxF "$line_api" "$PROFILE_FILE" 2>/dev/null; then
    log "persisting ETCDCTL_API in $PROFILE_FILE"
    printf '\n%s\n' "$line_api" >> "$PROFILE_FILE"
  fi

  if ! grep -qxF "$line_home" "$PROFILE_FILE" 2>/dev/null; then
    log "persisting ETCD in $PROFILE_FILE"
    printf '%s\n%s\n' "$line_home" "$line_path" >> "$PROFILE_FILE"
  fi

  export ETCDCTL_API=3
  export ETCD="$ETCD_INSTALL_DIR"
  export PATH="$PATH:$ETCD"
}

start_etcd() {
  [[ "$START_ETCD" == "true" ]] || {
    log "etcd startup disabled"
    return
  }

  if "$ETCD_INSTALL_DIR/etcdctl" --endpoints="$ETCD_ENDPOINT" endpoint health >/dev/null 2>&1; then
    log "etcd is already healthy at $ETCD_ENDPOINT"
    return
  fi

  if [[ -x "$ETCD_HOME_LINK/start-etcd.sh" ]]; then
    log "starting etcd with $ETCD_HOME_LINK/start-etcd.sh"
    (cd "$ETCD_HOME_LINK" && ./start-etcd.sh)
  else
    log "start-etcd.sh not found; starting etcd with default localhost settings"
    nohup "$ETCD_INSTALL_DIR/etcd" >/tmp/pixels-etcd.log 2>&1 &
  fi
}

verify_etcd() {
  local attempt

  for attempt in {1..20}; do
    if "$ETCD_INSTALL_DIR/etcdctl" --endpoints="$ETCD_ENDPOINT" endpoint health >/dev/null 2>&1; then
      log "etcd verification passed: $ETCD_ENDPOINT"
      "$ETCD_INSTALL_DIR/etcdctl" --endpoints="$ETCD_ENDPOINT" endpoint health
      return
    fi
    sleep 1
  done

  fail "etcd endpoint is not healthy: $ETCD_ENDPOINT"
}

main() {
  install_go_if_missing
  install_etcd
  persist_environment
  start_etcd
  verify_etcd
}

main "$@"
