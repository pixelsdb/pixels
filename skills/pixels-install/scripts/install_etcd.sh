#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/shell_env.sh
source "$SCRIPT_DIR/lib/shell_env.sh"

SKILL_DIR="${SKILL_DIR:-$(skill_dir)}"
STATE_DIR="${STATE_DIR:-$(state_dir)}"
REPO_ROOT="${REPO_ROOT:-$(require_repo_root)}"
ETCD_VERSION="${ETCD_VERSION:-3.3.4}"
ETCD_ARCHIVE="${ETCD_ARCHIVE:-$REPO_ROOT/scripts/tars/etcd-v$ETCD_VERSION-linux-amd64.tar.xz}"
ETCD_INSTALL_PARENT="${ETCD_INSTALL_PARENT:-$HOME/opt}"
ETCD_INSTALL_DIR="${ETCD_INSTALL_DIR:-$ETCD_INSTALL_PARENT/etcd-v$ETCD_VERSION-linux-amd64-bin}"
ETCD_HOME_LINK="${ETCD_HOME_LINK:-$ETCD_INSTALL_PARENT/etcd}"
START_ETCD="${START_ETCD:-true}"

# The shipped conf.yml hardcodes /home/ubuntu/... as data-dir. Always correct
# that path, but keep etcd localhost-only by default. For a cluster, set
# ETCD_ALLOW_REMOTE=true after confirming the host is on a trusted/private
# network and the security group/firewall only exposes 2379/2380 as intended.
ETCD_ALLOW_REMOTE="${ETCD_ALLOW_REMOTE:-false}"
ETCD_LISTEN_HOST="${ETCD_LISTEN_HOST:-127.0.0.1}"
ETCD_CLIENT_PORT="${ETCD_CLIENT_PORT:-2379}"
ETCD_PEER_PORT="${ETCD_PEER_PORT:-2380}"
# Host other servers should use to reach this etcd. Left empty, we try to
# auto-detect this host's primary non-loopback IP; set explicitly if that
# detection picks the wrong interface.
ETCD_ADVERTISE_HOST="${ETCD_ADVERTISE_HOST:-}"
ETCD_ENDPOINT="${ETCD_ENDPOINT:-http://127.0.0.1:$ETCD_CLIENT_PORT}"

# Installs and enables a systemd unit so etcd survives reboots, instead of
# only running for the lifetime of this script's background process. This
# writes outside $HOME (/etc/systemd/system) and enables an auto-start
# service, so - like changing MySQL root authentication or enabling remote
# DB access - we never do it silently: leave INSTALL_ETCD_SYSTEMD_SERVICE
# unset to be asked interactively, or set it to "true"/"false" to skip the
# prompt (e.g. from a non-interactive skill run that already has the user's
# go-ahead). ASSUME_YES=true also answers the prompt without a TTY.
INSTALL_ETCD_SYSTEMD_SERVICE="${INSTALL_ETCD_SYSTEMD_SERVICE:-}"
ASSUME_YES="${ASSUME_YES:-false}"
CONFIRM_ETCD_REMOTE_ACCESS="${CONFIRM_ETCD_REMOTE_ACCESS:-}"
ETCD_SERVICE_NAME="${ETCD_SERVICE_NAME:-etcd}"
SYSTEMD_UNIT_DIR="${SYSTEMD_UNIT_DIR:-/etc/systemd/system}"

ETCD_CONFIG_CHANGED=false
ETCD_SYSTEMD_INSTALLED=false

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

# docs/INSTALL.md's "Install etcd" section explicitly calls for installing
# golang before extracting the bundled etcd binary, even though the etcd
# 3.3.4 release used here ships a prebuilt binary that does not itself need
# a Go toolchain at runtime. We keep this step to match the documented flow.
install_go_if_missing() {
  if command -v go >/dev/null 2>&1; then
    log "Go is already installed: $(go version)"
    return
  fi

  require_command apt-get
  log "installing golang (per docs/INSTALL.md, Install etcd)"
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

# Sets one `key: value` line in conf.yml (etcd's config is flat YAML), adding
# it if missing. Uses `|` as the sed delimiter since values are URLs/paths
# containing `/`.
set_yaml_property() {
  local file="$1"
  local key="$2"
  local value="$3"
  local escaped_value

  escaped_value="$(printf '%s' "$value" | sed 's/[\\&|]/\\&/g')"

  if grep -qE "^${key}:" "$file"; then
    sed -i -E "s|^${key}:.*|${key}: ${escaped_value}|" "$file"
  else
    printf '%s: %s\n' "$key" "$value" >> "$file"
  fi
}

# Resolves the host other servers should use to reach this etcd: an explicit
# ETCD_ADVERTISE_HOST, or this machine's primary non-loopback IP.
detect_advertise_host() {
  if [[ -n "$ETCD_ADVERTISE_HOST" ]]; then
    printf '%s\n' "$ETCD_ADVERTISE_HOST"
    return
  fi

  local ip
  ip="$(hostname -I 2>/dev/null | awk '{print $1}')"
  [[ -n "$ip" ]] || ip="$(hostname -i 2>/dev/null | awk '{print $1}')"
  printf '%s\n' "${ip:-127.0.0.1}"
}

confirm_remote_access() {
  local reply

  [[ "$ETCD_ALLOW_REMOTE" == "true" ]] || return 0

  case "$CONFIRM_ETCD_REMOTE_ACCESS" in
    true) return 0 ;;
    false) fail "ETCD_ALLOW_REMOTE=true was requested, but CONFIRM_ETCD_REMOTE_ACCESS=false" ;;
  esac

  [[ "$ASSUME_YES" == "true" ]] && return 0

  if [[ ! -t 0 ]]; then
    fail "ETCD_ALLOW_REMOTE=true opens etcd client/peer listeners; set CONFIRM_ETCD_REMOTE_ACCESS=true after confirming private networking and firewall rules"
  fi

  read -r -p "Allow etcd to bind ${ETCD_LISTEN_HOST}:${ETCD_CLIENT_PORT}/${ETCD_PEER_PORT} for cluster access? Confirm only after checking private networking/firewall [y/N]: " reply
  [[ "$reply" =~ ^[Yy]$ ]] || fail "aborted remote etcd access"
}

configure_etcd_yaml() {
  local conf_file="$ETCD_HOME_LINK/conf.yml"
  local advertise_host etcd_name before_hash after_hash

  [[ -f "$conf_file" ]] || fail "etcd config not found: $conf_file"

  before_hash="$(md5sum "$conf_file" | awk '{print $1}')"

  # Always correct data-dir: the shipped default hardcodes /home/ubuntu/...,
  # which is wrong for any other user and any other PIXELS_HOME-style $HOME.
  set_yaml_property "$conf_file" data-dir "\"$ETCD_INSTALL_DIR/data\""

  etcd_name="$(sed -nE "s/^name:[[:space:]]*['\"]?([^'\"]*)['\"]?[[:space:]]*\$/\1/p" "$conf_file" | head -n1)"
  etcd_name="${etcd_name:-etcd0}"

  if [[ "$ETCD_ALLOW_REMOTE" == "true" ]]; then
    confirm_remote_access
    advertise_host="$(detect_advertise_host)"
    if [[ "$advertise_host" == "127.0.0.1" ]]; then
      warn "could not auto-detect a non-loopback IP for this host; etcd will advertise 127.0.0.1, which other servers cannot reach. Set ETCD_ADVERTISE_HOST explicitly to this host's real IP to fix this."
    fi

    log "binding etcd to $ETCD_LISTEN_HOST (client port $ETCD_CLIENT_PORT, peer port $ETCD_PEER_PORT) and advertising $advertise_host so other servers can reach it"

    set_yaml_property "$conf_file" listen-peer-urls "http://$ETCD_LISTEN_HOST:$ETCD_PEER_PORT"
    set_yaml_property "$conf_file" listen-client-urls "http://$ETCD_LISTEN_HOST:$ETCD_CLIENT_PORT"
    set_yaml_property "$conf_file" initial-advertise-peer-urls "http://$advertise_host:$ETCD_PEER_PORT"
    set_yaml_property "$conf_file" advertise-client-urls "http://$advertise_host:$ETCD_CLIENT_PORT"
    set_yaml_property "$conf_file" initial-cluster "\"$etcd_name=http://$advertise_host:$ETCD_PEER_PORT\""
  else
    log "ETCD_ALLOW_REMOTE=false; binding etcd to localhost only"
    set_yaml_property "$conf_file" listen-peer-urls "http://127.0.0.1:$ETCD_PEER_PORT"
    set_yaml_property "$conf_file" listen-client-urls "http://127.0.0.1:$ETCD_CLIENT_PORT"
    set_yaml_property "$conf_file" initial-advertise-peer-urls "http://127.0.0.1:$ETCD_PEER_PORT"
    set_yaml_property "$conf_file" advertise-client-urls "http://127.0.0.1:$ETCD_CLIENT_PORT"
    set_yaml_property "$conf_file" initial-cluster "\"$etcd_name=http://127.0.0.1:$ETCD_PEER_PORT\""
  fi

  after_hash="$(md5sum "$conf_file" | awk '{print $1}')"
  if [[ "$before_hash" != "$after_hash" ]]; then
    ETCD_CONFIG_CHANGED=true
    log "etcd config updated: $conf_file"
  else
    log "etcd config already matches the desired settings: $conf_file"
  fi
}

# Asks before installing the systemd unit, unless the caller already
# answered via INSTALL_ETCD_SYSTEMD_SERVICE or ASSUME_YES. Never installs it
# silently: this writes to /etc/systemd/system and enables an auto-start
# service, which is the same category of change as enabling MySQL remote
# access - it needs the user's explicit go-ahead, not just a script default.
confirm_systemd_service() {
  local reply

  case "$INSTALL_ETCD_SYSTEMD_SERVICE" in
    true) return 0 ;;
    false) return 1 ;;
  esac

  [[ "$ASSUME_YES" == "true" ]] && return 0

  if [[ ! -t 0 ]]; then
    log "not running interactively and INSTALL_ETCD_SYSTEMD_SERVICE/ASSUME_YES not set; skipping the etcd systemd unit (etcd will still be started, just not as an auto-start service). Set INSTALL_ETCD_SYSTEMD_SERVICE=true to install it non-interactively once the user has agreed."
    return 1
  fi

  read -r -p "Install a systemd unit so etcd starts automatically on boot (writes $SYSTEMD_UNIT_DIR/$ETCD_SERVICE_NAME.service, requires sudo)? [y/N]: " reply
  [[ "$reply" =~ ^[Yy]$ ]]
}

# Installs a systemd unit so etcd auto-starts on boot and restarts on
# failure, instead of only living as long as this script's background
# process. Resolves User/Group/paths from the account actually running this
# script - never hardcodes "ubuntu".
install_systemd_service() {
  local service_file="$SYSTEMD_UNIT_DIR/$ETCD_SERVICE_NAME.service"
  local exec_user exec_group

  if ! command -v systemctl >/dev/null 2>&1; then
    log "systemctl not found; skipping systemd unit (falling back to a manually backgrounded etcd process)"
    return
  fi

  if ! confirm_systemd_service; then
    log "skipping the etcd systemd unit (not confirmed); etcd will be started directly instead"
    return
  fi

  exec_user="$(id -un)"
  exec_group="$(id -gn)"

  log "writing $service_file (User=$exec_user Group=$exec_group)"

  sudo_cmd tee "$service_file" >/dev/null <<EOF
[Unit]
Description=Etcd service
After=network.target
After=network-online.target
Wants=network-online.target

[Service]
Type=notify
User=$exec_user
Group=$exec_group
ExecStart=$ETCD_HOME_LINK/etcd --config-file $ETCD_HOME_LINK/conf.yml
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

  sudo_cmd systemctl daemon-reload
  ETCD_SYSTEMD_INSTALLED=true
}

persist_environment() {
  local profile_file line_path

  profile_file="$(detect_profile_file)"
  line_path='export PATH=$PATH:$ETCD'

  log "persisting ETCDCTL_API and ETCD in $profile_file"
  persist_export "$profile_file" ETCDCTL_API 3
  persist_export "$profile_file" ETCD "$ETCD_INSTALL_DIR"
  persist_line "$profile_file" "$line_path"
  persist_toolchain_var ETCDCTL_API 3
  persist_toolchain_var ETCD "$ETCD_INSTALL_DIR"

  export ETCDCTL_API=3
  export ETCD="$ETCD_INSTALL_DIR"
  export PATH="$PATH:$ETCD"
}

start_etcd() {
  [[ "$START_ETCD" == "true" ]] || {
    log "etcd startup disabled"
    return
  }

  if [[ "$ETCD_SYSTEMD_INSTALLED" == "true" ]]; then
    if [[ "$ETCD_CONFIG_CHANGED" == "true" ]]; then
      log "etcd config changed; restarting $ETCD_SERVICE_NAME.service to apply it"
      sudo_cmd systemctl enable "$ETCD_SERVICE_NAME"
      sudo_cmd systemctl restart "$ETCD_SERVICE_NAME"
    elif "$ETCD_INSTALL_DIR/etcdctl" --endpoints="$ETCD_ENDPOINT" endpoint health >/dev/null 2>&1; then
      log "etcd is already healthy at $ETCD_ENDPOINT"
      sudo_cmd systemctl enable "$ETCD_SERVICE_NAME" >/dev/null 2>&1 || true
    else
      log "starting etcd via systemd ($ETCD_SERVICE_NAME.service)"
      sudo_cmd systemctl enable --now "$ETCD_SERVICE_NAME"
    fi
    return
  fi

  stop_existing_etcd_processes() {
    pkill -f "$ETCD_HOME_LINK/etcd" 2>/dev/null || true
    pkill -f "$ETCD_INSTALL_DIR/etcd" 2>/dev/null || true
    pkill -f "etcd --config-file=$ETCD_HOME_LINK/conf.yml" 2>/dev/null || true
    pkill -f "etcd --config-file=$ETCD_INSTALL_DIR/conf.yml" 2>/dev/null || true
    sleep 1
  }

  start_detached() {
    local work_dir="$1"
    shift

    if command -v setsid >/dev/null 2>&1; then
      (cd "$work_dir" && setsid -f "$@" >/tmp/pixels-etcd.log 2>&1 </dev/null)
    else
      warn "setsid not found; falling back to nohup background startup"
      (cd "$work_dir" && nohup "$@" >/tmp/pixels-etcd.log 2>&1 </dev/null &)
    fi
  }

  # No systemd unit available (e.g. containers without systemd, or
  # INSTALL_ETCD_SYSTEMD_SERVICE=false) - fall back to a manually
  # backgrounded process. start-etcd.sh itself just runs `etcd
  # --config-file=...` in the foreground, so it must be backgrounded here or
  # this script would hang forever waiting for etcd to exit.
  if [[ "$ETCD_CONFIG_CHANGED" != "true" ]] &&
     "$ETCD_INSTALL_DIR/etcdctl" --endpoints="$ETCD_ENDPOINT" endpoint health >/dev/null 2>&1; then
    log "etcd is already healthy at $ETCD_ENDPOINT"
    return
  fi

  log "stopping any previously started etcd process before direct startup"
  stop_existing_etcd_processes

  if [[ -x "$ETCD_HOME_LINK/start-etcd.sh" ]]; then
    log "starting etcd with $ETCD_HOME_LINK/start-etcd.sh"
    start_detached "$ETCD_HOME_LINK" ./start-etcd.sh
  else
    log "start-etcd.sh not found; starting etcd directly"
    start_detached "$ETCD_INSTALL_DIR" "$ETCD_INSTALL_DIR/etcd" --config-file "$ETCD_INSTALL_DIR/conf.yml"
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
  configure_etcd_yaml
  install_systemd_service
  start_etcd
  verify_etcd
}

main "$@"
