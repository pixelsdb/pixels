#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
PIXELS_HOME="${PIXELS_HOME:-$HOME/opt/pixels}"
PROFILE_FILE="${PROFILE_FILE:-$HOME/.bashrc}"
MYSQL_CONNECTOR_VERSION="${MYSQL_CONNECTOR_VERSION:-8.0.33}"
MYSQL_CONNECTOR_JAR="${MYSQL_CONNECTOR_JAR:-$PIXELS_HOME/lib/mysql-connector-j-$MYSQL_CONNECTOR_VERSION.jar}"
MYSQL_CONNECTOR_URL="${MYSQL_CONNECTOR_URL:-https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/$MYSQL_CONNECTOR_VERSION/mysql-connector-j-$MYSQL_CONNECTOR_VERSION.jar}"
AUTO_CONFIRM_INSTALL="${AUTO_CONFIRM_INSTALL:-true}"

log() {
  printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"
}

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "$1 command not found"
}

persist_pixels_home() {
  local line

  mkdir -p "$PIXELS_HOME"
  line="export PIXELS_HOME=$PIXELS_HOME"
  touch "$PROFILE_FILE"

  if ! grep -qxF "$line" "$PROFILE_FILE" 2>/dev/null; then
    log "persisting PIXELS_HOME in $PROFILE_FILE"
    printf '\n%s\n' "$line" >> "$PROFILE_FILE"
  fi

  export PIXELS_HOME
}

run_pixels_install() {
  [[ -x "$REPO_ROOT/install.sh" ]] || fail "install.sh not found or not executable: $REPO_ROOT/install.sh"
  require_command mvn

  log "installing Pixels from $REPO_ROOT to $PIXELS_HOME"
  if [[ "$AUTO_CONFIRM_INSTALL" == "true" ]]; then
    yes y | (cd "$REPO_ROOT" && ./install.sh)
  else
    (cd "$REPO_ROOT" && ./install.sh)
  fi
}

install_mysql_connector() {
  local existing

  mkdir -p "$PIXELS_HOME/lib"

  existing="$(find "$PIXELS_HOME/lib" -maxdepth 1 -name 'mysql-connector-j-*.jar' -print -quit 2>/dev/null || true)"
  if [[ -n "$existing" ]]; then
    log "MySQL JDBC connector already exists: $existing"
    return
  fi

  if [[ -f "$MYSQL_CONNECTOR_JAR" ]]; then
    log "MySQL JDBC connector already exists: $MYSQL_CONNECTOR_JAR"
    return
  fi

  require_command curl
  log "downloading MySQL JDBC connector $MYSQL_CONNECTOR_VERSION"
  curl -fL "$MYSQL_CONNECTOR_URL" -o "$MYSQL_CONNECTOR_JAR"
}

verify_install() {
  [[ -d "$PIXELS_HOME/bin" ]] || fail "missing directory: $PIXELS_HOME/bin"
  [[ -d "$PIXELS_HOME/sbin" ]] || fail "missing directory: $PIXELS_HOME/sbin"
  [[ -d "$PIXELS_HOME/etc" ]] || fail "missing directory: $PIXELS_HOME/etc"
  [[ -d "$PIXELS_HOME/lib" ]] || fail "missing directory: $PIXELS_HOME/lib"
  compgen -G "$PIXELS_HOME/bin/pixels-daemon-*-full.jar" >/dev/null || fail "pixels daemon jar not found in $PIXELS_HOME/bin"
  compgen -G "$PIXELS_HOME/sbin/pixels-cli-*-full.jar" >/dev/null || fail "pixels cli jar not found in $PIXELS_HOME/sbin"
  [[ -f "$PIXELS_HOME/etc/pixels.properties" ]] || fail "missing config: $PIXELS_HOME/etc/pixels.properties"
  find "$PIXELS_HOME/lib" -maxdepth 1 -name 'mysql-connector-j-*.jar' -print -quit | grep -q . || fail "MySQL JDBC connector not found in $PIXELS_HOME/lib"

  log "Pixels installation verified at $PIXELS_HOME"
}

main() {
  persist_pixels_home
  run_pixels_install
  install_mysql_connector
  verify_install
}

main "$@"
