#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/shell_env.sh
source "$SCRIPT_DIR/lib/shell_env.sh"

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
AGENT_DIR="${AGENT_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
DEPLOYMENT_FILE="${DEPLOYMENT_FILE:-$AGENT_DIR/deployment.env}"
USE_DEPLOYMENT_FILE="${USE_DEPLOYMENT_FILE:-true}"
if [[ "$USE_DEPLOYMENT_FILE" == "true" && -f "$DEPLOYMENT_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$DEPLOYMENT_FILE"
  set +a
fi
# Picks up JAVA_HOME/MAVEN_HOME from earlier, separately invoked install_jdk.sh
# / install_maven.sh runs so `mvn` (used by install.sh below) is on PATH here.
# load_toolchain_env preserves the confirmed PIXELS_HOME above.
load_toolchain_env
PIXELS_HOME="${PIXELS_HOME:-$HOME/opt/pixels}"
MYSQL_CONNECTOR_VERSION="${MYSQL_CONNECTOR_VERSION:-8.0.33}"
MYSQL_CONNECTOR_JAR="${MYSQL_CONNECTOR_JAR:-$PIXELS_HOME/lib/mysql-connector-j-$MYSQL_CONNECTOR_VERSION.jar}"
MYSQL_CONNECTOR_URL="${MYSQL_CONNECTOR_URL:-https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/$MYSQL_CONNECTOR_VERSION/mysql-connector-j-$MYSQL_CONNECTOR_VERSION.jar}"
AUTO_CONFIRM_INSTALL="${AUTO_CONFIRM_INSTALL:-true}"
ASSUME_YES="${ASSUME_YES:-false}"
CONFIRM_PIXELS_INSTALL="${CONFIRM_PIXELS_INSTALL:-}"

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

confirm_pixels_install() {
  local reply

  case "$CONFIRM_PIXELS_INSTALL" in
    true) return ;;
    false) fail "CONFIRM_PIXELS_INSTALL=false" ;;
  esac
  [[ "$ASSUME_YES" == "true" ]] && return

  printf '\nPixels install summary:\n'
  printf '  source repo: %s\n' "$REPO_ROOT"
  printf '  PIXELS_HOME: %s\n' "$PIXELS_HOME"
  printf '  deployment file: %s\n\n' "$DEPLOYMENT_FILE"

  [[ -t 0 ]] || fail "Pixels install location must be confirmed; set CONFIRM_PIXELS_INSTALL=true after reviewing PIXELS_HOME"
  read -r -p "Install Pixels to this PIXELS_HOME? [y/N]: " reply
  [[ "$reply" =~ ^[Yy]$ ]] || fail "aborted Pixels install"
}

persist_pixels_home() {
  local profile_file

  mkdir -p "$PIXELS_HOME"
  profile_file="$(detect_profile_file)"

  log "persisting PIXELS_HOME=$PIXELS_HOME in $profile_file"
  persist_export "$profile_file" PIXELS_HOME "$PIXELS_HOME"
  persist_toolchain_var PIXELS_HOME "$PIXELS_HOME"

  export PIXELS_HOME
}

run_pixels_install() {
  local auto_confirm="$AUTO_CONFIRM_INSTALL"

  [[ -x "$REPO_ROOT/install.sh" ]] || fail "install.sh not found or not executable: $REPO_ROOT/install.sh"
  require_command mvn

  # install.sh only prompts about overwriting $PIXELS_HOME/bin|sbin|etc when
  # they are non-empty (safe to auto-confirm per CLAUDE.md - those are
  # generated runtime artifacts). But when pixels.properties or
  # pixels-cpp.properties already exist, it also prompts to add new config
  # options, remove deprecated ones, or overwrite pixels-cpp.properties
  # outright - CLAUDE.md/AGENTS.md explicitly say not to do any of that
  # without the user's say-so. `yes y` cannot answer some prompts and not
  # others, so on a re-install over an existing PIXELS_HOME we force the
  # interactive path so a human reviews every prompt instead of silently
  # touching user config.
  if [[ "$auto_confirm" == "true" ]] &&
     { [[ -f "$PIXELS_HOME/etc/pixels.properties" ]] || [[ -f "$PIXELS_HOME/etc/pixels-cpp.properties" ]]; }; then
    log "existing config found under $PIXELS_HOME/etc; install.sh would prompt about modifying pixels.properties/pixels-cpp.properties, so running it interactively instead of auto-confirming (see CLAUDE.md: preserve user configuration by default)"
    auto_confirm="false"
  fi

  log "installing Pixels from $REPO_ROOT to $PIXELS_HOME"
  if [[ "$auto_confirm" == "true" ]]; then
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
  confirm_pixels_install
  persist_pixels_home
  run_pixels_install
  install_mysql_connector
  verify_install
}

main "$@"
