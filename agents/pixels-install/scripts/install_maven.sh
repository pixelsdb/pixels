#!/usr/bin/env bash
set -euo pipefail

MIN_MAVEN_VERSION="${MIN_MAVEN_VERSION:-3.8.0}"
MAVEN_VERSION="${MAVEN_VERSION:-3.9.9}"
MAVEN_BASE_URL="${MAVEN_BASE_URL:-https://archive.apache.org/dist/maven/maven-3}"
MAVEN_INSTALL_DIR="${MAVEN_INSTALL_DIR:-/opt/apache-maven-$MAVEN_VERSION}"
MAVEN_HOME_LINK="${MAVEN_HOME_LINK:-/opt/maven}"
PROFILE_FILE="${PROFILE_FILE:-$HOME/.bashrc}"
TMP_DIR="${TMP_DIR:-/tmp}"

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

version_ge() {
  local current="$1"
  local required="$2"

  [[ "$(printf '%s\n%s\n' "$required" "$current" | sort -V | head -n1)" == "$required" ]]
}

maven_version() {
  command -v mvn >/dev/null 2>&1 || return 1
  mvn -v 2>/dev/null | awk '/Apache Maven/ { print $3; exit }'
}

maven_satisfies_requirement() {
  local version

  version="$(maven_version || true)"
  [[ -n "$version" ]] || return 1
  version_ge "$version" "$MIN_MAVEN_VERSION"
}

require_java() {
  command -v java >/dev/null 2>&1 || fail "java command not found; install or select a JDK according to docs/INSTALL.md first"
  [[ -n "${JAVA_HOME:-}" ]] || fail "JAVA_HOME is not set; configure JAVA_HOME for the selected JDK first"
  [[ -x "$JAVA_HOME/bin/java" ]] || fail "JAVA_HOME does not contain bin/java: $JAVA_HOME"
}

install_maven() {
  local archive url

  command -v curl >/dev/null 2>&1 || fail "curl command not found; install curl first"
  command -v tar >/dev/null 2>&1 || fail "tar command not found; install tar first"

  archive="$TMP_DIR/apache-maven-$MAVEN_VERSION-bin.tar.gz"
  url="$MAVEN_BASE_URL/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz"

  if [[ ! -d "$MAVEN_INSTALL_DIR" ]]; then
    log "downloading Maven $MAVEN_VERSION"
    curl -fsSL "$url" -o "$archive"

    log "installing Maven to $MAVEN_INSTALL_DIR"
    sudo_cmd tar -xzf "$archive" -C /opt
  else
    log "Maven install directory already exists: $MAVEN_INSTALL_DIR"
  fi

  sudo_cmd ln -sfn "$MAVEN_INSTALL_DIR" "$MAVEN_HOME_LINK"
}

persist_environment() {
  local line_home line_path

  line_home="export MAVEN_HOME=$MAVEN_HOME_LINK"
  line_path='export PATH=$MAVEN_HOME/bin:$PATH'

  if ! grep -qxF "$line_home" "$PROFILE_FILE" 2>/dev/null; then
    log "persisting MAVEN_HOME in $PROFILE_FILE"
    printf '\n%s\n%s\n' "$line_home" "$line_path" >> "$PROFILE_FILE"
  fi

  export MAVEN_HOME="$MAVEN_HOME_LINK"
  export PATH="$MAVEN_HOME/bin:$PATH"
}

verify_maven() {
  local version mvn_java_home

  command -v mvn >/dev/null 2>&1 || fail "mvn command not found"
  version="$(maven_version || true)"
  [[ -n "$version" ]] || fail "could not parse Maven version"
  version_ge "$version" "$MIN_MAVEN_VERSION" || fail "Maven version is $version, expected at least $MIN_MAVEN_VERSION"

  mvn_java_home="$(mvn -v | awk -F': ' '/Java home/ { print $2; exit }')"
  [[ -n "$mvn_java_home" ]] || fail "could not read Java home from mvn -v"

  log "Maven verification passed: version=$version, java_home=$mvn_java_home"
  mvn -v
}

main() {
  require_java

  if maven_satisfies_requirement; then
    log "existing Maven satisfies requirement: version=$(maven_version)"
  else
    install_maven
  fi

  persist_environment
  verify_maven
}

main "$@"
