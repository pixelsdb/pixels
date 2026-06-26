#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/shell_env.sh
source "$SCRIPT_DIR/lib/shell_env.sh"
# Picks up JAVA_HOME from a previous, separately invoked install_jdk.sh run
# even if the calling agent never re-sourced the shell profile in between.
load_toolchain_env

MIN_MAVEN_VERSION="${MIN_MAVEN_VERSION:-3.8.0}"
# 3.9.8 is our default pinned version; override with MAVEN_VERSION if the
# user explicitly asks for a different one.
MAVEN_VERSION="${MAVEN_VERSION:-3.9.8}"
MAVEN_BASE_URL="${MAVEN_BASE_URL:-https://archive.apache.org/dist/maven/maven-3}"
# Installed under the current user's home (never /opt): the user running this
# agent is not necessarily a "pixels" or "ubuntu" account, so everything is
# anchored on $HOME, exactly like the Pixels and etcd installs.
MAVEN_INSTALL_PARENT="${MAVEN_INSTALL_PARENT:-$HOME/opt}"
MAVEN_INSTALL_DIR="${MAVEN_INSTALL_DIR:-$MAVEN_INSTALL_PARENT/apache-maven-$MAVEN_VERSION}"
MAVEN_HOME_LINK="${MAVEN_HOME_LINK:-$MAVEN_INSTALL_PARENT/maven}"
TMP_DIR="${TMP_DIR:-/tmp}"

log() {
  printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"
}

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
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
  local derived_home

  if [[ -z "${JAVA_HOME:-}" || ! -x "${JAVA_HOME:-}/bin/java" ]]; then
    derived_home="$(locate_existing_java_home || true)"
    if [[ -n "$derived_home" ]]; then
      log "JAVA_HOME was not set (or invalid); derived it from java on PATH: $derived_home"
      export JAVA_HOME="$derived_home"
    fi
  fi

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

  mkdir -p "$MAVEN_INSTALL_PARENT"

  if [[ ! -d "$MAVEN_INSTALL_DIR" ]]; then
    log "downloading Maven $MAVEN_VERSION from the Apache archive"
    curl -fsSL "$url" -o "$archive"

    log "installing Maven to $MAVEN_INSTALL_DIR"
    tar -xzf "$archive" -C "$MAVEN_INSTALL_PARENT"
  else
    log "Maven install directory already exists: $MAVEN_INSTALL_DIR"
  fi

  ln -sfn "$MAVEN_INSTALL_DIR" "$MAVEN_HOME_LINK"
}

persist_environment() {
  local maven_home="$1"
  local profile_file line_path

  profile_file="$(detect_profile_file)"
  line_path='export PATH=$MAVEN_HOME/bin:$PATH'

  log "persisting MAVEN_HOME=$maven_home in $profile_file"
  persist_export "$profile_file" MAVEN_HOME "$maven_home"
  persist_line "$profile_file" "$line_path"
  persist_toolchain_var MAVEN_HOME "$maven_home"

  export MAVEN_HOME="$maven_home"
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

  local maven_home_to_persist

  if maven_satisfies_requirement; then
    maven_home_to_persist="$(locate_existing_maven_home || true)"
    if [[ -n "$maven_home_to_persist" ]]; then
      log "existing Maven satisfies requirement: version=$(maven_version), home=$maven_home_to_persist; skipping download and just persisting MAVEN_HOME"
    else
      log "existing Maven satisfies requirement: version=$(maven_version), but its home directory could not be determined; leaving MAVEN_HOME untouched"
    fi
  else
    install_maven
    maven_home_to_persist="$MAVEN_HOME_LINK"
  fi

  if [[ -n "$maven_home_to_persist" ]]; then
    persist_environment "$maven_home_to_persist"
  fi

  verify_maven
}

main "$@"
