#!/usr/bin/env bash
set -euo pipefail

# Installs a Zulu OpenJDK build for the current server's CPU architecture
# under the current user's home directory, and persists JAVA_HOME for that
# user only (never a global /etc/environment-style change).
#
# docs/INSTALL.md recommends JDK 23+ for the documented Pixels + Trino 466
# path. Other query engines (e.g. Hadoop/Hive integrations) may require an
# older JDK; pass JDK_VERSION to override.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/shell_env.sh
source "$SCRIPT_DIR/lib/shell_env.sh"
load_toolchain_env

JDK_VERSION="${JDK_VERSION:-23}"
JDK_INSTALL_PARENT="${JDK_INSTALL_PARENT:-$HOME/opt}"
JDK_HOME_LINK="${JDK_HOME_LINK:-$JDK_INSTALL_PARENT/zulu-$JDK_VERSION}"
ZULU_METADATA_URL="${ZULU_METADATA_URL:-https://api.azul.com/metadata/v1/zulu/packages/}"
TMP_DIR="${TMP_DIR:-/tmp}"
FORCE_JDK_INSTALL="${FORCE_JDK_INSTALL:-false}"

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

# Maps `uname -m` to the architecture identifier expected by Azul's Zulu
# metadata API.
detect_arch() {
  local arch
  arch="$(uname -m)"

  case "$arch" in
    x86_64|amd64)
      printf 'x86\n'
      ;;
    aarch64|arm64)
      printf 'arm\n'
      ;;
    *)
      fail "unsupported architecture for Zulu JDK: $arch"
      ;;
  esac
}

detect_hw_bitness() {
  case "$(uname -m)" in
    x86_64|amd64|aarch64|arm64)
      printf '64\n'
      ;;
    *)
      printf '32\n'
      ;;
  esac
}

resolve_download_url() {
  local arch="$1"
  local hw_bitness="$2"
  local query response url

  require_command curl
  require_command python3

  query="java_version=$JDK_VERSION&os=linux&arch=$arch&hw_bitness=$hw_bitness&archive_type=tar.gz&java_package_type=jdk&javafx_bundled=false&release_status=ga&latest=true&page=1&page_size=1"
  response="$(curl -fsSL "${ZULU_METADATA_URL}?${query}")" || fail "failed to query Zulu metadata API"

  url="$(printf '%s' "$response" | python3 -c '
import json
import sys

try:
    data = json.load(sys.stdin)
except ValueError:
    sys.exit(0)

if isinstance(data, list) and data:
    print(data[0].get("download_url", ""))
')"

  printf '%s\n' "$url"
}

# Parses the major version out of `java -version`'s first line, handling
# both the modern scheme ("23.0.1") and the legacy 1.x scheme ("1.8.0_392").
detect_installed_major_version() {
  local java_home="$1"
  local version_line major

  version_line="$("$java_home/bin/java" -version 2>&1 | head -n1)"
  major="$(printf '%s\n' "$version_line" | sed -nE 's/.*"1\.([0-9]+)\..*/\1/p')"
  if [[ -z "$major" ]]; then
    major="$(printf '%s\n' "$version_line" | sed -nE 's/.*"([0-9]+)(\.[0-9]+)*.*/\1/p')"
  fi
  printf '%s\n' "$major"
}

# Looks for a JDK that already satisfies JDK_VERSION, whether it's already
# exported as JAVA_HOME or just sitting on PATH (e.g. installed via `apt`
# per docs/INSTALL.md, which never sets JAVA_HOME itself). Prints its home
# directory and returns success if found, so the caller can reuse it instead
# of downloading a fresh Zulu build.
find_satisfying_java_home() {
  local candidate_home installed_major

  [[ "$FORCE_JDK_INSTALL" != "true" ]] || return 1

  candidate_home="$(locate_existing_java_home)" || return 1
  [[ -x "$candidate_home/bin/java" ]] || return 1

  installed_major="$(detect_installed_major_version "$candidate_home")"
  [[ "$installed_major" =~ ^[0-9]+$ ]] || return 1
  (( installed_major >= JDK_VERSION )) || return 1

  printf '%s\n' "$candidate_home"
}

install_jdk() {
  local arch hw_bitness url archive top_level_dir extracted_dir

  arch="$(detect_arch)"
  hw_bitness="$(detect_hw_bitness)"
  log "resolving Zulu JDK $JDK_VERSION download for arch=$arch hw_bitness=$hw_bitness"

  url="$(resolve_download_url "$arch" "$hw_bitness")"
  if [[ -z "$url" ]]; then
    fail "could not resolve a Zulu JDK $JDK_VERSION download via the Azul metadata API ($ZULU_METADATA_URL); fall back to the manual method in docs/INSTALL.md (Install JDK): download a matching .deb from https://www.azul.com/downloads/?package=jdk#zulu and run 'sudo dpkg -i zulu<version>-linux_<arch>.deb'"
  fi

  mkdir -p "$JDK_INSTALL_PARENT"
  archive="$TMP_DIR/$(basename "$url")"

  log "downloading $url"
  curl -fsSL "$url" -o "$archive"

  require_command tar
  set +o pipefail
  top_level_dir="$(tar -tzf "$archive" | head -n1 | cut -d/ -f1)"
  set -o pipefail
  [[ -n "$top_level_dir" ]] || fail "could not determine top-level directory inside $archive"
  extracted_dir="$JDK_INSTALL_PARENT/$top_level_dir"

  if [[ -d "$extracted_dir" ]]; then
    log "Zulu JDK already extracted: $extracted_dir"
  else
    log "extracting Zulu JDK to $extracted_dir"
    tar -xzf "$archive" -C "$JDK_INSTALL_PARENT"
  fi

  ln -sfn "$extracted_dir" "$JDK_HOME_LINK"
}

persist_environment() {
  local java_home="$1"
  local profile_file line_path

  profile_file="$(detect_profile_file)"
  line_path='export PATH=$JAVA_HOME/bin:$PATH'

  log "persisting JAVA_HOME=$java_home in $profile_file"
  persist_export "$profile_file" JAVA_HOME "$java_home"
  persist_line "$profile_file" "$line_path"
  persist_toolchain_var JAVA_HOME "$java_home"

  export JAVA_HOME="$java_home"
  export PATH="$JAVA_HOME/bin:$PATH"
}

verify_jdk() {
  command -v java >/dev/null 2>&1 || fail "java command not found after install"
  log "java version:"
  java -version
}

main() {
  local existing_java_home

  if existing_java_home="$(find_satisfying_java_home)"; then
    log "an existing JDK already satisfies JDK_VERSION>=$JDK_VERSION (JAVA_HOME=$existing_java_home: $("$existing_java_home/bin/java" -version 2>&1 | head -n1)); skipping the Zulu download and just persisting JAVA_HOME"
    log "set FORCE_JDK_INSTALL=true to install Zulu JDK $JDK_VERSION anyway"
    persist_environment "$existing_java_home"
    verify_jdk
    return
  fi

  install_jdk
  persist_environment "$JDK_HOME_LINK"
  verify_jdk
}

main "$@"
