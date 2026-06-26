#!/usr/bin/env bash
# Shared helpers for the pixels-install agent scripts.
#
# All environment variables installed by this agent (JAVA_HOME, MAVEN_HOME,
# ETCD, PIXELS_HOME, ...) must be persisted into the *current user's* shell
# profile only - never into a global file such as /etc/environment or
# /etc/profile.d, and never assuming bash when the user's login shell is zsh.
#
# Each install_*.sh script also runs as its own process, so an `export` in
# one script's process never reaches the next script's process - only what
# got persisted to disk survives. docs/INSTALL.md's own walkthrough handles
# this by running `source ~/.bashrc` right after appending to it, but that
# only works in the same interactive shell session. Sourcing the user's real
# rc file from a non-interactive script is not reliable either: Ubuntu's
# default ~/.bashrc starts with a non-interactive guard
# (`case $- in *i*) ;; *) return;; esac`) that returns before reaching
# anything appended at the end of the file.
#
# So in addition to persisting into the user's real shell profile (for
# future interactive sessions, per the user's explicit request), every
# install_*.sh script also mirrors the same variable into a small, plain
# `toolchain.env` file with no such guard, and sources that file at startup.
# This lets each step pick up JAVA_HOME/MAVEN_HOME/ETCD/PIXELS_HOME from a
# prior step even when the agent invokes each script as a separate process.

_SHELL_ENV_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_TOOLCHAIN_ENV_FILE="$(cd "$_SHELL_ENV_LIB_DIR/../.." && pwd)/toolchain.env"

# Print the profile file that should receive persisted exports.
# Honors an explicit PROFILE_FILE override; otherwise picks the rc file
# matching the user's current login shell ($SHELL).
detect_profile_file() {
  if [[ -n "${PROFILE_FILE:-}" ]]; then
    printf '%s\n' "$PROFILE_FILE"
    return
  fi

  local shell_name
  shell_name="$(basename "${SHELL:-bash}")"

  case "$shell_name" in
    zsh)
      printf '%s\n' "$HOME/.zshrc"
      ;;
    *)
      printf '%s\n' "$HOME/.bashrc"
      ;;
  esac
}

# Idempotently append a line to a profile file, creating it if needed.
persist_line() {
  local profile_file="$1"
  local line="$2"

  touch "$profile_file"
  if ! grep -qxF "$line" "$profile_file" 2>/dev/null; then
    printf '\n%s\n' "$line" >> "$profile_file"
  fi
}

# Idempotently set one `export NAME=value` line in a profile file, replacing
# any prior line for the same NAME (whatever its old value was) instead of
# just appending. Use this for *_HOME-style variables whose value can change
# between runs (e.g. a JDK that didn't satisfy the version requirement
# yesterday gets replaced by a newer one today) - plain persist_line() would
# leave the old, now-wrong line in place alongside the new one.
persist_export() {
  local profile_file="$1"
  local name="$2"
  local value="$3"
  local desired_line="export ${name}=${value}"
  local tmp

  touch "$profile_file"
  if grep -qxF "$desired_line" "$profile_file" 2>/dev/null; then
    return 0
  fi

  tmp="$(mktemp "${profile_file}.XXXXXX")"
  grep -vE "^export ${name}=" "$profile_file" > "$tmp" 2>/dev/null || true
  printf '%s\n' "$desired_line" >> "$tmp"
  mv "$tmp" "$profile_file"
}

# Finds a usable JAVA_HOME: prefers an already-exported JAVA_HOME pointing at
# a real bin/java, otherwise derives one from whatever `java` is on PATH
# (resolving update-alternatives-style symlinks). This covers JDKs installed
# via `apt` (per docs/INSTALL.md) that never export JAVA_HOME themselves, so
# install_jdk.sh/install_maven.sh don't reinstall a JDK that is already fine.
locate_existing_java_home() {
  if [[ -n "${JAVA_HOME:-}" && -x "$JAVA_HOME/bin/java" ]]; then
    printf '%s\n' "$JAVA_HOME"
    return 0
  fi

  local java_path
  java_path="$(command -v java 2>/dev/null || true)"
  [[ -n "$java_path" ]] || return 1

  java_path="$(readlink -f "$java_path" 2>/dev/null || printf '%s' "$java_path")"
  [[ -x "$java_path" ]] || return 1
  (cd "$(dirname "$java_path")/.." && pwd)
}

# Same idea as locate_existing_java_home, but for Maven: prefers an
# already-exported MAVEN_HOME, otherwise derives one from `mvn` on PATH
# (e.g. a Maven installed by apt/yum, or by a previous run of this script).
locate_existing_maven_home() {
  if [[ -n "${MAVEN_HOME:-}" && -x "$MAVEN_HOME/bin/mvn" ]]; then
    printf '%s\n' "$MAVEN_HOME"
    return 0
  fi

  local mvn_path
  mvn_path="$(command -v mvn 2>/dev/null || true)"
  [[ -n "$mvn_path" ]] || return 1

  mvn_path="$(readlink -f "$mvn_path" 2>/dev/null || printf '%s' "$mvn_path")"
  [[ -x "$mvn_path" ]] || return 1
  (cd "$(dirname "$mvn_path")/.." && pwd)
}

# Idempotently set (replacing any prior value) one NAME=value line in the
# shared toolchain.env file used to chain variables between install_*.sh
# invocations. See the file header for why this exists.
persist_toolchain_var() {
  local name="$1"
  local value="$2"
  local file="${TOOLCHAIN_ENV_FILE:-$DEFAULT_TOOLCHAIN_ENV_FILE}"
  local tmp

  mkdir -p "$(dirname "$file")"
  touch "$file"
  tmp="$(mktemp "${file}.XXXXXX")"
  grep -v "^export ${name}=" "$file" > "$tmp" 2>/dev/null || true
  printf 'export %s=%s\n' "$name" "$value" >> "$tmp"
  mv "$tmp" "$file"
}

# Sources toolchain.env (if present) and puts any JAVA_HOME/MAVEN_HOME/ETCD
# bin directories it set onto PATH, so a script invoked as a fresh process
# can still find `java`/`mvn`/`etcdctl` installed by an earlier step.
load_toolchain_env() {
  local file="${TOOLCHAIN_ENV_FILE:-$DEFAULT_TOOLCHAIN_ENV_FILE}"
  local java_home_was_set=false java_home_value=""
  local maven_home_was_set=false maven_home_value=""
  local etc_was_set=false etc_value=""
  local pixels_home_was_set=false pixels_home_value=""

  [[ -f "$file" ]] || return 0
  if [[ -n "${JAVA_HOME+x}" ]]; then
    java_home_was_set=true
    java_home_value="$JAVA_HOME"
  fi
  if [[ -n "${MAVEN_HOME+x}" ]]; then
    maven_home_was_set=true
    maven_home_value="$MAVEN_HOME"
  fi
  if [[ -n "${ETCD+x}" ]]; then
    etc_was_set=true
    etc_value="$ETCD"
  fi
  if [[ -n "${PIXELS_HOME+x}" ]]; then
    pixels_home_was_set=true
    pixels_home_value="$PIXELS_HOME"
  fi

  # shellcheck disable=SC1090
  source "$file"
  [[ "$java_home_was_set" == "true" ]] && export JAVA_HOME="$java_home_value"
  [[ "$maven_home_was_set" == "true" ]] && export MAVEN_HOME="$maven_home_value"
  [[ "$etc_was_set" == "true" ]] && export ETCD="$etc_value"
  [[ "$pixels_home_was_set" == "true" ]] && export PIXELS_HOME="$pixels_home_value"

  if [[ -n "${JAVA_HOME:-}" && -x "$JAVA_HOME/bin/java" ]]; then
    export PATH="$JAVA_HOME/bin:$PATH"
  fi
  if [[ -n "${MAVEN_HOME:-}" && -x "$MAVEN_HOME/bin/mvn" ]]; then
    export PATH="$MAVEN_HOME/bin:$PATH"
  fi
  if [[ -n "${ETCD:-}" ]]; then
    export PATH="$PATH:$ETCD"
  fi
}

# --- Structured result reporting --------------------------------------------
# Diagnostic scripts (check_prerequisites.sh, smoke_test.sh, ...) run many
# independent checks. Bailing out on the first failure (the old behavior,
# relying on `set -e` + a `fail()` that calls `exit 1`) hides every other
# check's result from whoever - human or agent - has to decide what to do
# next. These helpers let a script run every check, record an ok/warn/fail/
# skip status with a short detail string for each, and emit one
# machine-parsable summary block at the end, so the agent reasons from a
# complete picture instead of re-running the script repeatedly to find each
# failure one at a time, and doesn't have to scrape prose log lines to do it.
declare -a _RESULT_NAMES=()
declare -a _RESULT_STATUSES=()
declare -a _RESULT_DETAILS=()

result_reset() {
  _RESULT_NAMES=()
  _RESULT_STATUSES=()
  _RESULT_DETAILS=()
}

# result_record <check_name> <ok|warn|fail|skip> <detail>
result_record() {
  local name="$1"
  local status="$2"
  local detail="$3"

  case "$status" in
    ok|warn|fail|skip) ;;
    *)
      printf 'ERROR: result_record: unknown status "%s" for check "%s"\n' "$status" "$name" >&2
      exit 1
      ;;
  esac

  case "$status" in
    ok) printf '[%s] OK   %s: %s\n' "$(date '+%H:%M:%S')" "$name" "$detail" ;;
    skip) printf '[%s] SKIP %s: %s\n' "$(date '+%H:%M:%S')" "$name" "$detail" ;;
    warn) printf 'WARN %s: %s\n' "$name" "$detail" >&2 ;;
    fail) printf 'FAIL %s: %s\n' "$name" "$detail" >&2 ;;
  esac

  _RESULT_NAMES+=("$name")
  _RESULT_STATUSES+=("$status")
  _RESULT_DETAILS+=("$detail")
}

# Emits a summary block covering every result_record call since the last
# result_reset, then returns 1 if any check failed (warn/skip do not fail
# the overall run). Callers should exit with this function's own exit code.
result_emit_summary() {
  local label="$1"
  local i
  local count="${#_RESULT_NAMES[@]}"
  local ok_count=0 warn_count=0 fail_count=0 skip_count=0

  for ((i = 0; i < count; i++)); do
    case "${_RESULT_STATUSES[$i]}" in
      ok) ((ok_count += 1)) ;;
      warn) ((warn_count += 1)) ;;
      fail) ((fail_count += 1)) ;;
      skip) ((skip_count += 1)) ;;
    esac
  done

  printf '\n=== %s result ===\n' "$label"
  for ((i = 0; i < count; i++)); do
    printf '%-4s %s: %s\n' "${_RESULT_STATUSES[$i]}" "${_RESULT_NAMES[$i]}" "${_RESULT_DETAILS[$i]}"
  done
  printf 'summary: ok=%d warn=%d fail=%d skip=%d status=%s\n' \
    "$ok_count" "$warn_count" "$fail_count" "$skip_count" \
    "$([[ "$fail_count" -eq 0 ]] && printf pass || printf fail)"
  printf '=== end %s result ===\n' "$label"

  [[ "$fail_count" -eq 0 ]]
}

# --- Phase checkpointing -----------------------------------------------------
# A full install walks ~13 phases (see agent.yaml) across many separate tool
# calls; a long-running session can be interrupted partway through (dropped
# SSH session, context compaction, the user stepping away). Without a
# checkpoint, resuming means either re-reading the whole conversation to
# guess what already happened, or re-running steps that already succeeded.
# scripts/progress.sh is the only thing that writes this file - it is a thin
# CLI over the functions below so the agent can call `progress.sh mark
# <phase>` / `progress.sh show` directly instead of every install_*.sh script
# needing to know about phase names.
DEFAULT_PROGRESS_FILE="$(cd "$_SHELL_ENV_LIB_DIR/../.." && pwd)/progress.env"

progress_file() {
  printf '%s\n' "${PROGRESS_FILE:-$DEFAULT_PROGRESS_FILE}"
}

# Idempotently records a phase as done, with a timestamp and optional note.
mark_phase_done() {
  local phase="$1"
  local note="${2:-}"
  local file
  local tmp
  file="$(progress_file)"

  mkdir -p "$(dirname "$file")"
  touch "$file"
  tmp="$(mktemp "${file}.XXXXXX")"
  grep -v "^${phase}=" "$file" > "$tmp" 2>/dev/null || true
  printf '%s=%s|%s\n' "$phase" "$(date '+%Y-%m-%dT%H:%M:%S')" "$note" >> "$tmp"
  mv "$tmp" "$file"
}

phase_is_done() {
  local phase="$1"
  local file
  file="$(progress_file)"

  [[ -f "$file" ]] && grep -q "^${phase}=" "$file"
}

# Prints every recorded phase as "<phase>: done at <timestamp> (<note>)".
print_progress() {
  local file
  file="$(progress_file)"

  if [[ ! -f "$file" || ! -s "$file" ]]; then
    printf 'no phases recorded yet (%s does not exist or is empty)\n' "$file"
    return 0
  fi

  while IFS='=' read -r phase rest; do
    [[ -n "$phase" ]] || continue
    local ts="${rest%%|*}"
    local note="${rest#*|}"
    if [[ -n "$note" && "$note" != "$rest" ]]; then
      printf '%s: done at %s (%s)\n' "$phase" "$ts" "$note"
    else
      printf '%s: done at %s\n' "$phase" "$ts"
    fi
  done < "$file"
}
