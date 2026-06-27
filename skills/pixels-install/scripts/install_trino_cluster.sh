#!/usr/bin/env bash
set -uo pipefail

# Fans out install_trino.sh across the whole cluster described by
# trino-deployment.env, instead of requiring the skill/user to copy the same
# command and run it by hand once per node. Run this ON THE COORDINATOR: it
# installs Trino locally for the coordinator's own role, then runs
# install_trino.sh on every worker over the passwordless SSH that
# prepare_trino_cluster.sh (via shared-scripts/setup_cluster.sh) already set
# up coordinator -> worker - the same trust relationship
# install_trino_shell_helpers.sh's generated start/stop/restart functions
# rely on.
#
# Assumption: this repository already exists at the same path on every
# worker (override with REMOTE_REPO_ROOT if it doesn't) - same assumption
# install_trino.sh itself already documents ("run it locally on each
# machine"). This script does not clone or sync the repository, only the
# generated trino-deployment.env (scp'd to each worker before running, so
# workers don't need a shared filesystem to learn the cluster topology).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/shell_env.sh
source "$SCRIPT_DIR/lib/shell_env.sh"

SKILL_DIR="${SKILL_DIR:-$(skill_dir)}"
STATE_DIR="${STATE_DIR:-$(state_dir)}"
REPO_ROOT="${REPO_ROOT:-$(require_repo_root)}"
REMOTE_REPO_ROOT="${REMOTE_REPO_ROOT:-$REPO_ROOT}"
REMOTE_STATE_DIR="${REMOTE_STATE_DIR:-$REMOTE_REPO_ROOT/.agents/state/pixels-install}"
REMOTE_SKILL_SCRIPT="${REMOTE_SKILL_SCRIPT:-$REMOTE_REPO_ROOT/.agents/skills/pixels-install/scripts/install_trino.sh}"
REMOTE_DEV_SCRIPT="${REMOTE_DEV_SCRIPT:-$REMOTE_REPO_ROOT/skills/pixels-install/scripts/install_trino.sh}"

TRINO_DEPLOYMENT_FILE="${TRINO_DEPLOYMENT_FILE:-$STATE_DIR/trino-deployment.env}"
LOG_DIR="${LOG_DIR:-$STATE_DIR/logs}"

SSH_USER="${SSH_USER:-root}"
SSH_PORT="${SSH_PORT:-}"
RUN_LOCAL_COORDINATOR="${RUN_LOCAL_COORDINATOR:-true}"
ASSUME_YES="${ASSUME_YES:-false}"
CONFIRM_TRINO_CLUSTER_INSTALL="${CONFIRM_TRINO_CLUSTER_INSTALL:-}"

log() {
  printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"
}

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

[[ -f "$TRINO_DEPLOYMENT_FILE" ]] || fail "trino-deployment.env not found at $TRINO_DEPLOYMENT_FILE; run prepare_trino_cluster.sh first"
set -a
# shellcheck disable=SC1090
source "$TRINO_DEPLOYMENT_FILE"
set +a

[[ -n "${TRINO_COORDINATOR_SSH_TARGET:-}" ]] || fail "TRINO_COORDINATOR_SSH_TARGET missing from $TRINO_DEPLOYMENT_FILE"

remote_spec() {
  local target="$1"
  if [[ -n "$SSH_USER" && "$target" != *@* ]]; then
    printf '%s@%s' "$SSH_USER" "$target"
  else
    printf '%s' "$target"
  fi
}

ssh_opts() {
  local -a opts=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
  [[ -n "$SSH_PORT" ]] && opts+=(-p "$SSH_PORT")
  printf '%s\n' "${opts[@]}"
}

confirm_cluster_install() {
  local reply

  case "$CONFIRM_TRINO_CLUSTER_INSTALL" in
    true) return ;;
    false) fail "CONFIRM_TRINO_CLUSTER_INSTALL=false" ;;
  esac
  [[ "$ASSUME_YES" == "true" ]] && return

  printf '\nTrino cluster install summary:\n'
  printf '  deployment file: %s\n' "$TRINO_DEPLOYMENT_FILE"
  printf '  version: %s\n' "${TRINO_VERSION:-466}"
  printf '  install_parent: %s\n' "${TRINO_INSTALL_PARENT:-$HOME/opt}"
  printf '  home_link: %s\n' "${TRINO_HOME_LINK:-${TRINO_INSTALL_PARENT:-$HOME/opt}/trino-server}"
  printf '  data_dir: %s\n' "${TRINO_DATA_DIR:-${TRINO_INSTALL_PARENT:-$HOME/opt}/var/trino/data}"
  printf '  coordinator: %s (%s)\n' "${TRINO_COORDINATOR_NAME:-coordinator}" "$TRINO_COORDINATOR_SSH_TARGET"
  printf '  coordinator_is_worker: %s\n' "${TRINO_COORDINATOR_IS_WORKER:-false}"
  printf '  workers: %s\n\n' "${TRINO_WORKER_NAMES:-(none)}"

  [[ -t 0 ]] || fail "Trino cluster install must be confirmed; set CONFIRM_TRINO_CLUSTER_INSTALL=true after reviewing the topology and install paths"
  read -r -p "Install/configure Trino across this cluster? [y/N]: " reply
  [[ "$reply" =~ ^[Yy]$ ]] || fail "aborted Trino cluster install"
}

run_local_coordinator() {
  [[ "$RUN_LOCAL_COORDINATOR" == "true" ]] || { result_record "node:${TRINO_COORDINATOR_NAME:-coordinator}" skip "RUN_LOCAL_COORDINATOR=false"; return; }

  local name="${TRINO_COORDINATOR_NAME:-coordinator}"
  local log_file="$LOG_DIR/trino_install_${name}.log"
  mkdir -p "$LOG_DIR"

  log "installing Trino locally for coordinator: $name (log: $log_file)"
  if TRINO_ROLE=coordinator CONFIRM_TRINO_INSTALL=true "$SCRIPT_DIR/install_trino.sh" >"$log_file" 2>&1; then
    result_record "node:$name" ok "coordinator install succeeded (log: $log_file)"
  else
    result_record "node:$name" fail "coordinator install failed, see $log_file (tail: $(tail -n 1 "$log_file" 2>/dev/null))"
  fi
}

# Copies trino-deployment.env to the worker and runs install_trino.sh there
# in the background; writes the worker's exit code to a small marker file
# so the caller can collect it after `wait`.
launch_worker() {
  local ssh_target="$1"
  local name="$2"
  local -a scp_opts=() ssh_args=()
  local log_file marker_file remote_deployment_file remote_command

  mkdir -p "$LOG_DIR"
  log_file="$LOG_DIR/trino_install_${name}.log"
  marker_file="$LOG_DIR/trino_install_${name}.exitcode"
  rm -f "$marker_file"

  while IFS= read -r opt; do ssh_args+=("$opt"); done < <(ssh_opts)
  [[ -n "$SSH_PORT" ]] && scp_opts+=(-P "$SSH_PORT")
  scp_opts+=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)

  remote_deployment_file="$REMOTE_STATE_DIR/trino-deployment.env"
  remote_command="cd '$REMOTE_REPO_ROOT' && mkdir -p '$REMOTE_STATE_DIR' && if [ -x '$REMOTE_SKILL_SCRIPT' ]; then script='$REMOTE_SKILL_SCRIPT'; elif [ -x '$REMOTE_DEV_SCRIPT' ]; then script='$REMOTE_DEV_SCRIPT'; else echo 'install_trino.sh not found; install the pixels-install skill or set REMOTE_SKILL_SCRIPT' >&2; exit 1; fi; STATE_DIR='$REMOTE_STATE_DIR' REPO_ROOT='$REMOTE_REPO_ROOT' TRINO_ROLE=worker CONFIRM_TRINO_INSTALL=true \"\$script\""

  (
    {
      echo "--- preparing remote state dir on $name ($ssh_target) ---"
      ssh "${ssh_args[@]}" "$(remote_spec "$ssh_target")" "mkdir -p '$REMOTE_STATE_DIR'" &&
      echo "--- copying trino-deployment.env to $name ($ssh_target) ---"
      scp "${scp_opts[@]}" "$TRINO_DEPLOYMENT_FILE" "$(remote_spec "$ssh_target"):$remote_deployment_file" &&
      echo "--- running install_trino.sh on $name ($ssh_target) ---" &&
      ssh "${ssh_args[@]}" "$(remote_spec "$ssh_target")" "$remote_command"
    } >"$log_file" 2>&1
    echo "$?" > "$marker_file"
  ) &

  log "dispatched worker install: $name ($ssh_target), pid $!, log: $log_file"
}

run_workers() {
  local -a names=()
  local -a targets=()
  local i name target log_file exit_code

  read -r -a targets <<< "${TRINO_WORKER_SSH_TARGETS:-}"
  read -r -a names <<< "${TRINO_WORKER_NAMES:-}"

  if [[ "${#targets[@]}" -eq 0 ]]; then
    log "no workers in $TRINO_DEPLOYMENT_FILE; nothing to dispatch"
    return
  fi

  for ((i = 0; i < ${#targets[@]}; i++)); do
    launch_worker "${targets[$i]}" "${names[$i]:-worker-$i}"
  done

  log "waiting for ${#targets[@]} worker install(s) to finish in parallel..."
  wait

  for ((i = 0; i < ${#targets[@]}; i++)); do
    name="${names[$i]:-worker-$i}"
    log_file="$LOG_DIR/trino_install_${name}.log"
    exit_code="$(cat "$LOG_DIR/trino_install_${name}.exitcode" 2>/dev/null || echo 1)"

    if [[ "$exit_code" == "0" ]]; then
      result_record "node:$name" ok "worker install succeeded (log: $log_file)"
    else
      result_record "node:$name" fail "worker install failed with exit $exit_code, see $log_file (tail: $(tail -n 1 "$log_file" 2>/dev/null))"
    fi
  done
}

main() {
  result_reset
  confirm_cluster_install
  run_local_coordinator
  run_workers
  result_emit_summary install_trino_cluster
}

main "$@"
