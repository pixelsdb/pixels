#!/usr/bin/env bash
set -euo pipefail

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SHELL_ENV="$SKILL_DIR/scripts/lib/shell_env.sh"

command -v bash >/dev/null 2>&1 || {
  printf 'Bash is required for this test\n' >&2
  exit 1
}
command -v zsh >/dev/null 2>&1 || {
  printf 'Zsh is required for this test\n' >&2
  exit 1
}

TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/pixels-install-shell-test.XXXXXX")"
trap 'rm -rf "$TEST_ROOT"' EXIT

HOME="$TEST_ROOT/home"
STATE_DIR="$TEST_ROOT/state"
PROFILE_FILE="$TEST_ROOT/profile"
mkdir -p "$HOME" "$STATE_DIR"

login_shell="$(getent passwd "$(id -un)" | awk -F: 'NR == 1 { print $7; exit }')"
case "$(basename "$login_shell")" in
  bash) expected_profile="$HOME/.bashrc" ;;
  zsh) expected_profile="$HOME/.zshrc" ;;
  *) printf 'unsupported test account login shell: %s\n' "$login_shell" >&2; exit 1 ;;
esac
detected_profile="$(HOME="$HOME" SHELL=/bin/bash bash -c 'source "$1"; detect_profile_file' bash "$SHELL_ENV")"
[[ "$detected_profile" == "$expected_profile" ]]

if zsh "$SKILL_DIR/scripts/progress.sh" --help >"$TEST_ROOT/zsh-entrypoint-error" 2>&1; then
  printf 'a Bash installer unexpectedly ran under zsh\n' >&2
  exit 1
fi
grep -Fq 'must be executed by Bash' "$TEST_ROOT/zsh-entrypoint-error"
if bash -c 'source "$1"' bash "$SKILL_DIR/scripts/progress.sh" >"$TEST_ROOT/source-entrypoint-error" 2>&1; then
  printf 'a Bash installer unexpectedly succeeded when sourced\n' >&2
  exit 1
fi
grep -Fq 'do not source pixels-install installer scripts' "$TEST_ROOT/source-entrypoint-error"

quoted_value="$TEST_ROOT/path with space"
quoted_profile="$TEST_ROOT/profile with space"
PROFILE_FILE="$quoted_profile"
mkdir -p "$quoted_value"
bash -c 'source "$1"; persist_export "$2" TEST_PATH "$3"' \
  bash "$SHELL_ENV" "$PROFILE_FILE" "$quoted_value"
grep -Fxq "export TEST_PATH='$quoted_value'" "$PROFILE_FILE"

PIXELS_FUNCTIONS_FILE="$TEST_ROOT/pixels helpers.sh"
PIXELS_HOME="$TEST_ROOT/pixels home"
env \
  HOME="$HOME" \
  STATE_DIR="$STATE_DIR" \
  PROFILE_FILE="$PROFILE_FILE" \
  PIXELS_FUNCTIONS_FILE="$PIXELS_FUNCTIONS_FILE" \
  PIXELS_HOME="$PIXELS_HOME" \
  PIXELS_SHELL_HELPERS_TARGET=local \
  INSTALL_PIXELS_SHELL_HELPERS=true \
  "$SKILL_DIR/scripts/install_shell_helpers.sh"

bash -n "$PIXELS_FUNCTIONS_FILE"
zsh -n "$PIXELS_FUNCTIONS_FILE"
grep -Fq "[ -f \"$PIXELS_FUNCTIONS_FILE\" ] && source \"$PIXELS_FUNCTIONS_FILE\"" "$PROFILE_FILE"
bash -c 'source "$1"; [[ "$(_pixels_home)" == "$2" ]]' \
  bash "$PIXELS_FUNCTIONS_FILE" "$PIXELS_HOME"
zsh -f -c 'source "$1"; [[ "$(_pixels_home)" == "$2" ]]' \
  zsh "$PIXELS_FUNCTIONS_FILE" "$PIXELS_HOME"

TRINO_DEPLOYMENT_FILE="$STATE_DIR/trino-deployment.env"
cat > "$TRINO_DEPLOYMENT_FILE" <<EOF
TRINO_COORDINATOR_SSH_TARGET=coordinator
TRINO_COORDINATOR_HOST=127.0.0.1
TRINO_COORDINATOR_NAME=coordinator
TRINO_WORKER_SSH_TARGETS='worker-1 worker-2'
TRINO_WORKER_NAMES='worker-1 worker-2'
EOF

TRINO_FUNCTIONS_FILE="$TEST_ROOT/trino helpers.sh"
TRINO_HOME_LINK="$TEST_ROOT/trino home"
TRINO_PIXELS_HOME="$TEST_ROOT/trino pixels home"
TRINO_PIXELS_CONFIG="$TRINO_PIXELS_HOME/etc/pixels.properties"
env \
  HOME="$HOME" \
  STATE_DIR="$STATE_DIR" \
  TRINO_DEPLOYMENT_FILE="$TRINO_DEPLOYMENT_FILE" \
  PROFILE_FILE="$PROFILE_FILE" \
  TRINO_FUNCTIONS_FILE="$TRINO_FUNCTIONS_FILE" \
  TRINO_HOME_LINK="$TRINO_HOME_LINK" \
  TRINO_PIXELS_HOME="$TRINO_PIXELS_HOME" \
  TRINO_PIXELS_CONFIG="$TRINO_PIXELS_CONFIG" \
  TRINO_SHELL_HELPERS_TARGET=local \
  INSTALL_TRINO_SHELL_HELPERS=true \
  "$SKILL_DIR/scripts/install_trino_shell_helpers.sh"

bash -n "$TRINO_FUNCTIONS_FILE"
zsh -n "$TRINO_FUNCTIONS_FILE"
grep -Fq "[ -f \"$TRINO_FUNCTIONS_FILE\" ] && source \"$TRINO_FUNCTIONS_FILE\"" "$PROFILE_FILE"
bash -c 'source "$1"; [[ "$(_trino_home)" == "$2" && "$(_trino_pixels_home)" == "$3" && "$(_trino_pixels_config)" == "$4" ]]' \
  bash "$TRINO_FUNCTIONS_FILE" "$TRINO_HOME_LINK" "$TRINO_PIXELS_HOME" "$TRINO_PIXELS_CONFIG"
zsh -f -c 'source "$1"; [[ "$(_trino_home)" == "$2" && "$(_trino_pixels_home)" == "$3" && "$(_trino_pixels_config)" == "$4" ]]' \
  zsh "$TRINO_FUNCTIONS_FILE" "$TRINO_HOME_LINK" "$TRINO_PIXELS_HOME" "$TRINO_PIXELS_CONFIG"

remote_home="$TEST_ROOT/trino link's home"
remote_pixels_home="$TEST_ROOT/trino pixels's home"
remote_pixels_config="$remote_pixels_home/etc/pixels.properties"
bash -c 'source "$1"; ssh() { printf "%s\n" "$*"; }; TRINO_REMOTE_HOME_LINK="$2"; TRINO_PIXELS_HOME="$3"; TRINO_PIXELS_CONFIG="$4"; _trino_remote_run worker-1 start' \
  bash "$TRINO_FUNCTIONS_FILE" "$remote_home" "$remote_pixels_home" "$remote_pixels_config" > "$TEST_ROOT/bash-remote-command"
zsh -f -c 'source "$1"; ssh() { printf "%s\n" "$*"; }; TRINO_REMOTE_HOME_LINK="$2"; TRINO_PIXELS_HOME="$3"; TRINO_PIXELS_CONFIG="$4"; _trino_remote_run worker-1 start' \
  zsh "$TRINO_FUNCTIONS_FILE" "$remote_home" "$remote_pixels_home" "$remote_pixels_config" > "$TEST_ROOT/zsh-remote-command"
for remote_output in "$TEST_ROOT/bash-remote-command" "$TEST_ROOT/zsh-remote-command"; do
  grep -Fq 'PIXELS_HOME=' "$remote_output"
  grep -Fq 'launcher' "$remote_output"
  grep -Fq 'start' "$remote_output"
done

# Bash arrays are 0-indexed and Zsh arrays are 1-indexed, so array index
# arithmetic parses cleanly under both shells while silently skipping a node
# in one of them. `bash -n`/`zsh -n` cannot catch that, so drive the cluster
# functions with a stubbed launcher and compare which nodes they act on.
mkdir -p "$TRINO_HOME_LINK/bin"
cat > "$TRINO_HOME_LINK/bin/launcher" <<'EOF'
#!/usr/bin/env bash
printf 'local %s\n' "$1"
EOF
chmod +x "$TRINO_HOME_LINK/bin/launcher"

cluster_order_script='
source "$1"
TRINO_HOME_LINK="$2"
_trino_remote_run() { printf "remote %s %s\n" "$1" "$2"; }
stop_trino_cluster
start_trino_cluster
'
bash -c "$cluster_order_script" bash "$TRINO_FUNCTIONS_FILE" "$TRINO_HOME_LINK" \
  > "$TEST_ROOT/bash-cluster-order"
zsh -f -c "$cluster_order_script" zsh "$TRINO_FUNCTIONS_FILE" "$TRINO_HOME_LINK" \
  > "$TEST_ROOT/zsh-cluster-order"

cat > "$TEST_ROOT/expected-cluster-order" <<'EOF'
stopping trino on root@worker-1 (worker, remote)
remote root@worker-1 stop
stopping trino on root@worker-2 (worker, remote)
remote root@worker-2 stop
stopping trino on root@coordinator (coordinator, local)
local stop
trino cluster stopped
starting trino on root@coordinator (coordinator, local)
local start
starting trino on root@worker-1 (worker, remote)
remote root@worker-1 start
starting trino on root@worker-2 (worker, remote)
remote root@worker-2 start
trino cluster started
EOF
if ! cmp -s "$TEST_ROOT/bash-cluster-order" "$TEST_ROOT/expected-cluster-order"; then
  printf 'the cluster functions act on the wrong nodes under bash\n' >&2
  diff -u "$TEST_ROOT/expected-cluster-order" "$TEST_ROOT/bash-cluster-order" >&2
  exit 1
fi
if ! cmp -s "$TEST_ROOT/zsh-cluster-order" "$TEST_ROOT/bash-cluster-order"; then
  printf 'the cluster functions act on different nodes under zsh than under bash\n' >&2
  diff -u "$TEST_ROOT/bash-cluster-order" "$TEST_ROOT/zsh-cluster-order" >&2
  exit 1
fi

mock_bin="$TEST_ROOT/mock-bin"
mkdir -p "$mock_bin"
cat > "$mock_bin/ssh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

last="${!#}"
if [[ "$last" == bash\ -lc\ * ]]; then
  bash -n -c "$last"
fi
EOF
cat > "$mock_bin/scp" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod +x "$mock_bin/ssh" "$mock_bin/scp"

env \
  PATH="$mock_bin:$PATH" \
  STATE_DIR="$STATE_DIR" \
  TRINO_DEPLOYMENT_FILE="$TRINO_DEPLOYMENT_FILE" \
  RUN_LOCAL_COORDINATOR=false \
  CONFIRM_TRINO_CLUSTER_INSTALL=true \
  REMOTE_REPO_ROOT="$TEST_ROOT/remote repo" \
  REMOTE_SKILL_SCRIPT="$TEST_ROOT/remote repo/install trino.sh" \
  REMOTE_DEV_SCRIPT="$TEST_ROOT/remote repo/install trino.sh" \
  REMOTE_STATE_DIR="$TEST_ROOT/remote state" \
  REMOTE_SCRIPT_DIR="$TEST_ROOT/remote scripts" \
  REMOTE_ARTIFACT_DIR="$TEST_ROOT/remote artifacts" \
  "$SKILL_DIR/scripts/install_trino_cluster.sh" >/dev/null

printf 'shell compatibility checks passed\n'
