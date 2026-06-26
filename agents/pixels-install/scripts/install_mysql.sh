#!/usr/bin/env bash
set -euo pipefail

# Installs MySQL, sets the root password, and creates the Pixels metadata
# database/user described in docs/INSTALL.md ("Install MySQL"). Both the
# MySQL root password and the pixels DB user password default to
# "password" (as documented) but are always confirmed interactively unless
# the caller already exported them or passed --assume-yes. Per the
# pixels-install guardrails, this script never runs mysql_secure_installation
# non-interactively; it only sets the root password and creates the
# application database/user via direct SQL.
#
# On success it writes agents/pixels-install/deployment.secrets.env (mode
# 600, untracked by git) so configure_pixels.sh can pick up the same
# shell-quoted METADATA_DB_USER/METADATA_DB_PASSWORD without the caller
# re-typing them.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AGENT_DIR="${AGENT_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
REPO_ROOT="${REPO_ROOT:-$(cd "$AGENT_DIR/../.." && pwd)}"

MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-}"
METADATA_DB_USER="${METADATA_DB_USER:-pixels}"
METADATA_DB_PASSWORD="${METADATA_DB_PASSWORD:-}"
METADATA_DB_NAME="${METADATA_DB_NAME:-pixels_metadata}"
# '%' allows the pixels DB user to connect from any host; set to 'localhost'
# for a single-node deployment with no remote metadata access.
METADATA_DB_USER_HOST="${METADATA_DB_USER_HOST:-%}"
SCHEMA_FILE="${SCHEMA_FILE:-$REPO_ROOT/scripts/sql/metadata_schema.sql}"
SECRETS_FILE="${SECRETS_FILE:-$AGENT_DIR/deployment.secrets.env}"
ASSUME_YES="${ASSUME_YES:-false}"
DEFAULT_PASSWORD="password"
MYSQL_ROOT_OPTION_FILE=""

cleanup() {
  if [[ -n "$MYSQL_ROOT_OPTION_FILE" ]]; then
    rm -f "$MYSQL_ROOT_OPTION_FILE"
  fi
}
trap cleanup EXIT

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

sudo_cmd() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
  elif command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
    sudo -n "$@"
  else
    fail "run as root or configure passwordless sudo for this user"
  fi
}

confirm_action() {
  local message="$1"
  local reply

  [[ "$ASSUME_YES" == "true" ]] && return 0
  [[ -t 0 ]] || fail "not running interactively; set ASSUME_YES=true to proceed without confirmation: $message"

  read -r -p "$message [y/N]: " reply
  [[ "$reply" =~ ^[Yy]$ ]] || fail "aborted: $message"
}

# Reads a password into the named variable, prompting twice for confirmation
# when not already set via environment variable. Never echoes the value.
prompt_password() {
  local prompt_text="$1"
  local varname="$2"
  local value confirm_value

  if [[ -n "${!varname}" ]]; then
    return
  fi

  [[ -t 0 ]] || fail "$varname is not set and no TTY is available; export $varname before running non-interactively"

  read -r -s -p "$prompt_text [default: $DEFAULT_PASSWORD]: " value
  echo >&2
  value="${value:-$DEFAULT_PASSWORD}"

  read -r -s -p "Confirm $prompt_text: " confirm_value
  echo >&2
  confirm_value="${confirm_value:-$DEFAULT_PASSWORD}"

  [[ "$value" == "$confirm_value" ]] || fail "passwords did not match for $varname"
  printf -v "$varname" '%s' "$value"
}

validate_identifier() {
  local value="$1"
  local name="$2"

  [[ "$value" =~ ^[A-Za-z0-9_]+$ ]] ||
    fail "$name must contain only letters, numbers, and underscore: $value"
}

validate_user_host() {
  case "$METADATA_DB_USER_HOST" in
    %|localhost|127.0.0.1|::1) return ;;
  esac

  [[ "$METADATA_DB_USER_HOST" =~ ^[A-Za-z0-9_.:-]+$ ]] ||
    fail "METADATA_DB_USER_HOST contains unsupported characters: $METADATA_DB_USER_HOST"
}

validate_password() {
  local value="$1"
  local name="$2"

  [[ "$value" != *$'\n'* && "$value" != *$'\r'* ]] ||
    fail "$name must not contain newlines"
}

validate_inputs() {
  validate_identifier "$METADATA_DB_USER" METADATA_DB_USER
  validate_identifier "$METADATA_DB_NAME" METADATA_DB_NAME
  validate_user_host
  validate_password "$MYSQL_ROOT_PASSWORD" MYSQL_ROOT_PASSWORD
  validate_password "$METADATA_DB_PASSWORD" METADATA_DB_PASSWORD
}

mysql_string_literal() {
  local value="$1"

  value="${value//\\/\\\\}"
  value="${value//\'/\\\'}"
  printf "'%s'" "$value"
}

create_mysql_root_option_file() {
  local previous_umask

  [[ -n "$MYSQL_ROOT_OPTION_FILE" && -f "$MYSQL_ROOT_OPTION_FILE" ]] && return

  previous_umask="$(umask)"
  umask 177
  MYSQL_ROOT_OPTION_FILE="$(mktemp "${TMPDIR:-/tmp}/pixels-mysql-root.XXXXXX.cnf")"
  cat > "$MYSQL_ROOT_OPTION_FILE" <<EOF
[client]
user=root
password=$MYSQL_ROOT_PASSWORD
EOF
  umask "$previous_umask"
  chmod 600 "$MYSQL_ROOT_OPTION_FILE"
}

install_mysql_server() {
  if command -v mysql >/dev/null 2>&1; then
    log "mysql client already installed"
  else
    require_command apt-get
    log "installing mysql-server"
    sudo_cmd apt-get update
    sudo_cmd env DEBIAN_FRONTEND=noninteractive apt-get install -y mysql-server
  fi

  sudo_cmd systemctl enable --now mysql 2>/dev/null || sudo_cmd service mysql start
}

# Fresh Ubuntu mysql-server installs use auth_socket for root, so root@localhost
# can run mysql via sudo without a password. Once we set a real root password
# below, this falls back to password auth for subsequent calls in the same run.
mysql_root_exec() {
  if sudo_cmd mysql -uroot -e 'SELECT 1' >/dev/null 2>&1; then
    sudo_cmd mysql -uroot "$@"
  else
    create_mysql_root_option_file
    mysql --defaults-extra-file="$MYSQL_ROOT_OPTION_FILE" "$@"
  fi
}

set_root_password() {
  local root_password_sql
  root_password_sql="$(mysql_string_literal "$MYSQL_ROOT_PASSWORD")"

  log "setting MySQL root password"
  mysql_root_exec <<SQL
SET SESSION sql_mode = REPLACE(@@SESSION.sql_mode, 'NO_BACKSLASH_ESCAPES', '');
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY $root_password_sql;
FLUSH PRIVILEGES;
SQL
}

create_pixels_user_and_db() {
  local user_sql host_sql password_sql
  user_sql="$(mysql_string_literal "$METADATA_DB_USER")"
  host_sql="$(mysql_string_literal "$METADATA_DB_USER_HOST")"
  password_sql="$(mysql_string_literal "$METADATA_DB_PASSWORD")"

  log "creating database '$METADATA_DB_NAME' and user '$METADATA_DB_USER'@'$METADATA_DB_USER_HOST'"
  mysql_root_exec <<SQL
SET SESSION sql_mode = REPLACE(@@SESSION.sql_mode, 'NO_BACKSLASH_ESCAPES', '');
CREATE DATABASE IF NOT EXISTS \`$METADATA_DB_NAME\`;
CREATE USER IF NOT EXISTS $user_sql@$host_sql IDENTIFIED BY $password_sql;
ALTER USER $user_sql@$host_sql IDENTIFIED BY $password_sql;
GRANT ALL PRIVILEGES ON \`$METADATA_DB_NAME\`.* TO $user_sql@$host_sql;
FLUSH PRIVILEGES;
SQL
}

load_schema() {
  [[ -f "$SCHEMA_FILE" ]] || fail "metadata schema file not found: $SCHEMA_FILE"

  if mysql_root_exec -N -e "SHOW TABLES IN \`$METADATA_DB_NAME\`" | grep -q .; then
    log "database '$METADATA_DB_NAME' already has tables; skipping schema load"
    return
  fi

  log "loading schema from $SCHEMA_FILE"
  mysql_root_exec "$METADATA_DB_NAME" < "$SCHEMA_FILE"
}

write_secrets_file() {
  local previous_umask
  previous_umask="$(umask)"
  umask 177

  {
    cat <<EOF
# Generated by pixels-install/scripts/install_mysql.sh - contains a secret,
# do not commit. Sourced by configure_pixels.sh to keep pixels.properties in
# sync with the MySQL credentials created here.
EOF
    printf 'METADATA_DB_USER=%q\n' "$METADATA_DB_USER"
    printf 'METADATA_DB_PASSWORD=%q\n' "$METADATA_DB_PASSWORD"
    printf 'METADATA_DB_NAME=%q\n' "$METADATA_DB_NAME"
  } > "$SECRETS_FILE"

  umask "$previous_umask"
  chmod 600 "$SECRETS_FILE"
  log "credentials written to $SECRETS_FILE (configure_pixels.sh reads this automatically)"
}

main() {
  confirm_action "This will install/configure MySQL and create the Pixels metadata database '$METADATA_DB_NAME'. Continue?"

  prompt_password "MySQL root password" MYSQL_ROOT_PASSWORD
  prompt_password "Pixels metadata DB password for user '$METADATA_DB_USER'" METADATA_DB_PASSWORD
  validate_inputs

  install_mysql_server
  set_root_password
  create_pixels_user_and_db
  load_schema
  write_secrets_file

  log "MySQL setup complete: database=$METADATA_DB_NAME user=$METADATA_DB_USER@$METADATA_DB_USER_HOST"
  if [[ "$METADATA_DB_USER_HOST" != "localhost" ]]; then
    log "the '$METADATA_DB_USER' grant allows connections from '$METADATA_DB_USER_HOST', but MySQL itself may still only be listening on 127.0.0.1 by default"
    log "per docs/INSTALL.md (Install MySQL): change bind-address in mysqld.cnf and restart MySQL if you actually need other hosts to reach this server"
  fi
}

main "$@"
