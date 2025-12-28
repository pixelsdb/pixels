#!/bin/bash

usage() {
    echo "Pixels Maintenance Tool - Backup and Restore Utility"
    echo ""
    echo "Usage: $0 {dump|restore|restore_mysql|restore_etcd} [options]"
    echo ""
    echo "Actions:"
    echo "  dump            Backup both MySQL metadata and ETCD snapshot"
    echo "  restore         Full restoration of MySQL and ETCD (includes sync)"
    echo "  restore_mysql   Restore only the MySQL database"
    echo "  restore_etcd    Restore ETCD snapshot and sync to data directory"
    echo ""
    echo "Options:"
    echo "  -m, --db-file       Path to the MySQL .sql file (default: ./pixels_db_TIMESTAMP.sql)"
    echo "  -e, --etcd-file         Path to the ETCD .db snapshot (default: ./pixels_etcd_TIMESTAMP.db)"
    echo "  --etcd-dir          Actual ETCD data directory (default: \$HOME/opt/etcd/data)"
    echo "  --etcd-tmp-dir      Temporary path for ETCD extraction (default: /tmp/etcd-restore-data_TIMESTAMP)"
    echo ""
    echo "Examples:"
    echo "  $0 dump"
    echo "  $0 restore --db-file ./backup.sql --etcd-file ./etcd.db"
    echo "  $0 restore_etcd --etcd-file ./etcd.db --etcd-dir /custom/etcd/path"
}

# 1. Environment Check
if [ -z "$PIXELS_HOME" ]; then
    echo "ERROR: Environment variable PIXELS_HOME is not set."
    usage
    exit 1
fi

CONFIG_FILE="${PIXELS_HOME}/etc/pixels.properties"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "ERROR: Configuration file not found at $CONFIG_FILE"
    usage
    exit 1
fi

# 2. Extract configuration values
get_prop() {
    grep "^$1=" "$CONFIG_FILE" | cut -d'=' -f2- | tr -d '\r'
}

# MySQL Configs
DB_USER=$(get_prop "metadata.db.user")
DB_PASS=$(get_prop "metadata.db.password")
DB_URL=$(get_prop "metadata.db.url")
DB_HOST=$(echo "$DB_URL" | sed -e 's/.*:\/\/\([^:/]*\).*/\1/')
DB_PORT=$(echo "$DB_URL" | sed -e 's/.*:\([0-9]*\)\/.*/\1/')
DB_NAME=$(echo "$DB_URL" | sed -e 's/.*\/\([^?]*\).*/\1/' | cut -d'/' -f2)
DB_PORT=${DB_PORT:-3306}

# ETCD Configs
ETCD_HOSTS=$(get_prop "etcd.hosts")
ETCD_PORT=$(get_prop "etcd.port")
ETCD_ENDPOINT="${ETCD_HOSTS}:${ETCD_PORT}"

# 3. Argument Parsing
ACTION=$1
shift


# Set default filenames if not provided
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
# Default Values
ETCD_DATA_DIR="$HOME/opt/etcd/data"
ETCD_TMP_DIR="/tmp/etcd-restore-data_${TIMESTAMP}"
DB_FILE=""
ETCD_FILE=""

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --etcd-dir) ETCD_DATA_DIR="$2"; shift ;;
        -e|--etcd-tmp-dir) ETCD_TMP_DIR="$2"; shift ;;
        -m|--db-file) DB_FILE="$2"; shift ;;
        --etcd-file) ETCD_FILE="$2"; shift ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

DB_FILE=${DB_FILE:-"./pixels_db_$TIMESTAMP.sql"}
ETCD_FILE=${ETCD_FILE:-"./pixels_etcd_$TIMESTAMP.db"}

restore_mysql() {
    if [ -f "$DB_FILE" ]; then
        echo "Restoring MySQL..."
        mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" < "$DB_FILE"
        [ $? -eq 0 ] && echo "MySQL Restore OK" || echo "MySQL Restore FAILED"
    else
        echo "Skip MySQL: $DB_FILE not found"
    fi
}

restore_etcd() {
    # ETCD Restore
    if [ -f "$ETCD_FILE" ]; then
        echo "Restoring ETCD (Requires etcd node restart manually)..."
        # Note: snapshot restore usually creates a data directory
        ETCDCTL_API=3 etcdctl snapshot restore "$ETCD_FILE" --data-dir="${ETCD_TMP_DIR}"
        echo "ETCD Restore: Data extracted to ${ETCD_TMP_DIR}. Please swap your etcd data directory."
    else
        echo "Skip ETCD: $ETCD_FILE not found"
    fi

    echo "--- Starting ETCD Sync Operation ---"
    echo "Target Directory: $ETCD_DATA_DIR"

    # Stop ETCD Service
    # We try systemctl first; if it's not a service, we force kill
    if systemctl is-active --quiet etcd; then
        sudo systemctl stop etcd
    else
        echo "ETCD not managed by systemd. Attempting to kill process..."
        sudo pkill -9 etcd || echo "No running etcd process found."
    fi
    # Wait to ensure port is released and files are unlocked
    sleep 2
    # Synchronize data using rsync
    echo "Synchronizing data via rsync..."
    mkdir -p "$ETCD_DATA_DIR"
    rsync -av --delete "$ETCD_TMP_DIR/" "$ETCD_DATA_DIR/"
    
    if [ $? -eq 0 ]; then
        echo "Rsync completed successfully."
    else
        echo "ERROR: Rsync failed."
        exit 1
    fi

    # Restart ETCD Service
    echo "Restarting ETCD process..."
    if systemctl list-unit-files | grep -q etcd.service; then
        sudo systemctl start etcd
    else
        echo "Starting ETCD as a background process..."
        # Adjust path to your etcd binary if necessary
        sudo nohup etcd --data-dir="$ETCD_DATA_DIR" > /dev/null 2>&1 &
    fi

    sleep 2
    if ETCDCTL_API=3 etcdctl --endpoints="$ETCD_ENDPOINT" endpoint health > /dev/null 2>&1; then
        echo "SUCCESS: ETCD is synced and healthy."
    else
        echo "WARNING: ETCD restarted but health check failed. Please check logs."
    fi
    echo "SUCCESS: ETCD has been synced and restarted."
}

dump() {
    echo "--- Starting Backup Process ---"
    # MySQL Dump
    echo "[1/2] Backing up MySQL ($DB_NAME)..."
    mysqldump -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" -p"$DB_PASS" \
        --databases "$DB_NAME" --single-transaction --quick --no-tablespaces > "$DB_FILE"
    [ $? -eq 0 ] && echo "MySQL Dump OK: $DB_FILE" || echo "MySQL Dump FAILED"

    # ETCD Snapshot
    echo "[2/2] Backing up ETCD Snapshot..."
    ETCDCTL_API=3 etcdctl --endpoints="$ETCD_ENDPOINT" snapshot save "$ETCD_FILE"
    [ $? -eq 0 ] && echo "ETCD Snapshot OK: $ETCD_FILE" || echo "ETCD Snapshot FAILED"
}

# 4. Execution Logic
case $ACTION in
    dump)
        dump
        ;;
    restore_mysql)
        restore_mysql
        ;;
    restore_etcd)
        restore_etcd
        ;;
    restore)
        echo "--- Starting Restoration Process ---"
        restore_mysql
        restore_etcd
        ;;
    *)
        usage
        exit 1
        ;;
esac