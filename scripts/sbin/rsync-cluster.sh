#!/bin/bash

# Check if PIXELS_HOME is set
if [ -z "$PIXELS_HOME" ]; then
  echo "ERROR: PIXELS_HOME is not set."
  exit 1
fi

DEFAULT_PIXELS_HOME=$PIXELS_HOME

# Get local hostname to detect localhost nodes
LOCAL_HOSTNAME=$(hostname)

# ---------------------------
# Function: sync_node_lib
# Args: node, node_home
# Description: Sync ${PIXELS_HOME}/lib to the target node via rsync
# ---------------------------
sync_node_lib() {
    local node=$1
    local home=$2

    # Skip empty node
    [[ -z "$node" ]] && return

    # Use default PIXELS_HOME if not specified
    home="${home:-${DEFAULT_PIXELS_HOME}}"

    # Skip localhost (localhost, 127.0.0.1, or the same hostname)
    if [[ "$node" == "localhost" || "$node" == "127.0.0.1" || "$node" == "$LOCAL_HOSTNAME" ]]; then
        if [[ "$home" == "${DEFAULT_PIXELS_HOME}" ]]; then
          echo "[Info] Skipping localhost node: $node"
          return
        fi
    fi

    echo "[Info] Syncing lib to node: $node ($home)"
    rsync -az --delete --info=progress2 "${PIXELS_HOME}" "${node}:${home}"
}

# ---------------------------
# Sync worker nodes
# ---------------------------
if [[ -f "$PIXELS_HOME/etc/workers" ]]; then
    while read -r worker home; do
        # Skip empty lines or comments
        [[ -z "$worker" || "$worker" =~ ^# ]] && continue
        sync_node_lib "$worker" "$home"
    done < "$PIXELS_HOME/etc/workers"
fi

# ---------------------------
# Sync retina node
# ---------------------------
if [[ -f "$PIXELS_HOME/etc/retina" ]]; then
    read -r retina home < "$PIXELS_HOME/etc/retina"
    sync_node_lib "$retina" "$home"
fi

