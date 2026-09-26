#!/usr/bin/env bash
# Real etcd + production TransServer/RetinaServer checkpoint/restart/fail-closed lifecycle.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
MVN=${MVN:-mvn}
ETCD_BIN=${ETCD_BIN:-etcd}
: "${PIXELS_HOME:?PIXELS_HOME must contain the matching Retina native runtime}"

if [[ $(javac -version 2>&1) != 'javac 1.8'* ]]; then
    echo "Normal Pixels daemon verification requires JDK 8" >&2
    exit 2
fi
command -v "$ETCD_BIN" >/dev/null

WORK=${INGEST_DAEMON_WORK_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/pixels-ingest-daemon-XXXXXXXX")}
mkdir -p "$WORK"
WORK=$(cd "$WORK" && pwd)
CLIENT_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()')
PEER_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()')
RETINA_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()')
TRANSACTION_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()')
ETCD_PID=''

cleanup() {
    local result=$?
    trap - EXIT
    if [[ -n "$ETCD_PID" ]]; then
        kill "$ETCD_PID" 2>/dev/null || true
        wait "$ETCD_PID" 2>/dev/null || true
    fi
    printf 'Normal daemon verification exit=%s evidence=%s\n' "$result" "$WORK"
    exit "$result"
}
trap cleanup EXIT

if [[ ${INGEST_DAEMON_SKIP_BUILD:-0} != 1 ]]; then
    env -u LD_PRELOAD "$MVN" -B -ntp -f "$ROOT/pom.xml" -pl pixels-daemon -am \
        -DskipTests -Dpixels.retina.test.allocator="$PIXELS_HOME/lib/libjemalloc.so" \
        install > "$WORK/build.log" 2>&1
fi
env -u LD_PRELOAD "$MVN" -B -ntp -f "$ROOT/pixels-daemon/pom.xml" \
    org.apache.maven.plugins:maven-dependency-plugin:2.10:build-classpath \
    -Dmdep.outputFile="$WORK/dependencies.cp" > "$WORK/dependencies.log" 2>&1

"$ETCD_BIN" --name lifecycle \
    --data-dir "$WORK/etcd-data" \
    --listen-client-urls "http://127.0.0.1:$CLIENT_PORT" \
    --advertise-client-urls "http://127.0.0.1:$CLIENT_PORT" \
    --listen-peer-urls "http://127.0.0.1:$PEER_PORT" \
    --initial-advertise-peer-urls "http://127.0.0.1:$PEER_PORT" \
    --initial-cluster "lifecycle=http://127.0.0.1:$PEER_PORT" \
    --logger zap --log-level error > "$WORK/etcd.log" 2>&1 &
ETCD_PID=$!
for _ in $(seq 1 100); do
    if curl --silent --fail "http://127.0.0.1:$CLIENT_PORT/health" | grep -q '"health":"true"'; then
        break
    fi
    kill -0 "$ETCD_PID" 2>/dev/null || {
        echo "etcd exited before readiness" >&2
        exit 1
    }
    sleep .1
done
curl --silent --fail "http://127.0.0.1:$CLIENT_PORT/health" | grep -q '"health":"true"'

CP="$ROOT/pixels-daemon/target/test-classes:$ROOT/pixels-daemon/target/classes:$(cat "$WORK/dependencies.cp")"
export LD_LIBRARY_PATH="$PIXELS_HOME/lib:${LD_LIBRARY_PATH:-}"
ALLOCATOR=()
if [[ -f "$PIXELS_HOME/lib/libjemalloc.so.2" ]]; then
    ALLOCATOR=("LD_PRELOAD=$PIXELS_HOME/lib/libjemalloc.so.2${LD_PRELOAD:+:$LD_PRELOAD}")
elif [[ -f "$PIXELS_HOME/lib/libjemalloc.so" ]]; then
    ALLOCATOR=("LD_PRELOAD=$PIXELS_HOME/lib/libjemalloc.so${LD_PRELOAD:+:$LD_PRELOAD}")
fi
run_phase() {
    local phase=$1
    local state=${2:-$WORK/state}
    env "${ALLOCATOR[@]}" \
        PIXELS_CONFIG="$ROOT/pixels-common/src/main/resources/pixels.properties" \
        java -Xmx1g -cp "$CP" \
        io.pixelsdb.pixels.daemon.transaction.ingest.NormalIngestDaemonMain \
        "$state" "$CLIENT_PORT" "$RETINA_PORT" "$TRANSACTION_PORT" "$phase"
}

run_phase write 2>&1 | tee "$WORK/daemon.log"
run_phase recover 2>&1 | tee -a "$WORK/daemon.log"
grep -q '^PIXELS_NORMAL_INGEST_DAEMON_PHASE1_PASS rows=73 sharedFileTransactions=2 ' "$WORK/daemon.log"
grep -q '^PIXELS_NORMAL_INGEST_DAEMON_PASS rows=73 pixelsFiles=3 services=TransServer,RetinaServer checkpointRestart=2 sharedFileTransactions=2$' "$WORK/daemon.log"

# A committed decision is recovery authority. A checksum failure must stop the transaction
# server; it must never be treated as an empty coordinator on a fresh deployment.
cp -a "$WORK/state" "$WORK/corrupt-decision-state"
python3 - "$WORK/corrupt-decision-state/decisions/decisions.log" <<'PY'
import pathlib
import struct
import sys

path = pathlib.Path(sys.argv[1])
payload = bytearray(path.read_bytes())
journal_header_bytes = struct.calcsize('>II')
frame_header_bytes = struct.calcsize('>II')
if len(payload) <= journal_header_bytes + frame_header_bytes:
    raise SystemExit("decision journal is unexpectedly short")
payload[-1] ^= 0x01
path.write_bytes(payload)
PY
if run_phase fail-closed "$WORK/corrupt-decision-state" > "$WORK/corrupt-decision.log" 2>&1; then
    echo "daemon accepted corrupt committed decision state" >&2
    exit 1
fi
grep -q 'Coordinator journal checksum mismatch' "$WORK/corrupt-decision.log"

# Reusing a legacy timestamp domain without advancing the allocator must be rejected before
# new writes are admitted.
if run_phase cutover-reject > "$WORK/cutover-reject.log" 2>&1; then
    echo "daemon accepted an allocator value at/below the cutover baseline" >&2
    exit 1
fi
grep -q 'is not above ingest/cutover timestamp floor' "$WORK/cutover-reject.log"

# The recovery pointer remains in real etcd. Moving its body creates a recoverable, explicit
# missing-data fault and must keep Retina out of READY.
mapfile -t checkpoint_bodies < <(find "$WORK/state/recovery" -maxdepth 1 -type f -name 'recovery_*')
if [[ ${#checkpoint_bodies[@]} != 1 ]]; then
    echo "expected exactly one published recovery checkpoint body" >&2
    exit 1
fi
mv "${checkpoint_bodies[0]}" "${checkpoint_bodies[0]}.missing"
if run_phase fail-closed > "$WORK/missing-checkpoint.log" 2>&1; then
    echo "daemon accepted a missing recovery checkpoint body" >&2
    exit 1
fi
grep -Eq 'reading the checkpoint body failed|No such file' \
    "$WORK/missing-checkpoint.log"
mv "${checkpoint_bodies[0]}.missing" "${checkpoint_bodies[0]}"

run_phase cutover 2>&1 | tee -a "$WORK/daemon.log"
grep -Eq '^PIXELS_NORMAL_INGEST_CUTOVER_PASS oldRows=73 totalRows=74 baseline=1000000000 commitTimestamp=[0-9]+ legacyFence=1$' \
    "$WORK/daemon.log"
echo 'PIXELS_NORMAL_INGEST_FAIL_CLOSED_PASS corruptDecision=1 missingCheckpoint=1 allocatorFloor=1'
