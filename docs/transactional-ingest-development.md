# Transactional ingestion: SQL integration milestone

Related: pixelsdb/pixels-trino#180.

## Implemented components

`pixels-common.ingest` contains immutable stream/batch identities, exact seals,
a bounded columnar codec, authenticated RPC clients and a checksummed atomic
LOCAL state file. `LocalMutationJournal` retains transaction-private payloads.

`RetinaIngestParticipant` validates authoritative stream registration and exact
seals before preparing. `DurableIngestCoordinator` persists mutually exclusive
COMMIT/ABORT decisions, drives installation, and publishes only the complete
installed commit prefix. Its local state volume has a single owner.

`PixelsIngestInstaller` records existing allocator results and buffer placements
before shared installation. It reuses `MainIndex`, `SinglePointIndex`, native
Retina visibility, shared `PixelsWriteBuffer` instances and background Pixels
file generation. Keyless file finalization persists MainIndex without requiring
a business primary index. Identical rows are never deduplicated by value.

Read pins prevent physical file publication from racing a statement's file/buffer
selection. The buffer reader consumes prefetch results in source order, associates
each batch with its own visibility bitmap, drains the entire queue, propagates
read failures, and owns data before closing a physical reader's native buffers.

## Validation

The matching pixels-trino checkout provides:

```sh
bash /path/to/pixels-trino/tools/verify-sql-insert.sh "$PWD"
```

It starts real Trino SQL execution, separate-process RPC services, and actual
Retina/Pixels storage. The catalog, topology and external ID allocation are test
fixtures; data, installation, MainIndex, visibility and query results are not.
The test verifies 1,008 visible rows, including two INSERT SELECT statements,
and an aborted statement whose accepted private rows never become public.

Focused tests:

```sh
bash tools/verify-ingest-contract.sh
mvn -pl pixels-core -DskipTests=false -Dtest=TestBufferSnapshotRead test
mvn -pl pixels-daemon -DskipTests=false -Dtest=TestPixelsIngestStorage test
```

When using JDK 23, the native/local reader tests require the Java module opens
used by `verify-sql-insert.sh`. Use the installed native library path and preload
jemalloc when required by the Retina build. The root Surefire configuration now
honors the explicit `skipTests` property; check the reported executed test counts.

## Operational limits

This is a LOCAL-durability, fixed-owner experimental path, disabled unless both
the connector and Retina ingestion settings are enabled. It does not implement
replicated recovery or safe live ownership transfer. Prepare acknowledgements
require durable local records; permanent loss of the acknowledged volume is not
covered.

The new RPC services are started explicitly by the integration harness. Standard
daemon lifecycle registration, migration from the legacy timestamp domain and
production activation are not completed by this milestone. Do not mix the new
transaction domain with legacy writers or rewriting GC on the same table.

Journals and install plans currently retain recovery references and enforce
capacity limits. Transaction checkpointing, WAL/terminal-state reclamation and
rewrite-GC coordination remain separate work. `DELETE_ROWS` is reserved but not
enabled. Replay after arbitrary rewrite, table movement or schema evolution is
rejected rather than silently accepted.

The existing indexes remain authoritative. No parallel row directory, new row-ID
allocator or replacement index implementation is introduced.
