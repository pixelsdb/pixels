# SQLite MainIndex

This module implements the SQLite-backed `MainIndex`. It stores
`rowId -> RowLocation` mappings as row-id ranges in SQLite and uses a per-file
durable marker to make file-scoped persistence retryable.

The primary table is `row_id_ranges`. A file-scoped persistence operation writes
the ranges for one file and one row in `row_id_range_flush_markers` in the same
SQLite transaction. The marker records the `file_id`, entry count, range count,
and a deterministic SHA-256 hash of the persisted ranges.

If a later retry sees a matching marker, the file's ranges are already durable.
If it sees conflicting marker metadata, or ranges without a matching marker, the
backend fails closed instead of silently accepting ambiguous index state.

## Test Setup

Commands below assume they are run from the repository root:

```bash
cd /path/to/pixels
```

If you are currently in this module directory, run:

```bash
cd ../..
```

The root `pom.xml` configures Surefire with `skipTests=true`, so
`mvn test -Dtest=...` still reports `Tests are skipped` for this module. To run
only a few SQLite tests without changing the POM, compile the module first and
then invoke Maven Failsafe directly. Failsafe is not bound by the inherited
Surefire `skipTests=true` setting.

## Compile The Module

```bash
mvn -pl pixels-index/pixels-index-main-sqlite -am \
  test-compile
```

This compiles the module and its reactor dependencies, including test classes,
but does not execute the JUnit tests.

## Correctness Tests

Run the main correctness suite:

```bash
mvn -pl pixels-index/pixels-index-main-sqlite -am \
  test-compile \
  org.apache.maven.plugins:maven-failsafe-plugin:2.22.2:integration-test \
  org.apache.maven.plugins:maven-failsafe-plugin:2.22.2:verify \
  -Dit.test=TestSqliteMainIndex \
  -DfailIfNoTests=false
```

This covers normal put/get/delete behavior and the durable flush marker cases:

- missing `fileId` flush is a no-op success;
- normal put -> flush -> lookup/delete;
- matching durable marker is accepted as an idempotent retry;
- marker metadata/hash conflicts fail closed and leave buffer retryable;
- dirty ranges without marker fail closed and leave buffer retryable;
- marker insert failure rolls back the range inserts;
- close/reopen flushes cached ranges and keeps rows readable.

Run the JDBC range query correctness test:

```bash
mvn -pl pixels-index/pixels-index-main-sqlite -am \
  test-compile \
  org.apache.maven.plugins:maven-failsafe-plugin:2.22.2:integration-test \
  org.apache.maven.plugins:maven-failsafe-plugin:2.22.2:verify \
  -Dit.test=TestSqliteMainIndexQuery \
  -DfailIfNoTests=false
```

This test writes a small file-scoped set of entries, flushes it, queries
`row_id_ranges` through JDBC, and asserts the persisted ranges are correct.

## Performance Benchmark

The benchmark is not a correctness gate. It is disabled by default and only runs
when explicitly enabled:

```bash
mvn -pl pixels-index/pixels-index-main-sqlite -am \
  test-compile \
  org.apache.maven.plugins:maven-failsafe-plugin:2.22.2:integration-test \
  org.apache.maven.plugins:maven-failsafe-plugin:2.22.2:verify \
  -Dit.test=TestSqliteMainIndexBenchmark \
  -DfailIfNoTests=false \
  -Dpixels.sqlite.main.index.benchmark=true \
  -Dpixels.sqlite.main.index.benchmark.contiguousRows=1000000 \
  -Dpixels.sqlite.main.index.benchmark.fragmentedRows=10000
```

Parameters:

- `pixels.sqlite.main.index.benchmark`: must be `true` to run the benchmark.
- `pixels.sqlite.main.index.benchmark.contiguousRows`: row count for contiguous
  rowId workloads. Default: `1000000`.
- `pixels.sqlite.main.index.benchmark.fragmentedRows`: row count for fragmented
  rowId workloads. Default: `100000`.

The benchmark prints a parameter block first, for example:

```text
SQLite MainIndex benchmark parameters
  -Dpixels.sqlite.main.index.benchmark=true
  -Dpixels.sqlite.main.index.benchmark.contiguousRows=1000000
  -Dpixels.sqlite.main.index.benchmark.fragmentedRows=10000
  index.sqlite.path=/tmp/sqlite
  java.version=23.0.2
  os.name=Linux
  os.arch=amd64
```

Then it prints a summary table:

```text
SQLite MainIndex benchmark summary
rows = logical MainIndex entries; ranges = persisted row_id_ranges.
markerRetry = retry when a matching per-file durable marker already exists.
emptyRetry = immediate second flush after marker retry discarded the buffer.
workload                    shape                                   rows     ranges markers    put(ms)    put rows/s  flush(ms)   flush ranges/s markerRetry(ms) emptyRetry(ms)    get(ms)    get rows/s
hot put/get path            contiguous, pre-flush get          1,000,000          1       1    ...
contiguous first flush      contiguous rows -> 1 range         1,000,000          1       1    ...
fragmented first flush      1-row gaps -> many ranges             10,000     10,000       1    ...
marker-hit retry flush      matching marker already durable       10,000     10,000       1    ...
```

How to read the table:

- `rows`: logical entries inserted into `MainIndex`.
- `ranges`: persisted `row_id_ranges` count after flush.
- `markers`: persisted `row_id_range_flush_markers` count.
- `put(ms)` / `put rows/s`: in-memory `putEntry` hot path.
- `flush(ms)` / `flush ranges/s`: first durable flush path.
- `markerRetry(ms)`: retry path when SQLite already has a matching durable marker.
- `emptyRetry(ms)`: immediate second flush after marker retry discarded the buffer.
- `get(ms)` / `get rows/s`: lookup cost after the workload setup.

For durable flush marker overhead, focus on:

- `contiguous first flush` `flush(ms)`: best-case file flush, many rows become one
  range plus one marker.
- `fragmented first flush` `flush(ms)`: many persisted ranges plus one marker.
- `marker-hit retry flush` `markerRetry(ms)`: crash/retry path after the previous
  transaction committed but the in-memory buffer was not discarded.

Large fragmented workloads can take much longer than contiguous workloads. That
is expected because `N` fragmented rows produce `N` SQLite ranges, while
contiguous rows often collapse into a single range.
