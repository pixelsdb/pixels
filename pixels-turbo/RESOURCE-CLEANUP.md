# Query temporary resource cleanup

S3QS result collection waits for correct operator outputs and a durable cleanup
handoff, not for physical SQS/S3 deletion. Compute invocation still uses the
existing `Invoker` and worker cancellation/stop protocol. Task sizing, memory
budgets and autoscaling policy are unchanged.

## Ownership and API

`common.turbo.cleanup.TemporaryResourceManager` is a shared library, not another
query coordinator. Lambda and Spike use the same storage cleanup drivers through
`ServiceLoader<CleanupDriver>`. The manager has no planner or AWS SDK dependency.

* `register(owner, resources)` writes complete intent **before** resource creation.
  The returned UUID identifies a cleanup scope; it is not a physical worker ID.
* `release(scope)` records that admission/retries and all readers of that scope
  are closed. It acknowledges the release, not deletion.
* `confirmStopped(scope)` records positive evidence that preparation and all
  possible writers have stopped. A cancelled future, expired worker lease or
  elapsed query deadline is not such evidence.
* `snapshot(scope)` exposes release/stop evidence, GC status, remaining failed
  resources, the last error and retry time. `completion(scope)` completes only
  after drivers verify absence.
* `runOnce()` reconciles durable scopes without the original query, its callbacks
  or its JVM. A storage driver implements idempotent `delete` and `exists`.

The engine registers SHUFFLE and RESULT as separate scopes. After logical
execution it closes admission and releases shuffle; after positive stop evidence
it publishes the stopped marker. The Trino connector registers temporary output
prefixes before startup, and releases them at transaction/reader teardown after
**all** subplans are quiescent. It does not wait for shuffle GC. Persistent table
outputs, original input, roles, VPCs, security groups, deployed functions and
shared worker pools are never registered as query garbage.

Every planned S3QS shuffle gets a unique queue/object namespace. An old reaper
cannot delete a later execution's shuffle. Transaction IDs and final output
namespaces must likewise not be reused across live transactions.

## Journal and recovery

Configure the **same dedicated journal** in the query engine and backend manager:

```properties
enabled.storage.schemes=s3,s3qs,file
executor.cleanup.journal=s3://<management-bucket>/<deployment>/cleanup/
```

The journal must be outside all query data prefixes. Its credentials come from
the deployment, never from task records. Filesystem directories are also
supported; an empty setting uses `PIXELS_HOME/var/cleanup`. Local storage only
survives while that filesystem survives and is not a substitute for a shared
journal on ephemeral query controllers.
Use the same deployment region, endpoint and storage identity on both sides;
one journal belongs to one deployment.

Each scope has an immutable `.resources.json` document and separate `.released`
and `.stopped` evidence documents. Only reapers write `.status.json`. Atomic file
replacement or completed object publication keeps partial writes invisible.
Concurrent reapers may repeat operations; drivers must tolerate that. Successful
cleanup is recorded only after absence checks, including queues whose delete
request was acknowledged but which still exist. Partial failures retry after
five seconds without changing the SQL outcome or abandoning the obligation.

The old `QueryCleanupManifest` remains a query-local audit snapshot. Its GC state
can lag; `cleanupJournal` and `cleanupScope` point to authoritative manager state.
The former query-level abort-grace/cleanup-timeout budgets no longer turn slow GC
into a failed SQL result or a terminal deferred cleanup.

Unknown writers and missing reader release **remain blocked and visible**. This
revision deliberately does not infer stop or reader release after a timeout.
After 24 hours such records are marked `NEEDS_ATTENTION` and logged for backend
reconciliation; their age never authorizes deletion.
If the engine disappears before publishing that evidence, a backend/operator
must establish the missing facts before calling `confirmStopped`/`release`.
This implementation does not add a force-stop API to Lambda/Spike or claim that
worker quiescence is the end of platform invocation billing. Journal records are
retained for auditing; deployment retention is separate from query data deletion.
Storage-prefix cleanup follows the storage plugin's deletion semantics: S3
temporary buckets must be unversioned; historical versions and orphan multipart
uploads are outside this driver's contract.

## Backend management entry points

The query JVM starts a shared daemon reaper as an accelerator. For recovery after
that JVM exits, deploy a **separate periodic backend sweep** of the same journal.
Do not rely on a query thread, worker `finally`, or the query's last notification.

* Lambda: deploy the existing worker package with handler
  `io.pixelsdb.pixels.worker.lambda.CleanupWorker::handleRequest` as a separate
  management function. Its configuration requires an S3 journal. An empty event
  sweeps; `{"scope":"<scope-uuid>"}` returns status. It must have access to the
  management journal and only the deployment's temporary S3/SQS resources.
* Spike/control processes: periodically invoke the same library's `runOnce()`, or
  run its CLI using the backend's normal runtime classpath:

```sh
java -cp '<backend-runtime-classpath>' \
  io.pixelsdb.pixels.common.turbo.cleanup.TemporaryResourceManager
java -cp '<backend-runtime-classpath>' \
  io.pixelsdb.pixels.common.turbo.cleanup.TemporaryResourceManager --status <scope-uuid>
```

The Spike server's deployment is outside this repository; no new provider-specific
RPC is required for storage GC. Set `PIXELS_HOME`/`PIXELS_CONFIG` as for other
Pixels runtimes. The classpath needs the common and applicable storage plugins,
with their `META-INF/services` entries preserved. This change supplies entry
points; it does not create cloud functions, schedules, roles or other AWS resources.

## Verification

Enable the focused profiles explicitly (the default Maven build skips tests):

```sh
mvn -o -pl pixels-planner -am \
  -Pissue1277-cleanup-test,issue1277-planner-test \
  -Dtest=TestTemporaryResourceManager,TestCleanupS3Journal,TestManagedCleanup,TestShuffleAttemptNamespace,TestQueueCleanupDriver,TestQueryFailureLifecycle,TestCoordinatedPlanExecution,TestQueryExecutionState,TestWorkerCancellationProtocol,TestS3QSResourceFailureLifecycle \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Tests cover independent-JVM recovery, late stop acknowledgements, result reader
release, pre-create intent, lost create responses, partial deletion, retry after
restart, absence verification and successful collection during blocked deletion.
Connector changes live in the companion `pixels-trino` branch of the same name.
