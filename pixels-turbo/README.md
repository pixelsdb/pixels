# Pixels Turbo

`Pixels-Turbo` is the query engine in Pixels that aims at improving the elasticity, performance, and cost-efficiency of
query processing. 
It prioritizes query processing in an auto-scaling MPP cluster running in spot VMs,
and automatically invokes cloud functions to process unpredictable workload spikes when the spot VMs do not have
enough resources.

## Components

Currently, `Pixels-Turbo` uses Trino as the query entry-point and the query processor in the MPP cluster, 
and implements the serverless query accelerator from scratch.
`Pixels-Turbo` is composed of the following components:
- [`pixels-trino`](https://github.com/pixelsdb/pixels-trino). In addition to reading data and metadata, 
this component automatically pushes down query operators in the Trino query plan into a logical sub-plan when the Trino cluster
does not have enough resources to process the query.
- [`pixels-planner`](../pixels-planner) accepts the logical sub-plan and compiles it into a physical sub-plan. During the planning, pixels-planner
selects the physical query operators and the parameters of the physical query operators. The root node of the 
physical plan provides the methods to invoke the serverless workers to execute the plan.
- `pixels-invoker-[service]` implements the invokers to be used by the physical plan to invoker the serverless workers in a 
specific cloud function service (e.g., AWS Lambda).
- `pixels-worker-[service]` implements the serverless workers in a specific cloud function service (e.g., AWS Lambda).
- `pixels-worker-common` has the common logics and the base implementations of the serverless workers.
- [`pixels-executor`](../pixels-executor) implements the basic executors of relational operations, such as projection, filter, join, and aggregation.
These executors are used in the serverless workers to execute the assigned physical operator.
- `pixels-scaling-[service]` implements the auto-scaling metrics collector for the specific virtual machine service (e.g., AWS EC2). 
It reports the performance metrics, such as CPU/memory usage and query concurrency, in the MPP cluster. These metrics are 
consumed by the scaling manager (e.g., AWS EC2 Autoscaling Group) in the cloud platform to make scaling decisions.

## Installation

Install `Pixels-Trino` following the instructions [HERE](../docs/INSTALL.md).

To use Pixels-Turbo, we need to set the following properties in `PIXELS_HOME/etc/pixels.properties`:
```properties
executor.input.storage.scheme=s3
executor.intermediate.storage.scheme=s3
executor.intermediate.folder=/pixels-turbo/intermediate/
executor.output.storage.scheme=output-storage-scheme-dummy
executor.output.folder=output-folder-dummy
```
These storage schemes are used to access the input data (the storage scheme of the base tables defined by
`CREATE TABLE` statements), the intermediate data (intermediate results generated during query execution), and the
output data (the result of the sub-plan executed in the serverless workers), respectively.
If any of these storage schemes is `minio` or `redis`, we also need to set the properties for the corresponding storage:
```properties
minio.region=eu-central-2
minio.endpoint=http://localhost:9000
minio.access.key=minio-access-key-dummy
minio.secret.key=minio-secret-key-dummy
redis.endpoint=localhost:6379
redis.access.key=redis-user-dummy
redis.secret.key=redis-password-dummy
```
Ensure they are valid so that the serverless workers can access the corresponding storage systems.
Especially, the `executor.input.storage.scheme` must be consistent with the storage scheme of the base
tables. This is checked during query-planning for Pixels-Turbo.
In addition, the `executor.intermediate.folder` and `executor.output.folder` are the base path where the intermediate
and output data are stored. They also need to be valid and accessible for the serverless workers.

Then, deploy the serverless workers following the instructions in the README.md of `pixels-worker-[service]`.
Currently, we support AWS Lambda and vHive. So the worker service can be `pixels-worker-lambda` or `pixels-worker-vhive`.
More platform integrations will be provided in the future.

If the MPP cluster is running in a cloud machine service such as AWS EC2, follow the instructions in the README.md of `pixels-scaling-[service]`
to deploy the auto-scaling manager for the MPP cluster. Currently, we support auto-scaling in AWS EC2. So `pixels-scaling-ec2` is the only option.
It can be used as an example to implement the auto-scaling manager for other cloud platforms.

## S3QS asynchronous shuffle I/O

When the planner selects S3QS shuffle, producers hand completed partition batches to an invocation-local,
bounded I/O pool while scanning continues. Each background task encodes bounded objects, uploads them,
then publishes their DATA messages. All DATA tasks must succeed before the producer publishes any END.
Consumers acknowledge processed DATA and END messages in a separate bounded pool and await all
acknowledgements before returning partition success. Background failures fail the task; no application-level
recovery is added. An END sequence N promises DATA sequences [0, N) from that logical producer to that
partition. Consumers deduplicate successfully processed DATA by producer and sequence, and finish once
every producer's range is complete and ACKs succeed, without a final empty poll. END may arrive before
DATA; missing sequences keep the consumer polling with cancellation checks, even after empty responses.
Sequence-boundary and producer-attempt checks are retained. Batch shuffle is unchanged.

These optional properties in the worker's `PIXELS_HOME/etc/pixels.properties` have defaults for compatibility
with older configurations:

```properties
s3qs.producer.io.threads=4
s3qs.consumer.ack.threads=2
s3qs.consumer.ack.max.inflight=32
```

Limits are per invocation, not additional cloud Workers. Producer execution uses a fixed number of I/O
threads, without a separate limit on the number of queued write jobs. With the partition memory policy
enabled, the shared output byte budget applies cancellation-aware backpressure and covers partial buffers,
queued batches and running writes until publication completes. Without that policy, there is no producer
queue admission bound. The former `s3qs.producer.io.max.inflight` property is no longer used.
The consumer acknowledgement in-flight limit still includes running and queued ACK jobs.
Producer jobs retain batches until publication completes; partial per-partition buffers are grouped using an
8 MiB memory estimate, with the existing writer enforcing the actual encoded object size. Scan-side partition
buffers still exist, so these limits are not a hard cap on the invocation's entire heap. Output I/O metrics count
background job durations and END publication; these may overlap scanning and must not be added to scan
wall time as an end-to-end latency breakdown.

## S3QS query completion and cleanup

Coordinated output collection waits for execution and owned shuffle cleanup to succeed, restoring the
original synchronous completion path. Cleanup first requires resource preparation and remote workers to
stop; unknown workers retain the resource inventory instead of permitting unsafe deletion. A cleanup
failure or timeout fails collection and keeps the manifest and terminal coordinator for recovery.
`executor.s3qs.cleanup.timeout.ms` bounds deletion, separately from the query execution deadline.
Only query-owned shuffle queues, registrations and objects are deleted here. Lambda function definitions,
VPCs, security groups and endpoints belong to the backend deployment lifecycle. Final result paths remain
owned by the connector's existing file cleaner after transaction termination and safe shuffle cleanup.

## Experimental resource-aware partition producers

```properties
executor.partition.memory.planner.enabled=false
executor.partition.memory.worker.enabled=false
executor.partition.memory.planning.mb=0
```

Planner enablement obtains the actual producer function's memory and CPU hint from its backend invoker
(`PARTITION` for Batch, `PARTITION_S3QS` for S3QS). Lambda already caches MemorySize when its invoker is
initialized; its CPU hint approximates one vCPU per 1769 MiB, up to six. Spike reports its existing
backend request defaults. Unknown/nonpositive backend memory is an explicit planning error. Changing a
function's configuration requires refreshing/reloading its invoker resource snapshot. By default planning
uses that backend memory. The optional `executor.partition.memory.planning.mb` is a planning-only upper
bound in MiB: 0 (also the default when absent) follows the backend; a positive value uses the smaller of
that value and backend memory. Negative, malformed or overflowing values fail planning. For example,
6144 on a 10240 MiB backend plans at 6144 MiB while retaining the backend's CPU hint. It does not change
Invoker resources, deployment memory, JVM heap or billing. It applies when planner memory policy is
enabled and travels to workers in the existing policy; no matching worker-side configuration is needed.
The old `worker.mb`, `reader.threads` and `window.row.groups` settings remain unused.

Base-table task sizing uses the existing metadata service's average column-chunk sizes, row count and
output schema. It never opens an input file or fetches RG footers. S3QS targets roughly 1.5 times planning-reference
memory in selected encoded input per logical task. Batch targets 90% of its output pool minus partition
tail capacity, using a 1.5x retained-output estimate and a coarse variable-width allowance. These are
initial heuristics, not exact peak-memory predictions. Missing statistics use the existing row-group
size as an estimate; unknown input ranges stay intact. Filters do not shrink memory estimates.

One PartitionInput remains one logical producer. RG ranges are split/combined before ids and END
membership are frozen. Each task receives about two input slices per planned scan thread; the former
fixed eight-split task cap no longer applies when the policy is enabled. Hash partition counts and the
DATA/END protocol are unchanged. More backend memory gives larger tasks; CPU hints determine the
independent slices available inside each task. Partition-tail capacity can reduce the scan-thread hint.

Budgets use up to 85% of planning-reference memory, leaving native/SDK headroom; workers also clamp this working
set to actual JVM max heap. Input and output byte pools each receive 35% of that working set. Another
15% bounds partition-tail capacity, and the remainder covers decoded batches and runtime overhead.
These figures are initial resource fractions, not an RSS hard cap. Lambda wrappers pass the actual
runtime memory limit and only tighten a policy whose reference exceeds that limit. A smaller planned
policy is preserved. For a task without a policy, worker enablement still derives a fallback from the
actual runtime platform limit (or JVM heap if unknown), without consulting the planner-only cap.
Memory metrics report `planningMemoryBytes` (effective policy reference) and `platformMemoryBytes`
(actual context limit, 0 when unknown); the legacy `backendMemoryBytes` field retains its policy-reference
meaning for compatibility. None of these admission counters is an RSS or JVM/native hard boundary.

Scan concurrency uses the backend CPU hint clamped to runtime availableProcessors and actual task
slices. Windows are byte-sized rather than capped at four RG: each reader targets its share of the input
pool, charging twice encoded bytes for chunks and a possible read copy. Normal sizing uses whole-RG
lengths from the already loaded file tail. Only a missing/oversized current RG requires exact selected
chunk sizing. Closed record readers and owned binary values allow input windows to be reclaimed.
Oversized selected RGs and retained Batch output fail explicitly; no spill or next-window prefetch is added.

S3QS pending, queued and executing batches share the output pool. Under pressure, scanners wait for
queued/running writes to release bytes. Only when no such write can progress is the largest pending
partition force-flushed; normal full groups and final tails still flush. This preserves mergeable batches,
though it may delay partial objects and does not guarantee every writer is occupied. Producer writes have no separate
job-count admission limit; the configured I/O thread count still bounds execution, and the 8 MiB object
limit still applies. The byte budget only exists when the memory policy is active; without it, the
producer write queue has no admission bound. Partitioner tails, encoder/SDK/native buffers and metadata are not all
represented by these two byte counters. The independent idle S3QS buffer pool retains at most four
buffers. Producer encoding scratch separately reuses `FixSizedBuffers` by writer concurrency until the
writer pool terminates; scratch is returned after trial encoding failure/overflow or upload completion.
An empty cache may allocate, so this is reuse, not another admission limit or zero-copy upload. Columns
continue to encode in each file's existing thread pool. Column exceptions complete their futures, and
rejected submissions still wait for columns already running before returning failure.
MemoryMetrics reports backend/usable memory, actual scan threads, byte limits/peaks/waits,
tail limit and window counts. Wait/cost counters may overlap and must not be summed as query latency.

## S3 input and worker resource lifecycle

S3 range reads use storage-owned clients. Failed requests propagate to the scheduler and window
waiters; a failed or interrupted window cancels siblings and settles its buffer callbacks before
releasing references. Null/short responses and submission failures also terminate the wait. The SDK
keeps responsibility for retries; S3 disables additional overlapping scheduler retries and per-read
client reconstruction/synchronous fallback.

With `s3.use.async.client=true`, all range sizes use the SDK Netty HTTP transport, whose cancellation
path aborts requests (the former SDK 2.30.17 CRT path did not implement this). With it disabled, GETs
run on their calling scanner so cancellation cannot leave detached synchronous executor work.
Reader close never closes shared clients. JVM-wide range admission uses the existing
`s3.max.request.concurrency` setting; synchronous reads keep their slot until the call returns.

Cancellation of an SDK future is not acknowledgement that its connection has stopped: Netty schedules
abort on its event loop. Reader close waits for owned submissions/SDK calls to settle, and reports
an error after five seconds if they do not. This is not a transport-quiescence guarantee or a strict
bound on all in-flight network buffers. A loopback test documents the event-loop-delayed abort case.

The shared worker footer cache retains at most 200 file tails and 5000 RG footers; these are entry
limits, not byte limits. Other callers using the default cache constructor retain the previous behavior.
S3QS task scopes reference-count local queue wrappers and release them after the owning scanner,
producer writer and consumer ACK pools actually terminate. Slow children keep their leases even
when their handler has returned an error. Partial setup and repeated scope close are handled without
releasing another task's lease; a failed wrapper close is logged while remaining wrappers are released.
Remote queues and shared clients retain their existing query/storage ownership.

## S3QS partitioned-join consumer behavior

The producer memory policy above does not apply to BasePartitionedJoinWorker. A normal S3QS join task
first obtains both schemas (possibly waiting for messages), consumes its small-table partition into an
in-memory hash table, and then consumes the large-table partition. The S3QS physical reader downloads
one complete object before decoding row batches. Message polling, object reads and join computation
run sequentially on the executing thread in the two-sided S3QS path; the generic cores-times-two pool
created at entry is used by the Batch file branches, not by this S3QS data path.

Joined batches accumulate in unbounded result queues and are written through the normal PixelsWriter
after probing completes. There is no consumer input/output byte policy, explicit hash-table capacity
budget, spill, or overlapping result writer in this path. The 8 MiB input-object limit therefore does
not bound decoded data, retained build state, or accumulated join output. Hash-table tuples retain
column vectors; variable-width references may also prolong input-buffer lifetimes.

Acknowledgements remain separate asynchronous work: two threads and at most 32 queued/running ACKs
by default. A full ACK scope blocks further message handling; successful task completion waits for ACKs.
These limits were retained independently of producer write-slot removal. The stage runner executes
logical tasks sequentially and maintains a separate lease heartbeat; waiting on one partition does
not switch that invocation to another logical task.

## Start Pixels (with Turbo)

In `etc/catalog/pixels.properties` under the installation directory of Trino, set `cloud.function.switch` to `auto` if you have installed the auto-scaling manager and 
want to enable auto-scaling of MPP cluster and adaptive invocation of serverless workers; or set it to `on` if you want to always push the queries into serverless workers.
```properties
# serverless config
# it can be on, off, auto
cloud.function.switch=auto
local.scan.concurrency=0
clean.intermediate.result=true
```
`local.scan.concurrency` is the number of concurrent scan tasks that can be executed locally (not in the serverless workers) in the MPP cluster.
It only takes effects for the table scan operator when the query is pushed down into serverless workers.
By setting this number to a value larger than 0, the MPP worker and the serverless workers can work in parallel to improve table scan throughput.
`clean.intermediate.result` controls whether delete the intermediate results when the queries executed in serverless workers are finished.

After that, start Trino and run queries. Pixels will automatically push the queries into the serverless workers when Trino 
is too busy to process the new coming queries (in `auto` mode).
