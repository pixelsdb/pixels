# Issue 1277: gated Spike/S3QS verification

This is an opt-in integration test against real Spike, ECS, S3, and SQS. The
test is skipped unless `PIXELS_S3QS_SPIKE_IT_ENABLED=true`; a green Maven
build without that variable is not remote verification. The root POM also
skips tests by default, so select `-Pissue1277-spike-it` and inspect the
Surefire report for `skipped=0`.

## Build and deployment contract

Build from the repository root:

```bash
mvn -pl pixels-turbo/pixels-worker-spike -am -DskipTests package
docker build -f pixels-turbo/pixels-worker-spike/Dockerfile -t "$IMAGE_URL" .
```

Push the image to a registry reachable by ECS, record its immutable digest,
and register two temporary Spike functions (partition producer and
partitioned join consumer) using that image. Do not replace an existing
function or retag a production image. The test server must use `max_retry=1`
and a dedicated metadata database. Use fixed, prewarmed Fargate pools so
Pixels Worker invocation scaling is not confused with ECS service scaling.
The tested capacity is four producer instances and two join instances, each
at 1024 CPU units and 2048 MiB. The ECS task role needs ECR pull/CloudWatch
logging plus access to only the chosen test S3 prefix and SQS queues; the
driver credentials need S3/SQS creation and cleanup permissions. Prefer a
dedicated least-privilege role over a shared full-access role.

The Worker must reach the Spike endpoint on TCP 50052 and the test
Coordinator on its advertised host/port. When the driver is behind NAT, use
an authenticated relay; do not advertise an address the Worker cannot reach.
Unset HTTP(S)/ALL proxy variables on the Spike server if they intercept
Worker gRPC. Set `PIXELS_CONFIG` **before** the test JVM starts: `ConfigFactory`
and the Spike client cache settings. Start from the complete
`pixels-common/src/main/resources/pixels.properties` and set at least:

```properties
enabled.storage.schemes=s3,s3qs
executor.function.service=spike
executor.exchange.method=s3qs
executor.shuffle.storage.scheme=s3qs
executor.worker.lease.period.ms=10000
spike.hostname=127.0.0.1
spike.port=13306
spike.cpu=1024
spike.memory=2048
spike.timeout=600
worker.coordinate.server.host=<Worker-reachable Coordinator address>
worker.coordinate.server.port=18893
s3qs.partition.worker.name=<temporary producer function>
s3qs.partitioned.join.worker.name=<temporary join function>
```

For R1, set `executor.s3qs.scaling.enabled=false` and max workers to 1.
For R2/R3/R4, set it to `true`, initial/min workers to 1, max workers to 2,
max scale-out step to 1, and sample period to 1000 ms. Use a separate test
JVM for each configuration because the invoker and config are cached.

## Run and verify

Provide `PIXELS_S3QS_IT_BUCKET`, `PIXELS_S3QS_IT_PREFIX`,
`PIXELS_S3QS_IT_QUEUE_PREFIX`, `PIXELS_S3QS_SPIKE_IT_COORDINATOR_HOST`, and
`PIXELS_S3QS_SPIKE_IT_COORDINATOR_PORT` in the environment. Never commit
credentials or a populated environment file. Run one scenario at a time:

```bash
PIXELS_S3QS_SPIKE_IT_ENABLED=true PIXELS_S3QS_SPIKE_IT_SCENARIO=baseline \
  mvn -pl pixels-turbo/pixels-worker-spike -am -Pissue1277-spike-it \
  -Dtest=TestS3QSSpikeRemoteEndToEnd \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Supported scenarios are `baseline` (R1), `scaling` (R2), `isolation` (R3),
and `draining` (R4 actuator). R2/R3 use 12 tasks on each shuffle side, two
hash partitions and two join tasks; the reference result is a row multiset,
so legitimate duplicate rows cannot be hidden by set deduplication. R3 runs
two independent queries in one driver JVM. R4 manually reduces one active
stage from two to one Worker while work remains, then verifies completion and
the full result. A manually induced drain does **not** prove automatic
scale-in. Inspect `ISSUE1277_STAGE_SAMPLE`, `s3qs_scaling_decision`,
`s3qs_task_claimed`, `s3qs_task_completed`, and
`ISSUE1277_REMOTE_MANUAL_DRAIN` events together with Surefire reports.

The related local checks are:

```bash
mvn -pl pixels-planner -am -Pissue1277-planner-test \
  -Dtest=TestPendingTaskScalingPolicy,TestStageRuntimeController,TestPartitionedJoinS3QSOperator,TestCoordinatedPlanExecution,TestShuffleInfo \
  -Dsurefire.failIfNoSpecifiedTests=false test
mvn -pl pixels-turbo/pixels-worker-common -am -Pissue1277-worker-test \
  -Dtest=TestS3QSStageWorkerRunner,TestS3QSProducerWiring,TestS3QSConsumerWiring \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

The WorkerCommon consumer class contains one real AWS test and skips it when
the S3/SQS environment variables are absent; inspect its skipped count.

## Safe cleanup

After all in-flight calls finish, delete only the temporary Spike functions,
then confirm their ECS services and RUNNING tasks are gone. Terminate the
test relay, remove its dedicated security groups/key pair, stop the isolated
MySQL/Spike processes, delete the test log group and unique ECR tag, and
verify the exact test S3 prefix and dedicated SQS queues are empty. Preserve
shared queues, roles, repositories, and existing image tags. Record any
residue or failed deletion rather than treating an RPC timeout as proof of
remote termination. Spike's own FIFO wait and cancellation limits remain
outside this success-path test; see `docs/S3QS-SHUFFLE.md` section 22.
