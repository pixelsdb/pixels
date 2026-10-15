package io.pixelsdb.pixels.daemon.transaction.ingest;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.ingest.durable.AtomicStateFile;
import io.pixelsdb.pixels.common.ingest.wire.IngestWire;
import io.pixelsdb.pixels.ingest.IngestProto.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/** Writer identities are independent of engine task IDs and survive coordinator replay. */
public class TestIngestWriterAllocation {
    @TempDir Path directory;
    private final AtomicLong ids = new AtomicLong(100);
    private static final Clock CLOCK =
            Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
    private static final long TASK_ID = 1L << 32;
    private static final long STATEMENT_ID = 1L;
    private static final long FIRST_STATEMENT_ORDINAL = 1L;
    private static final Route ROUTE =
            Route.newBuilder().setShardId(0).setHost("127.0.0.1").setPort(10000).build();
    private static final TableSpec TABLE =
            TableSpec.newBuilder().setTableId(73).setSchemaName("s").setTableName("t")
                    .setSchemaVersion(1).setLayoutId(1).addRoutes(ROUTE)
                    .addColumns(TableColumn.newBuilder().setId(1).setName("v").setType("bigint"))
                    .build();

    private DurableIngestCoordinator open(int limit) throws IOException {
        return new DurableIngestCoordinator(
                new AtomicStateFile(directory, 1024 * 1024),
                new DurableIngestCoordinator.Tables() {
                    public TableSpec load(String schema, String table) { return TABLE; }
                    public List<Route> routes() { return Collections.singletonList(ROUTE); }
                },
                new DurableIngestCoordinator.Participants() {
                    public PrepareToken prepare(String owner, Transaction tx) throws IOException {
                        return PrepareToken.newBuilder().setOwner(owner)
                                .setDigest(ByteString.copyFrom(IngestWire.prepareDigest(tx, owner)))
                                .build();
                    }
                    public boolean install(
                            String owner, Transaction tx, boolean forceFileTail) { return true; }
                    public void discard(String owner, long txId) {}
                },
                ids::incrementAndGet, CLOCK, 0, 60000, 100, limit);
    }

    private Transaction begin(DurableIngestCoordinator coordinator) throws Exception {
        String requestId = UUID.randomUUID().toString();
        return coordinator.begin(BeginWriteRequest.newBuilder().setRequestId(requestId)
                .setSchemaName("s").setTableName("t").setReadTimestamp(0)
                .setStatementId(STATEMENT_ID).setQueryId(requestId + "-query")
                .setStatementOrdinal(FIRST_STATEMENT_ORDINAL)
                .setScope(TransactionScope.AUTOCOMMIT)
                .setRepresentation(WriteRepresentation.BUFFERED)
                .setAckMode(CommitAckMode.VISIBLE).build());
    }

    private static AllocateWriterRequest request(long txId, String requestId) {
        return AllocateWriterRequest.newBuilder().setTransactionId(txId).setTaskId(TASK_ID)
                .setRequestId(requestId).setStatementId(STATEMENT_ID).build();
    }

    private static StreamId stream(long txId, long writerId) {
        return StreamId.newBuilder().setTransactionId(txId).setWriterId(writerId)
                .setTableId(73).setShardId(0).setKind(MutationKind.APPEND_ROWS)
                .setStatementId(STATEMENT_ID).build();
    }

    @Test
    public void parallelSinksInOneTaskHaveIndependentStreamIdentities() throws Exception {
        try (DurableIngestCoordinator coordinator = open(64)) {
            Transaction tx = begin(coordinator);
            WriterAssignment a = coordinator.allocateWriter(request(tx.getTransactionId(), "sink-a"));
            WriterAssignment b = coordinator.allocateWriter(request(tx.getTransactionId(), "sink-b"));
            assertEquals(a.getTaskId(), b.getTaskId());
            assertNotEquals(a.getWriterId(), b.getWriterId());
            coordinator.register(stream(tx.getTransactionId(), a.getWriterId()));
            coordinator.register(stream(tx.getTransactionId(), b.getWriterId()));
            assertEquals(2, coordinator.get(tx.getTransactionId()).getStreamsCount());
        }
    }

    @Test
    public void requestRetransmissionSurvivesRestartWithoutAllocatingAgain() throws Exception {
        AllocateWriterRequest request;
        WriterAssignment original;
        try (DurableIngestCoordinator coordinator = open(64)) {
            request = request(begin(coordinator).getTransactionId(), "stable-request");
            original = coordinator.allocateWriter(request);
            assertEquals(original, coordinator.allocateWriter(request));
        }
        try (DurableIngestCoordinator coordinator = open(64)) {
            assertEquals(original, coordinator.allocateWriter(request));
            WriterAssignment next = coordinator.allocateWriter(request.toBuilder().setRequestId("next").build());
            assertNotEquals(original.getWriterId(), next.getWriterId());
            assertEquals(2, coordinator.get(request.getTransactionId()).getWritersCount());
        }
    }

    @Test
    public void conflictingRequestIdentityIsRejected() throws Exception {
        try (DurableIngestCoordinator coordinator = open(64)) {
            AllocateWriterRequest request = request(begin(coordinator).getTransactionId(), "request");
            coordinator.allocateWriter(request);
            assertThrows(IOException.class,
                    () -> coordinator.allocateWriter(request.toBuilder().setTaskId(TASK_ID + 1).build()));
            assertEquals(1, coordinator.get(request.getTransactionId()).getWritersCount());
        }
    }

    @Test
    public void sealAndAbortFenceNewAllocations() throws Exception {
        try (DurableIngestCoordinator coordinator = open(64)) {
            AllocateWriterRequest request = request(begin(coordinator).getTransactionId(), "request");
            WriterAssignment assignment = coordinator.allocateWriter(request);
            coordinator.completeStatement(CompleteStatementRequest.newBuilder()
                    .setTransactionId(request.getTransactionId())
                    .setStatementId(STATEMENT_ID).build());
            coordinator.prepare(PrepareWriteRequest.newBuilder().setTransactionId(request.getTransactionId()).build());
            assertEquals(assignment, coordinator.allocateWriter(request));
            assertThrows(IOException.class,
                    () -> coordinator.allocateWriter(request.toBuilder().setRequestId("late").build()));
            coordinator.abort(request.getTransactionId());
            assertThrows(IOException.class, () -> coordinator.allocateWriter(request));
        }
    }

    @Test
    public void capacityDoesNotBreakIdempotentRetransmission() throws Exception {
        try (DurableIngestCoordinator coordinator = open(2)) {
            AllocateWriterRequest request = request(begin(coordinator).getTransactionId(), "one");
            WriterAssignment first = coordinator.allocateWriter(request);
            coordinator.allocateWriter(request.toBuilder().setRequestId("two").build());
            assertEquals(first, coordinator.allocateWriter(request));
            assertThrows(IOException.class,
                    () -> coordinator.allocateWriter(request.toBuilder().setRequestId("three").build()));
        }
    }

    @Test
    public void existingStreamIdsAreNotReallocated() throws Exception {
        try (DurableIngestCoordinator coordinator = open(64)) {
            Transaction tx = begin(coordinator);
            coordinator.register(stream(tx.getTransactionId(), 99));
            assertEquals(100, coordinator.allocateWriter(request(tx.getTransactionId(), "new")).getWriterId());
        }
    }

    @Test
    public void concurrentAllocationDoesNotReuseWriterIds() throws Exception {
        try (DurableIngestCoordinator coordinator = open(64)) {
            Transaction tx = begin(coordinator);
            ExecutorService pool = Executors.newFixedThreadPool(8);
            try {
                List<Future<WriterAssignment>> futures = new ArrayList<>();
                for (int i = 0; i < 32; i++) {
                    final String token = "sink-" + i;
                    futures.add(pool.submit(() -> coordinator.allocateWriter(request(tx.getTransactionId(), token))));
                }
                Set<Long> writerIds = new HashSet<>();
                for (Future<WriterAssignment> future : futures) {
                    assertTrue(writerIds.add(future.get(10, TimeUnit.SECONDS).getWriterId()));
                }
                assertEquals(32, coordinator.get(tx.getTransactionId()).getWritersCount());
            } finally {
                pool.shutdownNow();
            }
        }
    }
}
