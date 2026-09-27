/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 */
package io.pixelsdb.pixels.daemon.transaction.ingest;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.ingest.MutationStreamSeal;
import io.pixelsdb.pixels.common.ingest.MutationStreamId;
import io.pixelsdb.pixels.common.ingest.durable.AtomicStateFile;
import io.pixelsdb.pixels.common.ingest.wire.IngestWire;
import io.pixelsdb.pixels.ingest.IngestProto.*;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestExplicitIngestTransaction
{
    private static final int STATE_FILE_CAPACITY_BYTES = 1024 * 1024;
    private static final long TRANSACTION_LEASE_MILLIS = 60_000L;
    private static final int MAX_TRANSACTIONS = 100;
    private static final int MAX_STREAMS = 64;
    private static final long TABLE_A_ID = 73L;
    private static final long TABLE_B_ID = 74L;
    private static final long FIRST_STATEMENT_ID = 11L;
    private static final long SECOND_STATEMENT_ID = 12L;
    private static final long FIRST_STATEMENT_ORDINAL = 1L;
    private static final long SECOND_STATEMENT_ORDINAL = 2L;
    private static final long EMPTY_PRIVATE_READ_FRONTIER = 0L;
    private static final long FIRST_WRITER_ID = 21L;
    private static final long SECOND_WRITER_ID = 22L;
    private static final long FIRST_BATCH_COUNT = 1L;
    private static final long FIRST_ROW_COUNT = 2L;
    private static final long FIRST_PAYLOAD_BYTES = 16L;
    private static final long SECOND_ROW_COUNT = 3L;
    private static final long SECOND_PAYLOAD_BYTES = 24L;
    private static final int FIRST_SHARD_ID = 0;
    private static final int SECOND_SHARD_ID = 1;
    private static final int FIRST_PARTICIPANT_PORT = 10_000;
    private static final int SECOND_PARTICIPANT_PORT = 10_001;
    private static final int EXPECTED_STATEMENT_COUNT = 2;
    private static final int EXPECTED_TABLE_COUNT = 2;
    private static final int EXPECTED_PARTICIPANT_COUNT = 2;
    private static final int BOUNDARY_TRANSACTION_COUNT = 2;
    private static final long VISIBILITY_WAIT_MILLIS = 1_000L;
    private static final long INSTALLATION_TEST_TIMEOUT_SECONDS = 5L;
    private static final String SCHEMA_NAME = "s";
    private static final String TABLE_A_NAME = "a";
    private static final String TABLE_B_NAME = "b";
    private static final String LOOPBACK_HOST = "127.0.0.1";
    private static final Clock CLOCK =
            Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
    private static final Route FIRST_ROUTE = Route.newBuilder()
            .setShardId(FIRST_SHARD_ID).setHost(LOOPBACK_HOST)
            .setPort(FIRST_PARTICIPANT_PORT).build();
    private static final Route SECOND_ROUTE = Route.newBuilder()
            .setShardId(SECOND_SHARD_ID).setHost(LOOPBACK_HOST)
            .setPort(SECOND_PARTICIPANT_PORT).build();
    private static final List<Route> ROUTES = Arrays.asList(FIRST_ROUTE, SECOND_ROUTE);
    private static final TableSpec TABLE_A = table(TABLE_A_ID, TABLE_A_NAME);
    private static final TableSpec TABLE_B = table(TABLE_B_ID, TABLE_B_NAME);

    @TempDir Path directory;
    private final AtomicLong identities = new AtomicLong(100L);
    private final AtomicInteger prepares = new AtomicInteger();
    private final AtomicInteger installs = new AtomicInteger();

    @Test
    public void explicitTransactionSurvivesRestartAndPublishesAllParticipants() throws Exception
    {
        long transactionId;
        try (DurableIngestCoordinator coordinator = open())
        {
            Transaction transaction = coordinator.begin(begin("explicit-request"));
            transactionId = transaction.getTransactionId();
            complete(coordinator, transactionId, FIRST_STATEMENT_ID, TABLE_A_ID,
                    FIRST_WRITER_ID, FIRST_SHARD_ID, FIRST_ROW_COUNT, FIRST_PAYLOAD_BYTES);

            transaction = coordinator.beginStatement(BeginStatementRequest.newBuilder()
                    .setTransactionId(transactionId)
                    .setStatementId(SECOND_STATEMENT_ID)
                    .setQueryId("query-b")
                    .setOrdinal(SECOND_STATEMENT_ORDINAL)
                    .setReadOwnThroughOrdinal(FIRST_STATEMENT_ORDINAL)
                    .setSchemaName(SCHEMA_NAME)
                    .setTableName(TABLE_B_NAME)
                    .setWriteTable(true)
                    .build());
            assertEquals(EXPECTED_STATEMENT_COUNT, transaction.getStatementsCount());
            assertEquals(EXPECTED_TABLE_COUNT, transaction.getEnlistedTablesCount());
            complete(coordinator, transactionId, SECOND_STATEMENT_ID, TABLE_B_ID,
                    SECOND_WRITER_ID, SECOND_SHARD_ID, SECOND_ROW_COUNT, SECOND_PAYLOAD_BYTES);
        }

        try (DurableIngestCoordinator coordinator = open())
        {
            Transaction recovered = coordinator.get(transactionId);
            assertEquals(TransactionScope.EXPLICIT, recovered.getScope());
            assertEquals(WriteRepresentation.BUFFERED, recovered.getRepresentation());
            assertEquals(CommitAckMode.VISIBLE, recovered.getAckMode());
            assertEquals(EXPECTED_STATEMENT_COUNT, recovered.getStatementsCount());
            assertEquals(EXPECTED_TABLE_COUNT, recovered.getEnlistedTablesCount());

            Transaction prepared = coordinator.prepare(PrepareWriteRequest.newBuilder()
                    .setTransactionId(transactionId).build());
            assertEquals(TransactionState.PREPARED, prepared.getState());
            Transaction published = coordinator.commit(transactionId);
            assertEquals(TransactionState.PUBLISHED, published.getState());
            assertEquals(DecisionOutcome.COMMIT, published.getOutcome());
            assertEquals(PublicationProgress.VISIBLE_NOW, published.getProgress());
            assertFalse(published.getCommitToken().isEmpty());
            assertEquals(EXPECTED_PARTICIPANT_COUNT, prepares.get());
            assertEquals(EXPECTED_PARTICIPANT_COUNT, installs.get());
        }
    }

    @Test
    public void overlappingStatementAndCommitAfterRollbackAreRejected() throws Exception
    {
        try (DurableIngestCoordinator coordinator = open())
        {
            Transaction transaction = coordinator.begin(begin("rollback-request"));
            BeginStatementRequest overlapping = BeginStatementRequest.newBuilder()
                    .setTransactionId(transaction.getTransactionId())
                    .setStatementId(SECOND_STATEMENT_ID)
                    .setQueryId("overlap")
                    .setOrdinal(SECOND_STATEMENT_ORDINAL)
                    .setReadOwnThroughOrdinal(FIRST_STATEMENT_ORDINAL)
                    .setSchemaName(SCHEMA_NAME)
                    .setTableName(TABLE_B_NAME)
                    .build();
            assertThrows(IOException.class, () -> coordinator.beginStatement(overlapping));

            coordinator.completeStatement(CompleteStatementRequest.newBuilder()
                    .setTransactionId(transaction.getTransactionId())
                    .setStatementId(FIRST_STATEMENT_ID)
                    .build());
            coordinator.beginStatement(overlapping);
            Transaction aborted = coordinator.abort(transaction.getTransactionId());
            assertEquals(TransactionState.ABORTED, aborted.getState());
            assertEquals(DecisionOutcome.ABORT, aborted.getOutcome());
            assertTrue(aborted.getRollbackOnly());
            assertThrows(IOException.class, () -> coordinator.commit(transaction.getTransactionId()));
        }
    }

    @Test
    public void oneStatementCanReadOneTableAndWriteAnother() throws Exception
    {
        try (DurableIngestCoordinator coordinator = open())
        {
            Transaction transaction = coordinator.begin(begin("insert-select-request"));
            coordinator.completeStatement(CompleteStatementRequest.newBuilder()
                    .setTransactionId(transaction.getTransactionId())
                    .setStatementId(FIRST_STATEMENT_ID)
                    .build());

            BeginStatementRequest readSource = BeginStatementRequest.newBuilder()
                    .setTransactionId(transaction.getTransactionId())
                    .setStatementId(SECOND_STATEMENT_ID)
                    .setQueryId("insert-select")
                    .setOrdinal(SECOND_STATEMENT_ORDINAL)
                    .setReadOwnThroughOrdinal(FIRST_STATEMENT_ORDINAL)
                    .setSchemaName(SCHEMA_NAME)
                    .setTableName(TABLE_A_NAME)
                    .setWriteTable(false)
                    .build();
            transaction = coordinator.beginStatement(readSource);

            BeginStatementRequest writeTarget = readSource.toBuilder()
                    .setTableName(TABLE_B_NAME)
                    .setWriteTable(true)
                    .build();
            transaction = coordinator.beginStatement(writeTarget);

            StatementManifest statement = transaction.getStatementsList().stream()
                    .filter(value -> value.getStatementId() == SECOND_STATEMENT_ID)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Missing INSERT SELECT statement"));
            assertEquals(TABLE_B_ID, statement.getTableId());
            assertEquals(Arrays.asList(TABLE_A_ID, TABLE_B_ID), statement.getReadTableIdsList());
            assertEquals(EXPECTED_TABLE_COUNT, transaction.getEnlistedTablesCount());

            complete(coordinator, transaction.getTransactionId(), SECOND_STATEMENT_ID, TABLE_B_ID,
                    SECOND_WRITER_ID, SECOND_SHARD_ID, SECOND_ROW_COUNT, SECOND_PAYLOAD_BYTES);
            Transaction published = coordinator.commit(coordinator.prepare(
                    PrepareWriteRequest.newBuilder()
                            .setTransactionId(transaction.getTransactionId())
                            .build()).getTransactionId());
            assertEquals(TransactionState.PUBLISHED, published.getState());
        }
    }

    @Test
    public void corruptedStatementFrontierFailsClosedOnRestart() throws Exception
    {
        try (DurableIngestCoordinator coordinator = open())
        {
            coordinator.begin(begin("corrupt-frontier-request"));
        }
        rewriteSnapshot(snapshot -> snapshot.toBuilder()
                .setTransactions(0, snapshot.getTransactions(0).toBuilder()
                        .setStatements(0, snapshot.getTransactions(0).getStatements(0).toBuilder()
                                .setReadOwnThroughOrdinal(FIRST_STATEMENT_ORDINAL)))
                .build());

        IOException failure = assertThrows(IOException.class, () -> {
            try (DurableIngestCoordinator ignored = open()) {}
        });
        assertTrue(failure.getMessage().contains("statement"));
    }

    @Test
    public void missingRecoverySealFailsClosedOnRestart() throws Exception
    {
        try (DurableIngestCoordinator coordinator = open())
        {
            Transaction transaction = coordinator.begin(begin("missing-seal-request"));
            complete(coordinator, transaction.getTransactionId(), FIRST_STATEMENT_ID, TABLE_A_ID,
                    FIRST_WRITER_ID, FIRST_SHARD_ID, FIRST_ROW_COUNT, FIRST_PAYLOAD_BYTES);
        }
        rewriteSnapshot(snapshot -> snapshot.toBuilder()
                .setTransactions(0, snapshot.getTransactions(0).toBuilder().clearSeals())
                .build());

        IOException failure = assertThrows(IOException.class, () -> {
            try (DurableIngestCoordinator ignored = open()) {}
        });
        assertTrue(failure.getMessage().contains("seals"));
    }

    @Test
    public void durableCommitReturnsBeforeInstallAndPublishesAfterRestart() throws Exception
    {
        long transactionId;
        String commitToken;
        try (DurableIngestCoordinator coordinator = open())
        {
            Transaction transaction = coordinator.begin(
                    begin("durable-request", CommitAckMode.DURABLE));
            transactionId = transaction.getTransactionId();
            complete(coordinator, transactionId, FIRST_STATEMENT_ID, TABLE_A_ID,
                    FIRST_WRITER_ID, FIRST_SHARD_ID, FIRST_ROW_COUNT, FIRST_PAYLOAD_BYTES);
            coordinator.prepare(PrepareWriteRequest.newBuilder()
                    .setTransactionId(transactionId).build());

            Transaction decided = coordinator.commit(transactionId);
            assertEquals(TransactionState.COMMIT_DECIDED, decided.getState());
            assertEquals(DecisionOutcome.COMMIT, decided.getOutcome());
            assertEquals(PublicationProgress.INSTALLING, decided.getProgress());
            assertEquals(0, installs.get());
            commitToken = decided.getCommitToken();
            assertFalse(commitToken.isEmpty());
        }

        try (DurableIngestCoordinator coordinator = open())
        {
            Transaction recovered = coordinator.get(transactionId);
            assertEquals(TransactionState.COMMIT_DECIDED, recovered.getState());
            assertEquals(commitToken, recovered.getCommitToken());
            Transaction visible = coordinator.awaitVisible(VisibilityRequest.newBuilder()
                    .setTransactionId(transactionId)
                    .setCommitToken(commitToken)
                    .setDeadlineMillis(Math.addExact(CLOCK.millis(), VISIBILITY_WAIT_MILLIS))
                    .build());
            assertEquals(TransactionState.PUBLISHED, visible.getState());
            assertEquals(PublicationProgress.VISIBLE_NOW, visible.getProgress());
            assertEquals(1, installs.get());
        }
    }

    @Test
    public void laterCommitInstallsWhilePublicationPrefixIsBlocked() throws Exception
    {
        CountDownLatch firstInstallationStarted = new CountDownLatch(1);
        CountDownLatch secondInstallationStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstInstallation = new CountDownLatch(1);
        AtomicLong firstTransaction = new AtomicLong();
        DurableIngestCoordinator.Participants participants =
                new DurableIngestCoordinator.Participants()
                {
                    public PrepareToken prepare(String owner, Transaction transaction)
                            throws IOException
                    {
                        return PrepareToken.newBuilder()
                                .setOwner(owner)
                                .setDigest(ByteString.copyFrom(
                                        IngestWire.prepareDigest(transaction, owner)))
                                .build();
                    }

                    public boolean install(
                            String owner, Transaction transaction, boolean forceFileTail)
                            throws Exception
                    {
                        if (transaction.getTransactionId() == firstTransaction.get())
                        {
                            firstInstallationStarted.countDown();
                            if (!releaseFirstInstallation.await(
                                    INSTALLATION_TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                            {
                                throw new IOException("Timed out releasing first installation");
                            }
                        }
                        else
                        {
                            secondInstallationStarted.countDown();
                        }
                        return true;
                    }

                    public void discard(String owner, long transactionId) {}
                };

        try (DurableIngestCoordinator coordinator = open(participants))
        {
            Transaction first = durableTransaction(
                    coordinator, "install-first", FIRST_WRITER_ID, FIRST_ROW_COUNT);
            firstTransaction.set(first.getTransactionId());
            Transaction second = durableTransaction(
                    coordinator, "install-second", SECOND_WRITER_ID, SECOND_ROW_COUNT);
            ExecutorService publisher = Executors.newSingleThreadExecutor();
            try
            {
                Future<?> publication = publisher.submit(() -> {
                    try
                    {
                        coordinator.drivePublication();
                    }
                    catch (Exception e)
                    {
                        throw new RuntimeException(e);
                    }
                });
                assertTrue(firstInstallationStarted.await(
                        INSTALLATION_TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
                assertTrue(secondInstallationStarted.await(
                        INSTALLATION_TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
                assertEquals(EMPTY_PRIVATE_READ_FRONTIER, coordinator.publishedTimestamp());
                releaseFirstInstallation.countDown();
                publication.get(INSTALLATION_TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                assertEquals(TransactionState.PUBLISHED,
                        coordinator.get(first.getTransactionId()).getState());
                assertEquals(TransactionState.PUBLISHED,
                        coordinator.get(second.getTransactionId()).getState());
                assertEquals(second.getCommitTimestamp(), coordinator.publishedTimestamp());
            }
            finally
            {
                releaseFirstInstallation.countDown();
                publisher.shutdownNow();
            }
        }
    }

    @Test
    public void visibilityBarrierForcesOnlyItsFileTailBoundary() throws Exception
    {
        AtomicBoolean forced = new AtomicBoolean();
        DurableIngestCoordinator.Participants participants =
                new DurableIngestCoordinator.Participants()
                {
                    public PrepareToken prepare(String owner, Transaction transaction)
                            throws IOException
                    {
                        return PrepareToken.newBuilder()
                                .setOwner(owner)
                                .setDigest(ByteString.copyFrom(
                                        IngestWire.prepareDigest(transaction, owner)))
                                .build();
                    }

                    public boolean install(
                            String owner, Transaction transaction, boolean forceFileTail)
                    {
                        forced.compareAndSet(false, forceFileTail);
                        return forceFileTail;
                    }

                    public void discard(String owner, long transactionId) {}
                };

        try (DurableIngestCoordinator coordinator = open(participants))
        {
            Transaction transaction = coordinator.begin(
                    begin("file-barrier", CommitAckMode.DURABLE).toBuilder()
                            .setRepresentation(WriteRepresentation.FILE)
                            .build());
            complete(coordinator, transaction.getTransactionId(), FIRST_STATEMENT_ID, TABLE_A_ID,
                    FIRST_WRITER_ID, FIRST_SHARD_ID, FIRST_ROW_COUNT, FIRST_PAYLOAD_BYTES);
            coordinator.prepare(PrepareWriteRequest.newBuilder()
                    .setTransactionId(transaction.getTransactionId()).build());
            Transaction decided = coordinator.commit(transaction.getTransactionId());
            assertEquals(TransactionState.COMMIT_DECIDED, decided.getState());

            VisibleBarrier barrier = coordinator.flushVisibleBarrier(
                    VisibleBarrierRequest.newBuilder()
                            .setDeadlineMillis(Math.addExact(
                                    CLOCK.millis(), VISIBILITY_WAIT_MILLIS))
                            .build());
            assertTrue(forced.get());
            assertEquals(decided.getCommitTimestamp(), barrier.getCommittedBoundary());
            assertEquals(TransactionState.PUBLISHED,
                    coordinator.get(transaction.getTransactionId()).getState());
        }
    }

    @Test
    public void visibilityBarrierWaitsForEveryBoundaryContribution() throws Exception
    {
        Set<Long> contributed = ConcurrentHashMap.newKeySet();
        AtomicBoolean forcedBeforeBoundaryContribution = new AtomicBoolean();
        DurableIngestCoordinator.Participants participants =
                new DurableIngestCoordinator.Participants()
                {
                    public PrepareToken prepare(String owner, Transaction transaction)
                            throws IOException
                    {
                        return PrepareToken.newBuilder()
                                .setOwner(owner)
                                .setDigest(ByteString.copyFrom(
                                        IngestWire.prepareDigest(transaction, owner)))
                                .build();
                    }

                    public boolean install(
                            String owner, Transaction transaction, boolean forceFileTail)
                    {
                        if (forceFileTail
                                && contributed.size() < BOUNDARY_TRANSACTION_COUNT)
                        {
                            forcedBeforeBoundaryContribution.set(true);
                        }
                        contributed.add(transaction.getTransactionId());
                        return forceFileTail;
                    }

                    public void discard(String owner, long transactionId) {}
                };

        try (DurableIngestCoordinator coordinator = open(participants))
        {
            Transaction first = durableTransaction(
                    coordinator, "file-boundary-first", FIRST_WRITER_ID, FIRST_ROW_COUNT,
                    WriteRepresentation.FILE);
            Transaction second = durableTransaction(
                    coordinator, "file-boundary-second", SECOND_WRITER_ID, SECOND_ROW_COUNT,
                    WriteRepresentation.FILE);

            VisibleBarrier barrier = coordinator.flushVisibleBarrier(
                    VisibleBarrierRequest.newBuilder()
                            .setDeadlineMillis(Math.addExact(
                                    CLOCK.millis(), VISIBILITY_WAIT_MILLIS))
                            .build());

            assertFalse(forcedBeforeBoundaryContribution.get());
            assertEquals(BOUNDARY_TRANSACTION_COUNT, contributed.size());
            assertEquals(second.getCommitTimestamp(), barrier.getCommittedBoundary());
            assertEquals(TransactionState.PUBLISHED,
                    coordinator.get(first.getTransactionId()).getState());
            assertEquals(TransactionState.PUBLISHED,
                    coordinator.get(second.getTransactionId()).getState());
        }
    }

    private Transaction durableTransaction(
            DurableIngestCoordinator coordinator,
            String requestId,
            long writerId,
            long rowCount)
            throws Exception
    {
        return durableTransaction(
                coordinator, requestId, writerId, rowCount, WriteRepresentation.BUFFERED);
    }

    private Transaction durableTransaction(
            DurableIngestCoordinator coordinator,
            String requestId,
            long writerId,
            long rowCount,
            WriteRepresentation representation)
            throws Exception
    {
        Transaction transaction = coordinator.begin(
                begin(requestId, CommitAckMode.DURABLE).toBuilder()
                        .setRepresentation(representation)
                        .build());
        complete(coordinator, transaction.getTransactionId(), FIRST_STATEMENT_ID, TABLE_A_ID,
                writerId, FIRST_SHARD_ID, rowCount, FIRST_PAYLOAD_BYTES);
        coordinator.prepare(PrepareWriteRequest.newBuilder()
                .setTransactionId(transaction.getTransactionId()).build());
        return coordinator.commit(transaction.getTransactionId());
    }

    private void rewriteSnapshot(UnaryOperator<CoordinatorSnapshot> mutation) throws IOException
    {
        try (AtomicStateFile state = new AtomicStateFile(directory, STATE_FILE_CAPACITY_BYTES))
        {
            CoordinatorSnapshot snapshot = CoordinatorSnapshot.parseFrom(state.read());
            state.store(mutation.apply(snapshot).toByteArray());
        }
    }

    private DurableIngestCoordinator open() throws IOException
    {
        return open(defaultParticipants());
    }

    private DurableIngestCoordinator.Participants defaultParticipants()
    {
        return new DurableIngestCoordinator.Participants()
        {
            public PrepareToken prepare(String owner, Transaction transaction)
                    throws IOException
            {
                prepares.incrementAndGet();
                return PrepareToken.newBuilder()
                        .setOwner(owner)
                        .setDigest(ByteString.copyFrom(
                                IngestWire.prepareDigest(transaction, owner)))
                        .build();
            }

            public boolean install(
                    String owner, Transaction transaction, boolean forceFileTail)
            {
                installs.incrementAndGet();
                return true;
            }

            public void discard(String owner, long transactionId) {}
        };
    }

    private DurableIngestCoordinator open(DurableIngestCoordinator.Participants participants)
            throws IOException
    {
        return new DurableIngestCoordinator(
                new AtomicStateFile(directory, STATE_FILE_CAPACITY_BYTES),
                new DurableIngestCoordinator.Tables()
                {
                    public TableSpec load(String schema, String table) throws IOException
                    {
                        if (!SCHEMA_NAME.equals(schema)) {
                            throw new IOException("Unknown schema");
                        }
                        if (TABLE_A_NAME.equals(table)) {
                            return TABLE_A;
                        }
                        if (TABLE_B_NAME.equals(table)) {
                            return TABLE_B;
                        }
                        throw new IOException("Unknown table");
                    }

                    public List<Route> routes()
                    {
                        return ROUTES;
                    }
                },
                participants,
                identities::incrementAndGet,
                CLOCK,
                EMPTY_PRIVATE_READ_FRONTIER,
                TRANSACTION_LEASE_MILLIS,
                MAX_TRANSACTIONS,
                MAX_STREAMS);
    }

    private static BeginWriteRequest begin(String requestId)
    {
        return begin(requestId, CommitAckMode.VISIBLE);
    }

    private static BeginWriteRequest begin(String requestId, CommitAckMode ackMode)
    {
        return BeginWriteRequest.newBuilder()
                .setRequestId(requestId)
                .setSchemaName(SCHEMA_NAME)
                .setTableName(TABLE_A_NAME)
                .setReadTimestamp(EMPTY_PRIVATE_READ_FRONTIER)
                .setStatementId(FIRST_STATEMENT_ID)
                .setQueryId("query-a")
                .setStatementOrdinal(FIRST_STATEMENT_ORDINAL)
                .setScope(TransactionScope.EXPLICIT)
                .setRepresentation(WriteRepresentation.BUFFERED)
                .setAckMode(ackMode)
                .build();
    }

    private static void complete(
            DurableIngestCoordinator coordinator,
            long transactionId,
            long statementId,
            long tableId,
            long writerId,
            int shardId,
            long rowCount,
            long payloadBytes)
            throws Exception
    {
        MutationStreamId stream = new MutationStreamId(transactionId, statementId,
                writerId, tableId, shardId, MutationStreamId.Kind.APPEND_ROWS);
        coordinator.register(IngestWire.encode(stream));
        MutationStreamSeal seal = new MutationStreamSeal(stream, FIRST_BATCH_COUNT,
                rowCount, payloadBytes, MutationStreamSeal.emptyDigest());
        coordinator.completeStatement(CompleteStatementRequest.newBuilder()
                .setTransactionId(transactionId)
                .setStatementId(statementId)
                .addSeals(IngestWire.encode(seal))
                .build());
    }

    private static TableSpec table(long tableId, String tableName)
    {
        return TableSpec.newBuilder()
                .setTableId(tableId)
                .setSchemaName(SCHEMA_NAME)
                .setTableName(tableName)
                .setSchemaVersion(FIRST_STATEMENT_ORDINAL)
                .setLayoutId(FIRST_STATEMENT_ORDINAL)
                .addAllRoutes(ROUTES)
                .addColumns(TableColumn.newBuilder()
                        .setId(FIRST_STATEMENT_ORDINAL)
                        .setName("v")
                        .setType("bigint"))
                .build();
    }
}
