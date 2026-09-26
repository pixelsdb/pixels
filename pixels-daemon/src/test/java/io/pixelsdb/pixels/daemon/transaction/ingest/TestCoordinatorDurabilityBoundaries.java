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

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.ingest.MutationStreamId;
import io.pixelsdb.pixels.common.ingest.MutationStreamSeal;
import io.pixelsdb.pixels.common.ingest.wire.IngestWire;
import io.pixelsdb.pixels.ingest.IngestProto.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static io.pixelsdb.pixels.daemon.transaction.ingest.DurableIngestCoordinator.StateStore.Durability.DEFERRED;
import static io.pixelsdb.pixels.daemon.transaction.ingest.DurableIngestCoordinator.StateStore.Durability.SYNCHRONIZED;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCoordinatorDurabilityBoundaries
{
    private static final long STATEMENT_ID = 1L;
    private static final long TABLE_ID = 73L;
    private static final int SHARD_ID = 0;
    private static final int RETINA_PORT = 10_000;
    private static final int GROUP_COMMIT_TRANSACTIONS = 8;
    private static final long GROUP_COMMIT_DELAY_MICROS = 50_000L;
    private static final Route ROUTE = Route.newBuilder()
            .setShardId(SHARD_ID).setHost("127.0.0.1").setPort(RETINA_PORT).build();
    private static final TableSpec TABLE = TableSpec.newBuilder()
            .setTableId(TABLE_ID).setSchemaName("s").setTableName("t")
            .setSchemaVersion(1L).setLayoutId(1L).addRoutes(ROUTE)
            .addColumns(TableColumn.newBuilder().setId(1L).setName("v").setType("bigint"))
            .build();

    @Test
    public void replayableTransitionsShareTheNextDurabilityBarrier() throws Exception
    {
        RecordingStore store = new RecordingStore();
        AtomicLong identities = new AtomicLong(100L);
        try (DurableIngestCoordinator coordinator = coordinator(store, identities, 0L))
        {
            Transaction transaction = coordinator.begin(BeginWriteRequest.newBuilder()
                    .setRequestId("request")
                    .setSchemaName("s").setTableName("t").setReadTimestamp(0L)
                    .setStatementId(STATEMENT_ID).setQueryId("query").setStatementOrdinal(1L)
                    .setScope(TransactionScope.AUTOCOMMIT)
                    .setRepresentation(WriteRepresentation.FILE)
                    .setAckMode(CommitAckMode.DURABLE)
                    .build());
            WriterAssignment writer = coordinator.allocateWriter(
                    AllocateWriterRequest.newBuilder()
                            .setTransactionId(transaction.getTransactionId())
                            .setStatementId(STATEMENT_ID)
                            .setTaskId(1L)
                            .setRequestId("writer")
                            .build());
            store.clearDurabilities();

            MutationStreamId stream = new MutationStreamId(
                    transaction.getTransactionId(), STATEMENT_ID, writer.getWriterId(),
                    TABLE_ID, SHARD_ID, MutationStreamId.Kind.APPEND_ROWS);
            coordinator.register(IngestWire.encode(stream));
            assertEquals(Collections.singletonList(DEFERRED), store.durabilities());

            store.clearDurabilities();
            MutationStreamSeal seal = new MutationStreamSeal(
                    stream, 1L, 1L, 1L, MutationStreamSeal.emptyDigest());
            coordinator.completeStatement(CompleteStatementRequest.newBuilder()
                    .setTransactionId(transaction.getTransactionId())
                    .setStatementId(STATEMENT_ID)
                    .addSeals(IngestWire.encode(seal))
                    .build());
            assertEquals(java.util.Arrays.asList(DEFERRED, SYNCHRONIZED),
                    store.durabilities());

            store.clearDurabilities();
            coordinator.prepare(PrepareWriteRequest.newBuilder()
                    .setTransactionId(transaction.getTransactionId())
                    .build());
            assertEquals(java.util.Arrays.asList(DEFERRED, DEFERRED), store.durabilities());

            store.clearDurabilities();
            coordinator.commit(transaction.getTransactionId());
            assertEquals(java.util.Arrays.asList(DEFERRED, SYNCHRONIZED),
                    store.durabilities());
        }
    }

    @Test
    public void concurrentCommitsShareOneStateSynchronization() throws Exception
    {
        RecordingStore store = new RecordingStore();
        AtomicLong identities = new AtomicLong(1_000L);
        try (DurableIngestCoordinator coordinator =
                coordinator(store, identities, GROUP_COMMIT_DELAY_MICROS))
        {
            List<Long> transactionIds = new ArrayList<>();
            for (int index = 0; index < GROUP_COMMIT_TRANSACTIONS; index++)
            {
                Transaction transaction = coordinator.begin(BeginWriteRequest.newBuilder()
                        .setRequestId("request-" + index)
                        .setSchemaName("s").setTableName("t").setReadTimestamp(0L)
                        .setStatementId(STATEMENT_ID).setQueryId("query-" + index)
                        .setStatementOrdinal(1L).setScope(TransactionScope.AUTOCOMMIT)
                        .setRepresentation(WriteRepresentation.FILE)
                        .setAckMode(CommitAckMode.DURABLE)
                        .build());
                coordinator.completeStatement(CompleteStatementRequest.newBuilder()
                        .setTransactionId(transaction.getTransactionId())
                        .setStatementId(STATEMENT_ID)
                        .build());
                coordinator.prepare(PrepareWriteRequest.newBuilder()
                        .setTransactionId(transaction.getTransactionId())
                        .build());
                transactionIds.add(transaction.getTransactionId());
            }
            store.clearDurabilities();

            CountDownLatch ready = new CountDownLatch(GROUP_COMMIT_TRANSACTIONS);
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(GROUP_COMMIT_TRANSACTIONS);
            List<Future<?>> commits = new ArrayList<>();
            try
            {
                for (long transactionId : transactionIds)
                {
                    commits.add(executor.submit(() -> {
                        ready.countDown();
                        start.await();
                        coordinator.commit(transactionId);
                        return null;
                    }));
                }
                ready.await();
                start.countDown();
                for (Future<?> commit : commits)
                {
                    commit.get();
                }
            }
            finally
            {
                executor.shutdownNow();
            }

            assertEquals(GROUP_COMMIT_TRANSACTIONS,
                    store.durabilities().stream().filter(DEFERRED::equals).count());
            assertEquals(1L,
                    store.durabilities().stream().filter(SYNCHRONIZED::equals).count());
        }
    }

    private static DurableIngestCoordinator coordinator(
            RecordingStore store, AtomicLong identities, long groupCommitDelayMicros)
            throws IOException
    {
        return new DurableIngestCoordinator(
                store,
                new DurableIngestCoordinator.Tables()
                {
                    public TableSpec load(String schema, String table) { return TABLE; }
                    public List<Route> routes() { return Collections.singletonList(ROUTE); }
                },
                new DurableIngestCoordinator.Participants()
                {
                    public PrepareToken prepare(String owner, Transaction transaction)
                            throws IOException
                    {
                        return PrepareToken.newBuilder().setOwner(owner)
                                .setDigest(ByteString.copyFrom(
                                        IngestWire.prepareDigest(transaction, owner)))
                                .build();
                    }
                    public boolean install(
                            String owner, Transaction transaction, boolean forceFileTail)
                    {
                        return true;
                    }
                    public void discard(String owner, long transactionId) {}
                },
                identities::incrementAndGet, Clock.systemUTC(), 0L, 60_000L,
                100, 64, 120_000L, 100, 1, groupCommitDelayMicros);
    }

    private static final class RecordingStore implements DurableIngestCoordinator.StateStore
    {
        private CoordinatorSnapshot snapshot;
        private final List<Durability> durabilities = new ArrayList<>();

        @Override
        public CoordinatorSnapshot read()
        {
            return snapshot;
        }

        @Override
        public void store(CoordinatorSnapshot value, Durability durability)
        {
            snapshot = value;
            durabilities.add(durability);
        }

        @Override
        public void synchronize()
        {
            durabilities.add(SYNCHRONIZED);
        }

        private void clearDurabilities()
        {
            durabilities.clear();
        }

        private List<Durability> durabilities()
        {
            return new ArrayList<>(durabilities);
        }

        @Override
        public void close() {}
    }
}
