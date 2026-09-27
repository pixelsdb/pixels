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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;

public class TestDurableIngestCoordinatorGc
{
    private static final long STATEMENT_ID = 1L;
    private static final long FIRST_STATEMENT_ORDINAL = 1L;

    @TempDir Path directory;
    private final AtomicLong ids = new AtomicLong(100);
    private final AtomicInteger checkpoints = new AtomicInteger();
    private static final Clock CLOCK =
            Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
    private static final String OWNER = "127.0.0.1:10000";
    private static final Route ROUTE = Route.newBuilder()
            .setShardId(0).setHost("127.0.0.1").setPort(10000).build();
    private static final TableSpec TABLE = TableSpec.newBuilder()
            .setTableId(73).setSchemaName("s").setTableName("t")
            .setSchemaVersion(1).setLayoutId(1).addRoutes(ROUTE)
            .addColumns(TableColumn.newBuilder().setId(1).setName("v").setType("bigint"))
            .build();

    @Test
    public void testPublishedCheckpointBecomesCompactFenceAndSurvivesRestart() throws Exception
    {
        long transactionId;
        BeginWriteRequest begin = beginRequest("stable-request");
        try (DurableIngestCoordinator coordinator = open())
        {
            Transaction tx = coordinator.begin(begin);
            transactionId = tx.getTransactionId();
            MutationStreamId local = new MutationStreamId(
                    transactionId, STATEMENT_ID, 1, 73, 0, MutationStreamId.Kind.APPEND_ROWS);
            coordinator.register(IngestWire.encode(local));
            MutationStreamSeal receipt = new MutationStreamSeal(
                    local, 1, 1, 1, MutationStreamSeal.emptyDigest());
            coordinator.completeStatement(CompleteStatementRequest.newBuilder()
                    .setTransactionId(transactionId).setStatementId(STATEMENT_ID)
                    .addSeals(IngestWire.encode(receipt)).build());
            coordinator.prepare(PrepareWriteRequest.newBuilder()
                    .setTransactionId(transactionId).build());
            assertEquals(TransactionState.PUBLISHED,
                    coordinator.commit(transactionId).getState());

            coordinator.reconcileOnce();

            Transaction fence = coordinator.get(transactionId);
            assertEquals(TransactionState.PUBLISHED, fence.getState());
            assertEquals(0, fence.getStreamsCount());
            assertEquals(0, fence.getSealsCount());
            assertEquals(0, fence.getTokensCount());
            assertEquals(transactionId, coordinator.begin(begin).getTransactionId());
            assertEquals(1, checkpoints.get());
            assertTrue(coordinator.list(OWNER).getTransactionsList().isEmpty());
        }

        try (DurableIngestCoordinator reopened = open())
        {
            assertEquals(TransactionState.PUBLISHED, reopened.get(transactionId).getState());
            assertEquals(transactionId, reopened.begin(begin).getTransactionId());
            assertTrue(reopened.list(OWNER).getTransactionsList().isEmpty());
        }
    }

    @Test
    public void testExpiredFenceIsPrunedWithoutForgettingRetiredTransactionId() throws Exception
    {
        long transactionId;
        BeginWriteRequest begin = beginRequest("expired-request");
        try (DurableIngestCoordinator coordinator = open(CLOCK))
        {
            transactionId = coordinator.begin(begin).getTransactionId();
            coordinator.abort(transactionId);
            coordinator.reconcileOnce();
            assertEquals(TransactionState.ABORTED, coordinator.get(transactionId).getState());
        }

        Clock later = Clock.offset(CLOCK, java.time.Duration.ofMillis(120_001));
        try (DurableIngestCoordinator reopened = open(later))
        {
            reopened.reconcileOnce();
            IOException failure = assertThrows(IOException.class, () -> reopened.get(transactionId));
            assertTrue(failure.getMessage().contains("Retired ingest transaction"));
        }
    }

    private DurableIngestCoordinator open() throws IOException
    {
        return open(CLOCK);
    }

    private static BeginWriteRequest beginRequest(String requestId)
    {
        return BeginWriteRequest.newBuilder()
                .setRequestId(requestId)
                .setSchemaName("s")
                .setTableName("t")
                .setReadTimestamp(0)
                .setStatementId(STATEMENT_ID)
                .setQueryId(requestId + "-query")
                .setStatementOrdinal(FIRST_STATEMENT_ORDINAL)
                .setScope(TransactionScope.AUTOCOMMIT)
                .setRepresentation(WriteRepresentation.BUFFERED)
                .setAckMode(CommitAckMode.VISIBLE)
                .build();
    }

    private DurableIngestCoordinator open(Clock clock) throws IOException
    {
        return new DurableIngestCoordinator(
                new AtomicStateFile(directory, 1024 * 1024),
                new DurableIngestCoordinator.Tables()
                {
                    public TableSpec load(String schema, String table) { return TABLE; }
                    public List<Route> routes() { return Collections.singletonList(ROUTE); }
                },
                new DurableIngestCoordinator.Participants()
                {
                    public PrepareToken prepare(String owner, Transaction tx) throws IOException
                    {
                        return PrepareToken.newBuilder().setOwner(owner)
                                .setDigest(ByteString.copyFrom(IngestWire.prepareDigest(tx, owner)))
                                .build();
                    }
                    public boolean install(
                            String owner, Transaction tx, boolean forceFileTail) { return true; }
                    public void checkpoint(String owner, long txId) { checkpoints.incrementAndGet(); }
                    public void discard(String owner, long txId) {}
                },
                ids::incrementAndGet, clock, 0, 60_000, 100, 64,
                120_000, 100);
    }
}
