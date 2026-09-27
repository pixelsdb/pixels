/*
 * Copyright 2022 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.daemon.transaction;

import io.grpc.ServerInterceptors;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.ServerBuilder;
import io.pixelsdb.pixels.common.ingest.rpc.IngestAuth;
import io.pixelsdb.pixels.common.ingest.rpc.IngestClient;
import io.pixelsdb.pixels.common.ingest.rpc.IngestOptions;
import io.pixelsdb.pixels.common.ingest.wire.IngestWire;
import io.pixelsdb.pixels.common.server.Server;
import io.pixelsdb.pixels.core.ingest.IngestTables;
import io.pixelsdb.pixels.daemon.transaction.ingest.CoordinatorStateStore;
import io.pixelsdb.pixels.daemon.transaction.ingest.DurableIngestCoordinator;
import io.pixelsdb.pixels.daemon.transaction.ingest.IngestCoordinatorRpc;
import io.pixelsdb.pixels.ingest.IngestProto.ParticipantRequest;
import io.pixelsdb.pixels.ingest.IngestProto.PrepareToken;
import io.pixelsdb.pixels.ingest.IngestProto.Route;
import io.pixelsdb.pixels.ingest.IngestProto.TableSpec;
import io.pixelsdb.pixels.ingest.IngestProto.Transaction;

/**
 * @author hank
 * @create 2022-02-20
 */
public class TransServer implements Server
{
    private static final int MAX_TCP_PORT = 65_535;
    private static final Logger log = LogManager.getLogger(TransServer.class);

    private volatile boolean running = false;
    private final io.grpc.Server rpcServer;
    private final IngestClient ingestClient;
    private final DurableIngestCoordinator ingestCoordinator;
    private final AtomicBoolean closed = new AtomicBoolean();

    public TransServer(int port) throws Exception
    {
        if (port <= 0 || port > MAX_TCP_PORT)
        {
            throw new IllegalArgumentException("Invalid transaction service port: " + port);
        }
        IngestOptions options = new IngestOptions();
        TransServiceImpl transService = new TransServiceImpl();
        if (!options.enabled)
        {
            this.ingestClient = null;
            this.ingestCoordinator = null;
            this.rpcServer = ServerBuilder.forPort(port).addService(transService).build();
            return;
        }

        String secret = IngestAuth.configuredSecret();
        IngestClient client = IngestClient.fromConfig();
        DurableIngestCoordinator coordinator = null;
        try
        {
            long legacyPublished = TransServiceImpl.legacyPublishedTimestamp();
            if (options.cutoverBaselineTimestamp < legacyPublished)
            {
                throw new IOException("retina.ingest.cutover.baseline.timestamp="
                        + options.cutoverBaselineTimestamp + " is below the legacy published timestamp "
                        + legacyPublished + "; complete the offline cutover and configure its fixed baseline");
            }
            AtomicLong firstId = new AtomicLong(TransServiceImpl.allocateIngestTimestamp());
            coordinator = new DurableIngestCoordinator(
                    new CoordinatorStateStore(Paths.get(options.coordinatorStateDirectory),
                            options.maxStateBytes, options.coordinatorCompactionBytes),
                    new DurableIngestCoordinator.Tables()
                    {
                        @Override
                        public TableSpec load(String schema, String table) throws Exception
                        {
                            return IngestTables.load(schema, table);
                        }

                        @Override
                        public List<Route> routes() throws Exception
                        {
                            return IngestTables.routes();
                        }
                    },
                    new DurableIngestCoordinator.Participants()
                    {
                        @Override
                        public PrepareToken prepare(String owner, Transaction transaction)
                        {
                            return client.participant(owner).prepare(
                                    ParticipantRequest.newBuilder()
                                            .setOwner(owner).setTransaction(transaction).build());
                        }

                        @Override
                        public boolean install(
                                String owner, Transaction transaction, boolean forceFileTail)
                        {
                            return client.participant(
                                            owner,
                                            options.installRpcTimeoutMillis,
                                            TimeUnit.MILLISECONDS)
                                    .install(
                                    ParticipantRequest.newBuilder()
                                            .setOwner(owner)
                                            .setTransaction(transaction)
                                            .setForceFileTail(forceFileTail)
                                            .build())
                                    .getReady();
                        }

                        @Override
                        public void checkpoint(String owner, long transactionId)
                        {
                            client.participant(owner).checkpoint(IngestWire.id(transactionId));
                        }

                        @Override
                        public void discard(String owner, long transactionId)
                        {
                            client.participant(owner).discard(IngestWire.id(transactionId));
                        }
                    },
                    () -> {
                        long initial = firstId.getAndSet(-1);
                        if (initial >= 0)
                        {
                            return initial;
                        }
                        try
                        {
                            return TransServiceImpl.allocateIngestTimestamp();
                        }
                        catch (Exception e)
                        {
                            throw new IllegalStateException("Cannot allocate ingest timestamp", e);
                        }
                    },
                    Clock.systemUTC(), options.cutoverBaselineTimestamp,
                    options.transactionLeaseMillis, options.maxTransactions, options.maxStreams,
                    options.terminalRetentionMillis, options.maxTerminalTransactions,
                    options.installationThreads, options.coordinatorGroupCommitDelayMicros);
            long first = firstId.get();
            long floor = Math.max(options.cutoverBaselineTimestamp, coordinator.lastCommitTimestamp());
            if (first <= floor)
            {
                throw new IOException("Transaction id allocator next value " + first
                        + " is not above ingest/cutover timestamp floor " + floor
                        + "; advance the transaction id domain while writes are drained");
            }
            TransServiceImpl.setIngestPublishedTimestamp(coordinator::publishedTimestamp);
            this.rpcServer = ServerBuilder.forPort(port)
                    .addService(transService)
                    .addService(ServerInterceptors.intercept(
                            new IngestCoordinatorRpc(coordinator), IngestAuth.server(secret)))
                    .build();
            this.ingestClient = client;
            this.ingestCoordinator = coordinator;
        }
        catch (Throwable e)
        {
            TransServiceImpl.setIngestPublishedTimestamp(null);
            if (coordinator != null)
            {
                try { coordinator.close(); } catch (Exception suppressed) { e.addSuppressed(suppressed); }
            }
            client.close();
            throw e;
        }
    }

    @Override
    public boolean isRunning()
    {
        return this.running;
    }

    @Override
    public void shutdown()
    {
        this.running = false;
        if (!closed.compareAndSet(false, true))
        {
            return;
        }
        try
        {
            this.rpcServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            log.error("Interrupted when shutdown transaction server.", e);
        }
        finally
        {
            TransServiceImpl.setIngestPublishedTimestamp(null);
            if (ingestCoordinator != null)
            {
                try { ingestCoordinator.close(); }
                catch (IOException e) { log.error("Failed to close ingest coordinator", e); }
            }
            if (ingestClient != null)
            {
                ingestClient.close();
            }
        }
    }

    @Override
    public void run()
    {
        try
        {
            this.rpcServer.start();
            if (this.ingestCoordinator != null)
            {
                this.ingestCoordinator.start();
            }
            this.running = true;
            this.rpcServer.awaitTermination();
        } catch (IOException e)
        {
            log.error("I/O error when running.", e);
            throw new IllegalStateException("Transaction server I/O failure", e);
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            log.error("Interrupted when running.", e);
            throw new IllegalStateException("Transaction server interrupted unexpectedly", e);
        } finally
        {
            this.shutdown();
        }
    }
}
