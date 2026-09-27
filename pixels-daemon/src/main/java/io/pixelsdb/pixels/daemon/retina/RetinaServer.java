/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.daemon.retina;

import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.pixelsdb.pixels.common.index.service.IndexServiceProvider;
import io.pixelsdb.pixels.common.ingest.rpc.IngestAuth;
import io.pixelsdb.pixels.common.ingest.rpc.IngestClient;
import io.pixelsdb.pixels.common.ingest.rpc.IngestOptions;
import io.pixelsdb.pixels.common.ingest.wire.IngestWire;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.server.Server;
import io.pixelsdb.pixels.common.utils.NetUtils;
import io.pixelsdb.pixels.daemon.heartbeat.HeartbeatWorker;
import io.pixelsdb.pixels.daemon.heartbeat.NodeStatus;
import io.pixelsdb.pixels.ingest.IngestProto.OwnerRequest;
import io.pixelsdb.pixels.ingest.IngestProto.Transaction;
import io.pixelsdb.pixels.ingest.IngestProto.TransactionList;
import io.pixelsdb.pixels.retina.RetinaResourceManager;
import io.pixelsdb.pixels.retina.ingest.IngestParticipantRpc;
import io.pixelsdb.pixels.retina.ingest.InstallationStateStore;
import io.pixelsdb.pixels.retina.ingest.LocalMutationJournal;
import io.pixelsdb.pixels.retina.ingest.PixelsIngestInstaller;
import io.pixelsdb.pixels.retina.ingest.RetinaIngestParticipant;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @create 2024-12-20
 * @author gengdy
 */
public class RetinaServer implements Server
{
    private static final Logger log = LogManager.getLogger(RetinaServer.class);

    private volatile boolean running = false;
    private final int port;
    private volatile io.grpc.Server rpcServer;
    private volatile IngestClient ingestClient;
    private volatile RetinaIngestParticipant ingestParticipant;
    private volatile RetinaResourceManager retinaResources;
    private final AtomicBoolean ingestRecoveryStarted = new AtomicBoolean();
    private final AtomicReference<Throwable> ingestRecoveryFailure = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public RetinaServer(int port)
    {
        checkArgument(port > 0 && port <= 65535, "illegal rpc port");
        this.port = port;
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
        io.grpc.Server server = this.rpcServer;
        if (server != null)
        {
            try
            {
                server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                log.error("Interrupted when shutdown rpc server", e);
            }
        }
        RetinaIngestParticipant participant = this.ingestParticipant;
        if (participant != null)
        {
            try { participant.close(); }
            catch (IOException e) { log.error("Failed to close ingest participant", e); }
        }
        IngestClient client = this.ingestClient;
        if (client != null)
        {
            client.close();
        }
        RetinaResourceManager resources = this.retinaResources;
        if (resources != null)
        {
            try { resources.shutdown(); }
            catch (Exception e) { log.error("Failed to shutdown Retina shared resources", e); }
        }
    }

    @Override
    public void run()
    {
        InstallationStateStore installationState = null;
        try
        {
            HeartbeatWorker.setCurrentStatus(NodeStatus.INIT);
            IngestOptions options = new IngestOptions();
            RetinaResourceManager resources = RetinaResourceManager.Instance();
            Set<Long> bootstrapRecoveryFileIds = Collections.emptySet();
            if (options.enabled)
            {
                installationState = new InstallationStateStore(
                        Paths.get(options.participantPlanDirectory), options.maxStateBytes,
                        options.planCompactionBytes);
                bootstrapRecoveryFileIds = PixelsIngestInstaller.recoveryFileIds(
                        installationState);
            }
            RetinaServerImpl service = options.enabled
                    ? new RetinaServerImpl(
                            MetadataService.Instance(),
                            IndexServiceProvider.getService(IndexServiceProvider.ServiceMode.local),
                            resources,
                            bootstrapRecoveryFileIds)
                    : new RetinaServerImpl();
            ServerBuilder<?> builder = ServerBuilder.forPort(port).addService(service);
            if (options.enabled)
            {
                String ownerHost = IngestOptions.property("retina.server.host", "").trim();
                if (ownerHost.isEmpty())
                {
                    throw new IOException("retina.server.host is required for ingest owner identity");
                }
                String heartbeatHost = NetUtils.getLocalHostName();
                if (!ownerHost.equals(heartbeatHost))
                {
                    throw new IOException("retina.server.host=" + ownerHost
                            + " must match the Retina heartbeat identity " + heartbeatHost);
                }
                String owner = ownerHost + ":" + port;
                String secret = IngestAuth.configuredSecret();
                IngestClient client = IngestClient.fromConfig();
                this.retinaResources = resources;
                PixelsIngestInstaller installer = null;
                LocalMutationJournal journal = null;
                try
                {
                    installer = new PixelsIngestInstaller(
                            installationState,
                            options, owner, resources,
                            IndexServiceProvider.getService(IndexServiceProvider.ServiceMode.local),
                            MetadataService.Instance());
                    installationState = null;
                    journal = new LocalMutationJournal(
                            Files.createDirectories(Paths.get(options.participantWalDirectory)),
                            options.walSegmentBytes,
                            options.walMaxBytes, options.walMaxRecords,
                            options.walGroupCommitDelayMicros,
                            options.walReadCacheMaxBytes);
                    RetinaIngestParticipant participant = new RetinaIngestParticipant(
                            owner, journal,
                            new RetinaIngestParticipant.Decisions()
                            {
                                @Override
                                public Transaction get(long id)
                                {
                                    return client.coordinator().getWrite(IngestWire.id(id));
                                }

                                @Override
                                public Transaction abort(long id)
                                {
                                    return client.coordinator().abortWrite(IngestWire.id(id));
                                }

                                @Override
                                public TransactionList list(String requestedOwner)
                                {
                                    if (!owner.equals(requestedOwner))
                                    {
                                        throw new IllegalArgumentException("Incorrect participant owner");
                                    }
                                    return client.coordinator().listWrites(
                                            OwnerRequest.newBuilder().setOwner(owner).build());
                                }

                                @Override
                                public long publishedTimestamp()
                                {
                                    return client.coordinator().getPublication(
                                            com.google.protobuf.Empty.getDefaultInstance()).getPublishedTimestamp();
                                }
                            }, installer, resources.getIngestReadPins(),
                            options.privateReadMaxBatches, options.privateReadMaxBytes);
                    this.ingestClient = client;
                    this.ingestParticipant = participant;
                    builder.maxInboundMessageSize(options.maxStateBytes)
                            .addService(ServerInterceptors.intercept(
                                    new IngestParticipantRpc(participant),
                                    IngestAuth.server(secret)));
                }
                catch (Throwable e)
                {
                    if (journal != null)
                    {
                        try { journal.close(); } catch (Exception suppressed) { e.addSuppressed(suppressed); }
                    }
                    if (installer != null)
                    {
                        try { installer.close(); } catch (Exception suppressed) { e.addSuppressed(suppressed); }
                    }
                    client.close();
                    throw e;
                }
            }
            service.setReadyListener(() -> recoverIngestAndPublishReady(service));
            io.grpc.Server server = builder.build();
            this.rpcServer = server;
            server.start();
            this.running = true;
            recoverIngestAndPublishReady(service);
            server.awaitTermination();
            Throwable recoveryFailure = ingestRecoveryFailure.get();
            if (recoveryFailure != null)
            {
                throw new IllegalStateException(
                        "Retina ingest recovery failed; service was never READY", recoveryFailure);
            }
        } catch (Throwable e)
        {
            if (installationState != null)
            {
                try { installationState.close(); }
                catch (IOException suppressed) { e.addSuppressed(suppressed); }
            }
            HeartbeatWorker.setCurrentStatus(NodeStatus.EXIT);
            if (e instanceof InterruptedException)
            {
                Thread.currentThread().interrupt();
            }
            log.error("Retina server failed", e);
            throw new IllegalStateException(
                    "Retina server stopped because startup, recovery, or RPC service failed", e);
        } finally
        {
            this.shutdown();
        }
    }

    private void recoverIngestAndPublishReady(RetinaServerImpl service)
    {
        if (!this.running)
        {
            return;
        }
        RetinaIngestParticipant participant = this.ingestParticipant;
        if (participant != null && !participant.isReady()
                && (service.isReady() || service.isRecovering()))
        {
            if (!ingestRecoveryStarted.compareAndSet(false, true))
            {
                return;
            }
            try
            {
                participant.recover();
                log.info("Retina ingest participant recovery completed");
            }
            catch (Throwable e)
            {
                ingestRecoveryFailure.compareAndSet(null, e);
                HeartbeatWorker.setCurrentStatus(NodeStatus.EXIT);
                log.error("Retina ingest recovery failed; shutting down RPC service", e);
                io.grpc.Server server = this.rpcServer;
                if (server != null)
                {
                    server.shutdownNow();
                }
                return;
            }
        }
        if (participant != null && participant.isReady() && service.isRecovering())
        {
            try
            {
                service.completeTransactionalRecovery();
            }
            catch (Throwable e)
            {
                ingestRecoveryFailure.compareAndSet(null, e);
                HeartbeatWorker.setCurrentStatus(NodeStatus.EXIT);
                log.error("Retina transactional recovery could not publish READY", e);
                io.grpc.Server server = this.rpcServer;
                if (server != null)
                {
                    server.shutdownNow();
                }
                return;
            }
        }
        if (this.running && service.isReady()
                && (participant == null || participant.isReady()))
        {
            HeartbeatWorker.setCurrentStatus(NodeStatus.READY);
            log.info("Retina service and transactional ingestion are ready");
        }
    }
}
