/*
 * Copyright 2026 PixelsDB.
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels. If not, see <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.common.ingest.rpc;

import io.grpc.*;
import io.pixelsdb.pixels.common.ingest.*;
import io.pixelsdb.pixels.common.ingest.wire.IngestWire;
import io.pixelsdb.pixels.ingest.*;
import io.pixelsdb.pixels.ingest.IngestProto.*;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;

/** RPC client; the receiving participant owns all durable state. */
public final class IngestClient implements Closeable {
    private final String secret;
    private final int maxBytes;
    private final long timeout;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final IngestCoordinatorServiceGrpc.IngestCoordinatorServiceBlockingStub coordinator;

    public IngestClient(String host, int port, String secret, int maxBytes, long timeout) {
        this.secret = secret;
        this.maxBytes = maxBytes;
        this.timeout = timeout;
        coordinator = IngestCoordinatorServiceGrpc.newBlockingStub(channel(host + ":" + port));
    }

    public static IngestClient fromConfig() throws Exception {
        IngestOptions o = new IngestOptions();
        return new IngestClient(
                IngestOptions.property(
                        "retina.ingest.coordinator.host",
                        IngestOptions.property("trans.server.host", "127.0.0.1")),
                Integer.parseInt(
                        IngestOptions.property(
                                "retina.ingest.coordinator.port",
                                IngestOptions.property("trans.server.port", "18889"))),
                IngestAuth.configuredSecret(),
                o.maxStateBytes,
                o.transactionLeaseMillis);
    }

    private ManagedChannel channel(String target) {
        return channels.computeIfAbsent(
                target,
                k ->
                        ManagedChannelBuilder.forTarget(k)
                                .usePlaintext()
                                .maxInboundMessageSize(maxBytes)
                                .intercept(IngestAuth.client(secret))
                                .build());
    }

    public IngestCoordinatorServiceGrpc.IngestCoordinatorServiceBlockingStub coordinator() {
        return coordinator(timeout, TimeUnit.MILLISECONDS);
    }

    public IngestCoordinatorServiceGrpc.IngestCoordinatorServiceBlockingStub coordinator(
            long callTimeout, TimeUnit unit) {
        return coordinator.withDeadlineAfter(callTimeout, unit);
    }

    public IngestParticipantServiceGrpc.IngestParticipantServiceBlockingStub participant(
            String target) {
        return participant(target, timeout, TimeUnit.MILLISECONDS);
    }

    public IngestParticipantServiceGrpc.IngestParticipantServiceBlockingStub participant(
            String target, long callTimeout, TimeUnit unit) {
        return IngestParticipantServiceGrpc.newBlockingStub(channel(target))
                .withWaitForReady()
                .withDeadlineAfter(callTimeout, unit);
    }

    public MutationTransport transport(TableSpec table) {
        return new MutationTransport() {
            public CompletableFuture<Void> append(MutationBatch batch) {
                return CompletableFuture.runAsync(
                        () -> {
                            try {
                                coordinator()
                                        .registerStream(
                                                RegisterStreamRequest.newBuilder()
                                                        .setStream(
                                                                IngestWire.encode(
                                                                        batch.getStreamId()))
                                                        .build());
                                participant(
                                                IngestWire.owner(
                                                        IngestWire.route(
                                                                table,
                                                                batch.getStreamId().getShardId())))
                                        .append(IngestWire.encode(batch));
                            } catch (Exception e) {
                                throw new CompletionException(e);
                            }
                        });
            }

            public CompletableFuture<MutationStreamSeal> seal(MutationStreamSeal expected) {
                return CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return IngestWire.decode(
                                        participant(
                                                        IngestWire.owner(
                                                                IngestWire.route(
                                                                        table,
                                                                        expected.getStreamId()
                                                                                .getShardId())))
                                                .seal(IngestWire.encode(expected)));
                            } catch (Exception e) {
                                throw new CompletionException(e);
                            }
                        });
            }
        };
    }

    public void close() {
        for (ManagedChannel channel : channels.values()) channel.shutdownNow();
    }
}
