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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;

/** Deployment credential for authenticated ingestion RPCs. Network transport requires TLS or a trusted network. */
public final class IngestAuth {
    public static final Metadata.Key<String> KEY =
            Metadata.Key.of("x-pixels-ingest-secret", Metadata.ASCII_STRING_MARSHALLER);

    private IngestAuth() {}

    public static String configuredSecret() throws IOException {
        String path = IngestOptions.property("retina.ingest.auth.secret.file", "");
        if (path.isEmpty()) throw new IOException("Ingest credential file required");
        byte[] b = Files.readAllBytes(Paths.get(path));
        String secret = new String(b, StandardCharsets.UTF_8).trim();
        byte[] normalized = secret.getBytes(StandardCharsets.UTF_8);
        if (normalized.length < 24 || normalized.length > 4096) {
            throw new IOException("Invalid credential length after trimming whitespace");
        }
        for (int i = 0; i < secret.length(); i++) {
            char c = secret.charAt(i);
            if (c < 0x21 || c > 0x7e) {
                throw new IOException("Ingest credential must contain visible ASCII characters only");
            }
        }
        return secret;
    }

    public static ClientInterceptor client(String secret) {
        return new ClientInterceptor() {
            public <Q, A> ClientCall<Q, A> interceptCall(
                    MethodDescriptor<Q, A> method, CallOptions options, Channel next) {
                return new ForwardingClientCall.SimpleForwardingClientCall<Q, A>(
                        next.newCall(method, options)) {
                    public void start(Listener<A> listener, Metadata headers) {
                        headers.put(KEY, secret);
                        super.start(listener, headers);
                    }
                };
            }
        };
    }

    public static ServerInterceptor server(String secret) {
        return new ServerInterceptor() {
            public <Q, A> ServerCall.Listener<Q> interceptCall(
                    ServerCall<Q, A> call, Metadata headers, ServerCallHandler<Q, A> next) {
                String value = headers.get(KEY);
                if (value == null
                        || !MessageDigest.isEqual(
                                value.getBytes(StandardCharsets.UTF_8),
                                secret.getBytes(StandardCharsets.UTF_8))) {
                    call.close(Status.UNAUTHENTICATED, new Metadata());
                    return new ServerCall.Listener<Q>() {};
                }
                return next.startCall(call, headers);
            }
        };
    }
}
