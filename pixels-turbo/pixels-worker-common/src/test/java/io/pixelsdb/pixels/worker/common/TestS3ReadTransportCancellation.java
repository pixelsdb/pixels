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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.worker.common;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.storage.s3.PhysicalS3Reader;
import io.pixelsdb.pixels.storage.s3.S3;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.core.interceptor.*;
import org.reactivestreams.Publisher;
import java.util.Optional;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.*;

import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;

/** Real SDK and HTTP transport, loopback only: cancellation must close the peer connection. */
public class TestS3ReadTransportCancellation
{
    @Test(timeout = 20000)
    public void realSdkRetriesAndDeliversTheExactRange() throws Exception
    {
        ConfigFactory.Instance().addProperty("s3.enable.async", "true");
        ConfigFactory.Instance().addProperty("s3.use.async.client", "true");
        ExecutorService peer = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
             S3AsyncClient async = S3AsyncClient.builder()
                     .endpointOverride(new URI("http://127.0.0.1:" + server.getLocalPort()))
                     .region(Region.US_EAST_1)
                     .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                     .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                     .httpClientBuilder(NettyNioAsyncHttpClient.builder().maxConcurrency(1))
                     .build())
        {
            server.setSoTimeout(10000);
            Future<?> served = peer.submit(() -> {
                for (int attempt = 0; attempt < 2; attempt++)
                {
                    try (Socket socket = server.accept())
                    {
                        socket.setSoTimeout(3000);
                        StringBuilder request = new StringBuilder();
                        while (!request.toString().endsWith("\r\n\r\n"))
                        {
                            int value = socket.getInputStream().read();
                            if (value < 0) { throw new java.io.EOFException("missing request"); }
                            request.append((char) value);
                        }
                        assertTrue(request.toString().toLowerCase(java.util.Locale.ROOT).contains("range: bytes=4-7"));
                        String headers = attempt == 0 ?
                                "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n" :
                                "HTTP/1.1 206 Partial Content\r\nContent-Length: 4\r\nContent-Range: bytes 4-7/16\r\nConnection: close\r\n\r\n";
                        socket.getOutputStream().write(headers.getBytes(StandardCharsets.US_ASCII));
                        if (attempt == 1) { socket.getOutputStream().write(new byte[]{1, 2, 3, 4}); }
                        socket.getOutputStream().flush();
                    }
                }
                return null;
            });
            S3Client sync = mock(S3Client.class);
            when(sync.headObject(any(HeadObjectRequest.class))).thenReturn(HeadObjectResponse.builder().contentLength(16L).build());
            try (PhysicalS3Reader reader = new PhysicalS3Reader(new S3(sync, async), "bucket/input"))
            {
                assertEquals(0x01020304, reader.readAsync(4, 4).get(10, TimeUnit.SECONDS).getInt());
                served.get(2, TimeUnit.SECONDS);
            }
        }
        finally { peer.shutdownNow(); assertTrue(peer.awaitTermination(2, TimeUnit.SECONDS)); }
    }

    @Test(timeout = 20000)
    public void cancellationClosesWireBeforeHeadersAndDuringBody() throws Exception
    {
        ConfigFactory.Instance().addProperty("s3.enable.async", "true");
        ConfigFactory.Instance().addProperty("s3.use.async.client", "true");
        for (boolean headers : new boolean[]{false, true})
        {
            for (boolean closeReader : new boolean[]{false, true})
            {
                ExecutorService peer = Executors.newSingleThreadExecutor();
                CountDownLatch bodyReceived = new CountDownLatch(1);
                try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
                     S3AsyncClient async = S3AsyncClient.builder()
                             .endpointOverride(new URI("http://127.0.0.1:" + server.getLocalPort()))
                             .region(Region.US_EAST_1)
                             .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                             .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                             .overrideConfiguration(config -> config.addExecutionInterceptor(new ExecutionInterceptor()
                             {
                                 @Override
                                 public Optional<Publisher<ByteBuffer>> modifyAsyncHttpResponseContent(
                                         Context.ModifyHttpResponse context, ExecutionAttributes attributes)
                                 {
                                     return context.responsePublisher().map(publisher -> SdkPublisher.adapt(publisher).map(buffer -> {
                                         bodyReceived.countDown(); return buffer;
                                     }));
                                 }
                             }))
                             .httpClientBuilder(NettyNioAsyncHttpClient.builder().maxConcurrency(1).readTimeout(Duration.ofSeconds(10)))
                             .build())
                {
                    CountDownLatch requested = new CountDownLatch(1);
                    Future<Integer> disconnected = peer.submit(() -> {
                        try (Socket connection = server.accept())
                        {
                            connection.setSoTimeout(3000);
                            StringBuilder request = new StringBuilder();
                            while (!request.toString().endsWith("\r\n\r\n"))
                            {
                                int value = connection.getInputStream().read();
                                if (value < 0) { throw new java.io.EOFException("missing request headers"); }
                                request.append((char) value);
                            }
                            if (headers)
                            {
                                connection.getOutputStream().write(("HTTP/1.1 206 Partial Content\r\nContent-Length: 4194304\r\n" +
                                        "Content-Type: application/octet-stream\r\n\r\nx").getBytes(StandardCharsets.US_ASCII));
                                connection.getOutputStream().flush();
                            }
                            requested.countDown();
                            return connection.getInputStream().read();
                        }
                    });
                    S3Client sync = mock(S3Client.class);
                    when(sync.headObject(any(HeadObjectRequest.class))).thenReturn(
                            HeadObjectResponse.builder().contentLength(4194304L).build());
                    try (PhysicalS3Reader reader = new PhysicalS3Reader(new S3(sync, async), "bucket/input"))
                    {
                        CompletableFuture<ByteBuffer> read = reader.readAsync(0, 4194304);
                        assertTrue(requested.await(3, TimeUnit.SECONDS));
                        if (headers) { assertTrue("client must have received body bytes", bodyReceived.await(3, TimeUnit.SECONDS)); }
                        if (closeReader) { reader.close(); } else { read.cancel(true); }
                        assertEquals("cancel must close HTTP connection, headers=" + headers + ", close=" + closeReader,
                                Integer.valueOf(-1), disconnected.get(4, TimeUnit.SECONDS));
                        assertTrue(read.isCompletedExceptionally());
                    }
                }
                finally { peer.shutdownNow(); assertTrue(peer.awaitTermination(2, TimeUnit.SECONDS)); }
            }
        }
    }
    /** Documents the SDK boundary, not a promise that close() has stopped the wire. */
    @Test(timeout = 15000)
    public void sdkCancellationStillNeedsTheEventLoopToAbortTheConnection() throws Exception
    {
        ConfigFactory.Instance().addProperty("s3.use.async.client", "true");
        io.netty.channel.nio.NioEventLoopGroup loops = new io.netty.channel.nio.NioEventLoopGroup(1);
        CountDownLatch unblock = new CountDownLatch(1), blocked = new CountDownLatch(1), requested = new CountDownLatch(1);
        ExecutorService peer = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
             S3AsyncClient async = S3AsyncClient.builder()
                     .endpointOverride(new URI("http://127.0.0.1:" + server.getLocalPort()))
                     .region(Region.US_EAST_1)
                     .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                     .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                     .httpClientBuilder(NettyNioAsyncHttpClient.builder().maxConcurrency(1)
                             .eventLoopGroup(software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup.create(loops))).build())
        {
            Future<Integer> eof = peer.submit(() -> {
                try (Socket socket = server.accept())
                {
                    socket.setSoTimeout(5000);
                    StringBuilder headers = new StringBuilder();
                    while (!headers.toString().endsWith("\r\n\r\n"))
                    {
                        int value = socket.getInputStream().read();
                        if (value < 0) { throw new java.io.EOFException("early EOF"); }
                        headers.append((char) value);
                    }
                    requested.countDown();
                    return socket.getInputStream().read();
                }
            });
            S3Client sync = mock(S3Client.class);
            when(sync.headObject(any(HeadObjectRequest.class)))
                    .thenReturn(HeadObjectResponse.builder().contentLength(1024L).build());
            try (PhysicalS3Reader reader = new PhysicalS3Reader(new S3(sync, async), "bucket/input"))
            {
                CompletableFuture<ByteBuffer> read = reader.readAsync(0, 1024);
                assertTrue(requested.await(3, TimeUnit.SECONDS));
                loops.submit(() -> {
                    blocked.countDown();
                    try { unblock.await(); }
                    catch (InterruptedException failure) { Thread.currentThread().interrupt(); }
                });
                assertTrue(blocked.await(2, TimeUnit.SECONDS));
                reader.close();
                assertTrue(read.isCancelled());
                assertThrows(TimeoutException.class, () -> eof.get(150, TimeUnit.MILLISECONDS));
                unblock.countDown();
                assertEquals(Integer.valueOf(-1), eof.get(3, TimeUnit.SECONDS));
            }
            finally { unblock.countDown(); }
        }
        finally
        {
            unblock.countDown(); peer.shutdownNow();
            loops.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
        }
    }

}
