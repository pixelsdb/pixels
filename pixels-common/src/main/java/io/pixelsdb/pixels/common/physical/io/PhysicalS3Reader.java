/*
 * Copyright 2021 PixelsDB.
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
package io.pixelsdb.pixels.common.physical.io;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.storage.S3;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created at: 06/09/2021
 * Author: hank
 */
public class PhysicalS3Reader implements PhysicalReader
{
    private static Logger logger = LogManager.getLogger(PhysicalS3Reader.class);
    private static boolean enableAsync = false;
    private static boolean useAsyncClient = false;
    private static final ExecutorService clientService;
    private final static int LEN_1M = 1024*1024;
    private final static int LEN_10M = 1024*1024*10;
    private final static int ADAPTIVE_READ_TH = 2*1024*1024;

    static
    {
        int clientServiceThreads = Integer.parseInt(
                ConfigFactory.Instance().getProperty("s3.client.service.threads"));
        ThreadGroup clientServiceGroup = new ThreadGroup("s3.client.service");
        clientServiceGroup.setDaemon(true);
        clientServiceGroup.setMaxPriority(Thread.MAX_PRIORITY-1);
        clientService = Executors.newFixedThreadPool(clientServiceThreads, runnable -> {
            Thread thread = new Thread(clientServiceGroup, runnable);
            thread.setDaemon(true);
            /**
             * Issue #136:
             * Using MAX_PRIORITY-1 for client threads accelerates async read with sync client for
             * some queries that send many get object requests (e.g. 5%).
             * Note that clientService is only used for the cause that async client is disabled while
             * async read is enabled.
             */
            thread.setPriority(Thread.MAX_PRIORITY-1);
            return thread;
        });

        Runtime.getRuntime().addShutdownHook(new Thread(clientService::shutdownNow));
    }

    private final S3 s3;
    private final S3.Path path;
    private final String pathStr;
    private final long id;
    private final AtomicLong position;
    private final long length;
    private final S3Client client;
    private final S3AsyncClient asyncClient;
    private final S3AsyncClient asyncClient1M;
    private final S3AsyncClient asyncClient10M;

    public PhysicalS3Reader(Storage storage, String path) throws IOException
    {
        if (storage instanceof S3)
        {
            this.s3 = (S3) storage;
        }
        else
        {
            throw new IOException("Storage is not S3.");
        }
        if (path.startsWith("s3://"))
        {
            // remove the scheme.
            path = path.substring(5);
        }
        this.path = new S3.Path(path);
        this.pathStr = path;
        this.id = this.s3.getFileId(path);
        this.length = this.s3.getStatus(path).getLength();
        this.position = new AtomicLong(0);
        this.client = this.s3.getClient();
        this.asyncClient = this.s3.getAsyncClient();
        if (S3.isRequestDiversionEnabled())
        {
            this.asyncClient1M = this.s3.getAsyncClient1M();
            this.asyncClient10M = this.s3.getAsyncClient10M();
        }
        else
        {
            this.asyncClient1M = this.asyncClient10M = null;
        }
        enableAsync = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("s3.enable.async"));
        useAsyncClient = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("s3.use.async.client"));
        if (!useAsyncClient)
        {
            this.asyncClient.close();
        }
    }

    private String toRange(long start, int length)
    {
        assert start >= 0 && length > 0;
        StringBuilder builder = new StringBuilder("bytes=");
        builder.append(start).append('-').append(start+length-1);
        return builder.toString();
    }

    @Override
    public long getFileLength() throws IOException
    {
        return length;
    }

    @Override
    public void seek(long desired) throws IOException
    {
        if (0 <= desired && desired < length)
        {
            position.set(desired);
            return;
        }
        throw new IOException("Desired offset " + desired +
                " is out of bound (" + 0 + "," + length + ")");
    }

    @Override
    public ByteBuffer readFully(int len) throws IOException
    {
        if (this.position.get() + len > this.length)
        {
            throw new IOException("Current position " + this.position.get() + " plus " +
                    len + " exceeds object length " + this.length + ".");
        }
        GetObjectRequest request = GetObjectRequest.builder().bucket(path.bucket)
                .key(path.key).range(toRange(position.get(), len)).build();
        ResponseBytes<GetObjectResponse> response =
                client.getObject(request, ResponseTransformer.toBytes());
        try
        {
            this.position.addAndGet(len);
            return ByteBuffer.wrap(response.asByteArrayUnsafe());
        } catch (Exception e)
        {
            throw new IOException("Failed to read object.", e);
        }
    }

    @Override
    public void readFully(byte[] buffer) throws IOException
    {
        ByteBuffer byteBuffer = readFully(buffer.length);
        System.arraycopy(byteBuffer.array(), 0, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(byte[] buffer, int off, int len) throws IOException
    {
        ByteBuffer byteBuffer = readFully(len);
        System.arraycopy(byteBuffer.array(), 0, buffer, off, len);
    }

    /**
     * @return true if readAsync is supported.
     */
    @Override
    public boolean supportsAsync()
    {
        return enableAsync;
    }

    @Override
    public CompletableFuture<ByteBuffer> readAsync(long offset, int len) throws IOException
    {
        if (offset + len > this.length)
        {
            throw new IOException("Offset " + offset + " plus " +
                    len + " exceeds object length " + this.length + ".");
        }
        GetObjectRequest request = GetObjectRequest.builder().bucket(path.bucket)
                .key(path.key).range(toRange(offset, len)).build();
        CompletableFuture<ResponseBytes<GetObjectResponse>> future;
        if (useAsyncClient && len < ADAPTIVE_READ_TH)
        {
            if (S3.isRequestDiversionEnabled())
            {
                if (len < LEN_1M)
                {
                    future = asyncClient.getObject(request, AsyncResponseTransformer.toBytes());
                } else if (len < LEN_10M)
                {
                    future = asyncClient1M.getObject(request, AsyncResponseTransformer.toBytes());
                } else
                {
                    future = asyncClient10M.getObject(request, AsyncResponseTransformer.toBytes());
                }
            }
            else
            {
                future = asyncClient.getObject(request, AsyncResponseTransformer.toBytes());
            }
        }
        else
        {
            future = new CompletableFuture<>();
            clientService.execute(() -> {
                ResponseBytes<GetObjectResponse> response =
                        client.getObject(request, ResponseTransformer.toBytes());
                future.complete(response);
            });
        }

        try
        {
            /**
             * Issue #128:
             * We tried to use thenApplySync using the clientService executor,
             * it does not help improving the query performance.
             */
            return future.thenApply(resp ->
            {
                if (resp != null)
                {
                    return ByteBuffer.wrap(resp.asByteArrayUnsafe());
                }
                else
                {
                    logger.error("Failed complete the asynchronous read.");
                    return null;
                }
            });
        } catch (Exception e)
        {
            throw new IOException("Failed to read object.", e);
        }
    }

    @Override
    public long readLong() throws IOException
    {
        ByteBuffer buffer = readFully(Long.BYTES);
        return buffer.getLong();
    }

    @Override
    public int readInt() throws IOException
    {
        ByteBuffer buffer = readFully(Integer.BYTES);
        return buffer.getInt();
    }

    @Override
    public void close() throws IOException
    {
        // Should not close the client because it is shared by all threads.
        // this.client.close(); // Closing s3 client may take several seconds.
    }

    @Override
    public String getPath()
    {
        return pathStr;
    }

    /**
     * Get the last domain in path.
     *
     * @return
     */
    @Override
    public String getName()
    {
        return path.key;
    }

    /**
     * For a file or object in the storage, it may have one or more
     * blocks. Each block has its unique id. This method returns the
     * block id of the current block that is been reading.
     * <p>
     * For local fs, each file has only one block id, which is also
     * the file id.
     *
     * <p>Note: Storage.getFileId() assumes that each file or object
     * only has one block. In this case, the file id is the same as
     * the block id here.</p>
     *
     * @return
     * @throws IOException
     */
    @Override
    public long getBlockId() throws IOException
    {
        return this.id;
    }

    /**
     * Get the scheme of the backed physical storage.
     *
     * @return
     */
    @Override
    public Storage.Scheme getStorageScheme()
    {
        return s3.getScheme();
    }
}
