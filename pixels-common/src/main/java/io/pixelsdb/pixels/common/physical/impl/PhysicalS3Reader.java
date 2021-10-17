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
package io.pixelsdb.pixels.common.physical.impl;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.Storage;
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

    static
    {
        clientService = Executors.newFixedThreadPool(32);
        Runtime.getRuntime().addShutdownHook( new Thread(clientService::shutdownNow));
    }

    private S3 s3;
    private S3.Path path;
    private String pathStr;
    private long id;
    private AtomicLong position;
    private long length;
    private S3Client client;
    private S3AsyncClient asyncClient;

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
        throw new IOException("Desired offset is out of bound.");
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
        if (useAsyncClient)
        {
            future = asyncClient.getObject(request, AsyncResponseTransformer.toBytes());
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
        return s3.getFileId(pathStr);
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
