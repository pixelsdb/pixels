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
package io.pixelsdb.pixels.storage.s3;

import io.pixelsdb.pixels.common.physical.ObjectPath;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.ShutdownHookManager;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Objects.requireNonNull;

/**
 * The abstract class for the physical readers of AWS S3 compatible storage systems.
 *
 * @author hank
 * @create 2022-10-04
 */
public abstract class AbstractS3Reader implements PhysicalReader
{
    /*
     * The implementations of most methods in this class are from its subclass PhysicalS3Reader.
     */

    protected boolean enableAsync = false;
    protected boolean useAsyncClient = false;
    protected static final ExecutorService clientService;

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
             * Note that clientService is only used in case async client is disabled and async read is enabled.
             */
            thread.setPriority(Thread.MAX_PRIORITY-1);
            return thread;
        });

        ShutdownHookManager.Instance().registerShutdownHook(PhysicalS3Reader.class, true, clientService::shutdownNow);
    }

    protected final AbstractS3 s3;
    protected final ObjectPath path;
    protected final String pathStr;
    protected final long id;
    protected final long length;
    protected final S3Client client;
    protected long position;
    protected int numRequests;

    public AbstractS3Reader(Storage storage, String path) throws IOException
    {
        // instanceof tells if an object is an instance of a class or its parent classes.
        if (storage instanceof AbstractS3)
        {
            this.s3 = (AbstractS3) storage;
        }
        else
        {
            throw new IOException("Storage is not S3 compatible.");
        }
        if (path.contains("://"))
        {
            // remove the scheme.
            path = path.substring(path.indexOf("://") + 3);
        }
        this.path = new ObjectPath(path);
        this.pathStr = path;
        this.id = this.s3.getFileId(path);
        this.length = this.s3.getStatus(path).getLength();
        this.numRequests = 1;
        this.position = 0L;
        this.client = this.s3.getS3Client();
        this.enableAsync = Boolean.parseBoolean(ConfigFactory.Instance()
                .getProperty("s3.enable.async"));
        this.useAsyncClient = Boolean.parseBoolean(ConfigFactory.Instance()
                .getProperty("s3.use.async.client"));
    }

    protected String toRange(long start, int length)
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
            position = desired;
            return;
        }
        throw new IOException("Desired offset " + desired + " is out of bound (" + 0 + "," + length + ")");
    }

    @Override
    public ByteBuffer readFully(int len) throws IOException
    {
        if (this.position + len > this.length)
        {
            throw new IOException("Current position " + this.position + " plus " +
                    len + " exceeds object length " + this.length + ".");
        }

        try
        {
            GetObjectRequest request = GetObjectRequest.builder().bucket(path.bucket)
                .key(path.key).range(toRange(position, len)).build();
            ResponseBytes<GetObjectResponse> response =
                client.getObject(request, ResponseTransformer.toBytes());
            this.numRequests++;
            this.position += len;
            return response.asByteBuffer();
        } catch (Exception e)
        {
            throw new IOException("Failed to read object.", e);
        }
    }

    @Override
    public void readFully(byte[] buffer) throws IOException
    {
        readFully(buffer, 0, buffer.length);
    }

    @Override
    public void readFully(byte[] buffer, int off, int len) throws IOException
    {
        ByteBuffer byteBuffer = readFully(len);
        // This is more efficient than byteBuffer.put(buffer, off, len).
        System.arraycopy(byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(), buffer, off, len);
    }

    /**
     * @return true if readAsync is supported.
     */
    @Override
    public boolean supportsAsync()
    {
        return this.enableAsync;
    }

    @Override
    abstract public CompletableFuture<ByteBuffer> readAsync(long offset, int len) throws IOException;

    @Override
    public long readLong(ByteOrder byteOrder) throws IOException
    {
        ByteBuffer buffer = readFully(Long.BYTES).order(requireNonNull(byteOrder));
        return buffer.getLong();
    }

    @Override
    public int readInt(ByteOrder byteOrder) throws IOException
    {
        ByteBuffer buffer = readFully(Integer.BYTES).order(requireNonNull(byteOrder));
        return buffer.getInt();
    }

    @Override
    abstract public void close() throws IOException;

    @Override
    public String getPath()
    {
        return pathStr;
    }

    @Override
    public String getPathUri() throws IOException
    {
        return s3.ensureSchemePrefix(pathStr);
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
     * block id of the current block that is to be read.
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
     * Get the scheme of the backing physical storage.
     * @return
     */
    @Override
    public Storage.Scheme getStorageScheme()
    {
        return s3.getScheme();
    }

    @Override
    public int getNumReadRequests()
    {
        return numRequests;
    }
}
