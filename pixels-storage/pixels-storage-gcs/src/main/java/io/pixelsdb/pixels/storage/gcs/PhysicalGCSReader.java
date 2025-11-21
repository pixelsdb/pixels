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
package io.pixelsdb.pixels.storage.gcs;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import io.pixelsdb.pixels.common.physical.ObjectPath;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.ShutdownHookManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Objects.requireNonNull;

/**
 * The physical readers of Google Cloud Storage.
 *
 * @author hank
 * @create 2022-09-25
 */
public class PhysicalGCSReader implements PhysicalReader
{
    private static final Logger logger = LogManager.getLogger(PhysicalGCSReader.class);
    protected boolean enableAsync = false;
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
             * Note that clientService is only used for the cause that async client is disabled while
             * async read is enabled.
             */
            thread.setPriority(Thread.MAX_PRIORITY-1);
            return thread;
        });

        ShutdownHookManager.Instance().registerShutdownHook(PhysicalGCSReader.class, true, clientService::shutdownNow);
    }

    protected final GCS gcs;
    protected final ObjectPath path;
    protected final String pathStr;
    protected final long id;
    protected final long length;
    protected final com.google.cloud.storage.Storage client;
    protected final ReadChannel readChannel;
    protected long position;
    protected int numRequests;

    public PhysicalGCSReader(Storage storage, String path) throws IOException
    {
        if (storage instanceof GCS)
        {
            gcs = (GCS) storage;
        }
        else
        {
            throw new IOException("Storage is not GCS.");
        }
        if (path.contains("://"))
        {
            // remove the scheme.
            path = path.substring(path.indexOf("://") + 3);
        }
        this.path = new ObjectPath(path);
        this.pathStr = path;
        this.id = this.gcs.getFileId(path);
        this.length = this.gcs.getStatus(path).getLength();
        this.numRequests = 1;
        this.position = 0L;
        this.client = this.gcs.getClient();
        this.readChannel = this.client.reader(BlobId.of(this.path.bucket, this.path.key));
        this.enableAsync = Boolean.parseBoolean(ConfigFactory.Instance()
                .getProperty("gcs.enable.async"));
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
        throw new IOException("Desired offset " + desired +
                " is out of bound (" + 0 + "," + length + ")");
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
            this.readChannel.seek(this.position);
            ByteBuffer buffer = ByteBuffer.allocate(len);
            this.readChannel.read(buffer);
            this.numRequests++;
            this.position += len;
            return buffer;
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
        if (this.position + len > this.length)
        {
            throw new IOException("Current position " + this.position + " plus " +
                    len + " exceeds object length " + this.length + ".");
        }

        try
        {
            this.readChannel.seek(this.position);
            this.readChannel.read(ByteBuffer.wrap(buffer, off, len));
            this.numRequests++;
            this.position += len;
        } catch (Exception e)
        {
            throw new IOException("Failed to read object.", e);
        }
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
    public CompletableFuture<ByteBuffer> readAsync(long off, int len) throws IOException
    {
        if (off + len > this.length)
        {
            throw new IOException("Offset " + off + " plus " +
                    len + " exceeds object length " + this.length + ".");
        }
        CompletableFuture<ByteBuffer> future;
        future = new CompletableFuture<>();
        clientService.execute(() -> {
            try (ReadChannel reader = this.client.reader(BlobId.of(this.path.bucket, this.path.key)))
            {
                reader.seek(off);
                ByteBuffer buffer = ByteBuffer.allocate(len);
                reader.read(buffer);
                future.complete(buffer);
            } catch (IOException e)
            {
                future.completeExceptionally(e);
                logger.error("Failed to complete the asynchronous read, offset=" +
                        off + ", length= " + len + ".", e);
            }
        });

        this.numRequests++;
        return future;
    }

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
    public void close() throws IOException
    {
        if (this.readChannel != null)
        {
            this.readChannel.close();
        }
    }

    @Override
    public String getPath()
    {
        return pathStr;
    }

    @Override
    public String getPathUri() throws IOException
    {
        return gcs.ensureSchemePrefix(pathStr);
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
    public long getBlockId()
    {
        return this.id;
    }

    /**
     * Get the scheme of the backing physical storage.
     *
     * @return
     */
    @Override
    public Storage.Scheme getStorageScheme()
    {
        return Storage.Scheme.gcs;
    }

    @Override
    public int getNumReadRequests()
    {
        return numRequests;
    }
}
