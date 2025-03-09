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
package io.pixelsdb.pixels.storage.localfs;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.natives.PixelsRandomAccessFile;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2021-08-30
 */
public class PhysicalLocalReader implements PhysicalReader
{
    private final LocalFS local;
    private final String path;
    private final long id;
    private final PixelsRandomAccessFile raf;
    protected static final boolean enableAsync;
    protected static final ExecutorService readerService;

    static
    {
        enableAsync = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("localfs.enable.async.io"));
        int clientServiceThreads = Integer.parseInt(
                ConfigFactory.Instance().getProperty("localfs.reader.threads"));
        ThreadGroup readerGroup = new ThreadGroup("localfs.readers");
        readerGroup.setDaemon(true);
        readerGroup.setMaxPriority(Thread.MAX_PRIORITY-1);
        readerService = Executors.newFixedThreadPool(clientServiceThreads, runnable -> {
            Thread thread = new Thread(readerGroup, runnable);
            thread.setDaemon(true);
            // ensure reader threads have higher priority than other threads
            thread.setPriority(Thread.MAX_PRIORITY-1);
            return thread;
        });

        Runtime.getRuntime().addShutdownHook(new Thread(readerService::shutdownNow));
    }

    public PhysicalLocalReader(Storage storage, String path) throws IOException
    {
        if (storage instanceof LocalFS)
        {
            this.local = (LocalFS) storage;
        }
        else
        {
            throw new IOException("Storage is not LocalFS.");
        }
        if (path.startsWith("file://"))
        {
            // remove the scheme.
            path = path.substring(7);
        }
        this.path = path;
        this.raf = this.local.openRaf(path);
        this.id = this.local.getFileId(path);
    }

    @Override
    public long getFileLength() throws IOException
    {
        return raf.length();
    }

    @Override
    public void seek(long desired) throws IOException
    {
        raf.seek(desired);
    }

    @Override
    public ByteBuffer readFully(int length) throws IOException
    {
        return raf.readFully(length);
    }

    @Override
    public CompletableFuture<ByteBuffer> readAsync(long offset, int len) throws IOException
    {
        if (offset + len > this.raf.length())
        {
            throw new IOException("Offset " + offset + " plus " +
                    len + " exceeds object length " + this.raf.length() + ".");
        }

        CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
        readerService.execute(() -> {
            try
            {
                future.complete(raf.readFully(offset, len));
            } catch (IOException e)
            {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    @Override
    public void readFully(byte[] buffer) throws IOException
    {
        raf.readFully(buffer);
    }

    @Override
    public void readFully(byte[] buffer, int offset, int length) throws IOException
    {
        raf.readFully(buffer, offset, length);
    }

    @Override
    public boolean supportsAsync()
    {
        return enableAsync;
    }

    @Override
    public long readLong(ByteOrder byteOrder) throws IOException
    {
        if (requireNonNull(byteOrder).equals(ByteOrder.BIG_ENDIAN))
        {
            return raf.readLong();
        }
        else
        {
            return Long.reverseBytes(raf.readLong());
        }
    }

    @Override
    public int readInt(ByteOrder byteOrder) throws IOException
    {
        if (requireNonNull(byteOrder).equals(ByteOrder.BIG_ENDIAN))
        {
            return raf.readInt();
        }
        else
        {
            return Integer.reverseBytes(raf.readInt());
        }
    }

    @Override
    public void close() throws IOException
    {
        this.raf.close();
    }

    @Override
    public String getPath()
    {
        return path;
    }

    @Override
    public String getPathUri()
    {
        return "file://" + path;
    }

    /**
     * Get the last domain in path.
     *
     * @return
     */
    @Override
    public String getName()
    {
        if (path == null)
        {
            return null;
        }
        int slash = path.lastIndexOf("/");
        return path.substring(slash + 1);
    }

    /**
     * For local file, block id is also the file id.
     * @return
     * @throws IOException
     */
    @Override
    public long getBlockId() throws IOException
    {
        return id;
    }

    /**
     * Get the scheme of the backed physical storage.
     *
     * @return
     */
    @Override
    public Storage.Scheme getStorageScheme()
    {
        return local.getScheme();
    }

    @Override
    public int getNumReadRequests()
    {
        // Issue #403: no need to count the read requests on local fs, always return 0.
        return 0;
    }
}
