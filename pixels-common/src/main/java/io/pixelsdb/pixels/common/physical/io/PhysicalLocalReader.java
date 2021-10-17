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
import io.pixelsdb.pixels.common.physical.storage.LocalFS;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Created at: 30/08/2021
 * Author: hank
 */
public class PhysicalLocalReader implements PhysicalReader
{
    private LocalFS local;
    private String path;
    private long id;
    private RandomAccessFile raf;

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
        ByteBuffer buffer = ByteBuffer.allocate(length);
        raf.readFully(buffer.array());
        return buffer;
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

    /**
     * @return true if readAsync is supported.
     */
    @Override
    public boolean supportsAsync()
    {
        return false;
    }

    @Override
    public CompletableFuture<ByteBuffer> readAsync(long offset, int length) throws IOException
    {
        throw new IOException("Asynchronous read is not supported for local fs.");
    }

    @Override
    public long readLong() throws IOException
    {
        return raf.readLong();
    }

    @Override
    public int readInt() throws IOException
    {
        return raf.readInt();
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
}
