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
import io.pixelsdb.pixels.common.physical.direct.DirectRandomAccessFile;
import io.pixelsdb.pixels.common.physical.storage.LocalFS;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created at: 30/08/2021
 * Author: hank
 */
public class PhysicalLocalReader implements PhysicalReader
{
    private final LocalFS local;
    private final String path;
    private final long id;
    private final DirectRandomAccessFile raf;
    private final AtomicInteger numRequests;

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
        this.numRequests = new AtomicInteger(1);
    }

    @Override
    public long getFileLength() throws IOException
    {
        numRequests.incrementAndGet();
        return raf.length();
    }

    @Override
    public void seek(long desired) throws IOException
    {
        numRequests.incrementAndGet();
        raf.seek(desired);
    }

    @Override
    public ByteBuffer readFully(int length) throws IOException
    {
        numRequests.incrementAndGet();
        return raf.readFully(length);
    }

    @Override
    public void readFully(byte[] buffer) throws IOException
    {
        raf.readFully(buffer);
        numRequests.incrementAndGet();
    }

    @Override
    public void readFully(byte[] buffer, int offset, int length) throws IOException
    {
        raf.readFully(buffer, offset, length);
        numRequests.incrementAndGet();
    }

    /**
     * @return true if readDirect is supported.
     */
    @Override
    public boolean supportsDirect()
    {
        return true;
    }

    @Override
    public long readLong() throws IOException
    {
        numRequests.incrementAndGet();
        return raf.readLong();
    }

    @Override
    public int readInt() throws IOException
    {
        numRequests.incrementAndGet();
        return raf.readInt();
    }

    @Override
    public void close() throws IOException
    {
        numRequests.incrementAndGet();
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

    @Override
    public int getNumReadRequests()
    {
        return numRequests.get();
    }
}
