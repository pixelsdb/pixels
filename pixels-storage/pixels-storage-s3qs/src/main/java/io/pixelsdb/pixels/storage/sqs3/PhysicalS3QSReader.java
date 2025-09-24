/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.storage.sqs3;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.Storage;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class PhysicalS3QSReader implements PhysicalReader
{
    private final S3QS s3qs;
    private final String path;
    private final DataInputStream dataInputStream;

    public PhysicalS3QSReader(Storage storage, String path) throws IOException
    {
        if (storage instanceof S3QS)
        {
            this.s3qs = (S3QS) storage;
        }
        else
        {
            throw new IOException("Storage is not S3QS.");
        }
        this.path = path;
        this.dataInputStream = this.s3qs.open(path);
    }

    @Override
    public long getFileLength() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void seek(long desired) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer readFully(int length) throws IOException
    {
        byte[] buffer = new byte[length];
        dataInputStream.readFully(buffer);
        return ByteBuffer.wrap(buffer);
    }

    @Override
    public void readFully(byte[] buffer) throws IOException
    {
        dataInputStream.readFully(buffer);
    }

    @Override
    public void readFully(byte[] buffer, int offset, int length) throws IOException
    {
        dataInputStream.readFully(buffer, offset, length);
    }

    @Override
    public boolean supportsAsync()
    {
        return false;
    }

    @Override
    public CompletableFuture<ByteBuffer> readAsync(long offset, int length) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long readLong(ByteOrder byteOrder) throws IOException
    {
        if (requireNonNull(byteOrder).equals(ByteOrder.BIG_ENDIAN))
        {
            return dataInputStream.readLong();
        }
        else
        {
            return Long.reverseBytes(dataInputStream.readLong());
        }
    }

    @Override
    public int readInt(ByteOrder byteOrder) throws IOException
    {
        if (requireNonNull(byteOrder).equals(ByteOrder.BIG_ENDIAN))
        {
            return dataInputStream.readInt();
        }
        else
        {
            return Integer.reverseBytes(dataInputStream.readInt());
        }
    }

    @Override
    public void close() throws IOException
    {
        this.dataInputStream.close();
    }

    @Override
    public String getPath()
    {
        return this.path;
    }

    @Override
    public String getPathUri() throws IOException
    {
        return this.s3qs.ensureSchemePrefix(path);
    }

    /**
     * @return the port in path
     */
    @Override
    public String getName()
    {
        if (path == null)
        {
            return null;
        }
        int slash = path.lastIndexOf(":");
        return path.substring(slash + 1);
    }

    @Override
    public long getBlockId() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Storage.Scheme getStorageScheme()
    {
        return this.s3qs.getScheme();
    }

    /**
     * @return always 0
     */
    @Override
    public int getNumReadRequests()
    {
        return 0;
    }
}
