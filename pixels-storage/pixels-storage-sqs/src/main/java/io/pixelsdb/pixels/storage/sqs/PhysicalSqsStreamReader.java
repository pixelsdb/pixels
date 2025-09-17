/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.storage.sqs;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.Storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CompletableFuture;

public class PhysicalSqsStreamReader implements PhysicalReader
{
    @Override
    public long getFileLength() throws IOException
    {
        return 0;
    }

    @Override
    public void seek(long desired) throws IOException
    {

    }

    @Override
    public ByteBuffer readFully(int length) throws IOException
    {
        return null;
    }

    @Override
    public void readFully(byte[] buffer) throws IOException
    {

    }

    @Override
    public void readFully(byte[] buffer, int offset, int length) throws IOException
    {

    }

    @Override
    public boolean supportsAsync()
    {
        return PhysicalReader.super.supportsAsync();
    }

    @Override
    public CompletableFuture<ByteBuffer> readAsync(long offset, int length) throws IOException
    {
        return PhysicalReader.super.readAsync(offset, length);
    }

    @Override
    public long readLong(ByteOrder byteOrder) throws IOException
    {
        return 0;
    }

    @Override
    public int readInt(ByteOrder byteOrder) throws IOException
    {
        return 0;
    }

    @Override
    public void close() throws IOException
    {

    }

    @Override
    public String getPath()
    {
        return "";
    }

    @Override
    public String getPathUri() throws IOException
    {
        return "";
    }

    @Override
    public String getName()
    {
        return "";
    }

    @Override
    public long getBlockId() throws IOException
    {
        return 0;
    }

    @Override
    public Storage.Scheme getStorageScheme()
    {
        return null;
    }

    @Override
    public int getNumReadRequests()
    {
        return 0;
    }
}
