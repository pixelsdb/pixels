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

import io.pixelsdb.pixels.common.physical.PhysicalWriter;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author huasiy
 * @create 2024-11-08
 */
public class PhysicalSqsStreamWriter implements PhysicalWriter
{
    @Override
    public long prepare(int length) throws IOException
    {
        return 0;
    }

    @Override
    public long append(ByteBuffer buffer) throws IOException
    {
        return 0;
    }

    @Override
    public long append(byte[] buffer, int offset, int length) throws IOException
    {
        return 0;
    }

    @Override
    public void close() throws IOException
    {

    }

    @Override
    public void flush() throws IOException
    {

    }

    @Override
    public String getPath()
    {
        return "";
    }

    @Override
    public int getBufferSize()
    {
        return 0;
    }
}
