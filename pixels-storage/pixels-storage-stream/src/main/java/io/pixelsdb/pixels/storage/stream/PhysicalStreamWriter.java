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
package io.pixelsdb.pixels.storage.stream;

import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.Constants;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author huasiy
 * @create 2024-11-08
 */
public class PhysicalStreamWriter implements PhysicalWriter
{
    private Stream stream;
    private String path;
    private long position;
    private DataOutputStream dataOutputStream;

    public PhysicalStreamWriter(Storage stream, String path) throws IOException
    {
        if (stream instanceof Stream)
        {
            this.stream = (Stream) stream;
        }
        else
        {
            throw new IOException("Storage is not stream");
        }
        this.path = path;
        this.dataOutputStream = stream.create(path, false, Constants.STREAM_BUFFER_SIZE);
    }

    /**
     * Tell the writer the offset of next write.
     *
     * @param length length of content
     * @return starting offset after preparing.
     */
    @Override
    public long prepare(int length) throws IOException
    {
        return this.position;
    }

    /**
     * Append content to the file.
     *
     * @param buffer content buffer
     * @return start offset of content in the file.
     */
    @Override
    public long append(ByteBuffer buffer) throws IOException
    {
        buffer.flip();
        int length = buffer.remaining();
        return append(buffer.array(), buffer.arrayOffset() + buffer.position(), length);
    }

    /**
     * Append content to the file.
     *
     * @param buffer content buffer container
     * @param offset start offset of actual content buffer
     * @param length length of actual content buffer
     * @return start offset of content in the file.
     */
    @Override
    public long append(byte[] buffer, int offset, int length) throws IOException
    {
        long start = this.position;
        dataOutputStream.write(buffer, offset, length);
        position += length;
        return start;
    }

    @Override
    public void close() throws IOException
    {
        dataOutputStream.close();
    }

    @Override
    public void flush() throws IOException
    {
        dataOutputStream.flush();
    }

    @Override
    public String getPath() { return path; }

    @Override
    public int getBufferSize() { return Constants.STREAM_BUFFER_SIZE; }
}
