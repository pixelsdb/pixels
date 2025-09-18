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
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.Constants;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author hank
 * @create 2025-09-17
 */
public class PhysicalSqsStreamWriter implements PhysicalWriter
{
    private final String path;
    private long position;
    private final DataOutputStream dataOutputStream;

    public PhysicalSqsStreamWriter(Storage storage, String path) throws IOException
    {
        if (storage instanceof SqsStream)
        {
            this.path = path;
            this.dataOutputStream = storage.create(path, false, Constants.SQS_STREAM_BUFFER_SIZE);
        }
        else
        {
            throw new IOException("storage is not SqsStream");
        }
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
     * Append content to the stream.
     *
     * @param buffer content buffer
     * @return start offset of content in the stream.
     */
    @Override
    public long append(ByteBuffer buffer) throws IOException
    {
        buffer.flip();
        int length = buffer.remaining();
        return append(buffer.array(), buffer.arrayOffset() + buffer.position(), length);
    }

    /**
     * Append content to the stream.
     *
     * @param buffer content buffer container
     * @param offset start offset of actual content buffer
     * @param length length of actual content buffer
     * @return start offset of content in the stream
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
    public String getPath()
    {
        return path;
    }

    @Override
    public int getBufferSize()
    {
        return Constants.SQS_STREAM_BUFFER_SIZE;
    }
}
