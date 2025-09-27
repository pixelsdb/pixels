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

import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.Constants;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * @author hank
 * @create 2025-09-17
 */
public class PhysicalS3QSWriter implements PhysicalWriter
{
    private final String pathStr;
    private long position;
    private final DataOutputStream out;
    private S3Queue queue = null;

    public PhysicalS3QSWriter(Storage storage, String path) throws IOException
    {
        if (storage instanceof S3QS)
        {
            if (path.contains("://"))
            {
                // remove the scheme.
                path = path.substring(path.indexOf("://") + 3);
            }
            this.pathStr = path;
            this.position = 0L;
            this.out = storage.create(path, false, Constants.S3QS_BUFFER_SIZE);
        }
        else
        {
            throw new IOException("storage is not S3QS");
        }
    }

    protected void setQueue(S3Queue queue)
    {
        this.queue = queue;
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
        /**
         * Issue #217:
         * For compatibility reasons if this code is compiled by jdk>=9 but executed in jvm8.
         *
         * In jdk8, ByteBuffer.flip() is extended from Buffer.flip(), but in jdk11, different kind of ByteBuffer
         * has its own flip implementation and may lead to errors.
         */
        ((Buffer)buffer).flip();
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
        this.out.write(buffer, offset, length);
        this.position += length;
        return start;
    }

    @Override
    public void close() throws IOException
    {
        this.out.close();
        if (this.queue != null && !this.queue.isClosed())
        {
            this.queue.push(this.pathStr);
        }
    }

    @Override
    public void flush() throws IOException
    {
        this.out.flush();
    }

    @Override
    public String getPath()
    {
        return this.pathStr;
    }

    @Override
    public int getBufferSize()
    {
        return Constants.S3QS_BUFFER_SIZE;
    }
}
