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
package io.pixelsdb.pixels.storage.s3;

import io.pixelsdb.pixels.common.physical.ObjectPath;
import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.Constants;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * The physical writers for AWS S3 compatible storage systems.
 *
 * @author hank
 * @create 2021-09-06
 */
public class PhysicalS3Writer implements PhysicalWriter
{
    private AbstractS3 s3;
    private ObjectPath path;
    private final String pathStr;
    private long position;
    private S3Client client;
    private final OutputStream out;

    public PhysicalS3Writer(Storage storage, String path, boolean overwrite) throws IOException
    {
        if (storage instanceof AbstractS3)
        {
            this.s3 = (AbstractS3) storage;
        }
        else
        {
            throw new IOException("Storage is not S3.");
        }
        if (path.contains("://"))
        {
            // remove the scheme.
            path = path.substring(path.indexOf("://") + 3);
        }
        this.path = new ObjectPath(path);
        this.pathStr = path;
        this.position = 0L;
        this.client = s3.getClient();
        this.out = this.s3.create(path, overwrite, Constants.S3_BUFFER_SIZE);
    }

    /**
     * Prepare the writer to ensure the length can fit into current block.
     *
     * @param length length of content.
     * @return starting offset after preparing. If -1, means prepare has failed,
     * due to the specified length cannot fit into current block.
     */
    @Override
    public long prepare(int length) throws IOException
    {
        return position;
    }

    /**
     * Append content to the file.
     *
     * @param buffer content buffer.
     * @return start offset of content in the file.
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
        long start = position;
        this.out.write(buffer, offset, length);
        position += length;
        return start;
    }

    /**
     * Close writer.
     */
    @Override
    public void close() throws IOException
    {
        this.out.close();
        // Don't close the client as it is external.
        // this.client.close();
    }

    /**
     * Flush writer.
     */
    @Override
    public void flush() throws IOException
    {
        this.out.flush();
    }

    @Override
    public String getPath()
    {
        return pathStr;
    }

    @Override
    public int getBufferSize()
    {
        return Constants.S3_BUFFER_SIZE;
    }
}
