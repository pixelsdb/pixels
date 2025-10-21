/*
 * Copyright 2018 PixelsDB.
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
package io.pixelsdb.pixels.storage.hdfs;

import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.Constants;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * @author guodong
 * @author hank
 */
public class PhysicalHDFSWriter implements PhysicalWriter
{
    private static final Logger LOGGER = LogManager.getLogger(PhysicalHDFSWriter.class);

    private final HDFS hdfs;
    private final String path;
    private final long blockSize;
    private final short replication;
    private final boolean addBlockPadding;
    private final FSDataOutputStream rawWriter;

    public PhysicalHDFSWriter(Storage storage, String path, short replication,
                              boolean addBlockPadding, long blockSize, boolean overwrite) throws IOException
    {
        if (storage instanceof HDFS)
        {
            this.hdfs = (HDFS) storage;
        }
        else
        {
            throw new IOException("Storage is not HDFS.");
        }
        this.path = path;
        this.blockSize = blockSize;
        this.replication = replication;
        this.addBlockPadding = addBlockPadding;

        DataOutputStream dos = hdfs.create(path, overwrite, Constants.HDFS_BUFFER_SIZE, replication, blockSize);

        this.rawWriter = (FSDataOutputStream) dos;
    }

    @Override
    public long prepare(int length) throws IOException
    {
        if (length > blockSize)
        {
            return -1L;
        }
        // see if row group can fit in the current hdfs block, else pad the remaining space in the block
        long start = rawWriter.getPos();
        long availBlockSpace = blockSize - (start % blockSize);
        if (length < blockSize && length > availBlockSpace && addBlockPadding)
        {
            byte[] pad = new byte[(int) Math.min(Constants.HDFS_BUFFER_SIZE, availBlockSpace)];
            LOGGER.info(String.format("Padding Pixels by %d bytes while appending row group buffer...", availBlockSpace));
            start += availBlockSpace;
            while (availBlockSpace > 0)
            {
                int writeLen = (int) Math.min(availBlockSpace, pad.length);
                rawWriter.write(pad, 0, writeLen);
                availBlockSpace -= writeLen;
            }
        }
        return start;
    }

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

    @Override
    public long append(byte[] buffer, int offset, int length) throws IOException
    {
        long start = rawWriter.getPos();
        rawWriter.write(buffer, offset, length);
        return start;
    }

    @Override
    public void close() throws IOException
    {
        rawWriter.close();
    }

    @Override
    public void flush() throws IOException
    {
        rawWriter.flush();
    }

    @Override
    public String getPath()
    {
        return path;
    }

    @Override
    public int getBufferSize()
    {
        return Constants.HDFS_BUFFER_SIZE;
    }

    public long getBlockSize()
    {
        return blockSize;
    }

    public short getReplication()
    {
        return replication;
    }

    public boolean isAddBlockPadding()
    {
        return addBlockPadding;
    }
}
