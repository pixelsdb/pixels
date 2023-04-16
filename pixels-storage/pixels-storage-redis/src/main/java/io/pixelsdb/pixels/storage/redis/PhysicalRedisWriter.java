/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.storage.redis;

import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.Constants;
import redis.clients.jedis.JedisPooled;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * The physical writers for AWS S3 compatible storage systems.
 *
 * @author hank
 * @create 2022-08-22
 */
public class PhysicalRedisWriter implements PhysicalWriter
{
    private Redis redis;
    private byte[] path;
    private long position;
    private JedisPooled jedis;
    private OutputStream out;

    public PhysicalRedisWriter(Storage storage, String path, boolean overwrite) throws IOException
    {
        if (storage instanceof Redis)
        {
            this.redis = (Redis) storage;
        }
        else
        {
            throw new IOException("Storage is not Redis.");
        }
        if (path.contains("://"))
        {
            // remove the scheme.
            path = path.substring(path.indexOf("://") + 3);
        }
        this.path = path.getBytes(StandardCharsets.UTF_8);
        this.position = 0L;
        this.jedis = this.redis.getJedis();
        this.out = this.redis.create(path, overwrite, Constants.REDIS_BUFFER_SIZE);
    }

    @Override
    public long prepare(int length) throws IOException
    {
        return position;
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
        long start = position;
        this.out.write(buffer, offset, length);
        position += length;
        return start;
    }

    @Override
    public void close() throws IOException
    {
        this.out.close();
        // Don't close the client as it is external.
    }

    @Override
    public void flush() throws IOException
    {
        this.out.flush();
    }

    @Override
    public String getPath()
    {
        return new String(this.path, StandardCharsets.UTF_8);
    }

    @Override
    public int getBufferSize()
    {
        return Constants.REDIS_BUFFER_SIZE;
    }
}
