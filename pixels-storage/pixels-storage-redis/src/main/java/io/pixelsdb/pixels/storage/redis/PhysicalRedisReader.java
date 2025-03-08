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

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.Storage;
import redis.clients.jedis.JedisPooled;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/**
 * The physical reader for Redis.
 *
 * @author hank
 * @create 2022-08-22
 */
public class PhysicalRedisReader implements PhysicalReader
{
    private final Redis redis;
    private final JedisPooled jedis;
    private final byte[] path;
    private final long id;
    private final long length;
    private final AtomicLong position;
    private final AtomicInteger numRequests;

    public PhysicalRedisReader(Storage storage, String path) throws IOException
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
        this.id = this.redis.getFileId(path);
        this.jedis = this.redis.getJedis();
        this.length = this.jedis.strlen(path);
        this.position = new AtomicLong(0);
        this.numRequests = new AtomicInteger(1);
    }

    @Override
    public long getFileLength() throws IOException
    {
        return this.length;
    }

    @Override
    public void seek(long desired) throws IOException
    {
        if (0 <= desired && desired < length)
        {
            position.set(desired);
            return;
        }
        throw new IOException("Desired offset " + desired +
                " is out of bound (" + 0 + "," + length + ")");
    }

    @Override
    public ByteBuffer readFully(int len) throws IOException
    {
        long pos = this.position.get();
        if (pos + len > this.length)
        {
            throw new IOException("Current position " + pos + " plus " +
                    len + " exceeds object length " + this.length + ".");
        }
        try
        {
            byte[] value = this.jedis.getrange(this.path, pos, pos + len);
            this.numRequests.incrementAndGet();
            this.position.addAndGet(len);
            return ByteBuffer.wrap(value);
        } catch (Exception e)
        {
            throw new IOException("Failed to read object.", e);
        }
    }

    @Override
    public void readFully(byte[] buffer) throws IOException
    {
        ByteBuffer byteBuffer = readFully(buffer.length);
        System.arraycopy(byteBuffer.array(), 0, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(byte[] buffer, int off, int len) throws IOException
    {
        ByteBuffer byteBuffer = readFully(len);
        System.arraycopy(byteBuffer.array(), 0, buffer, off, len);
    }

    @Override
    public long readLong(ByteOrder byteOrder) throws IOException
    {
        ByteBuffer buffer = readFully(Long.BYTES).order(requireNonNull(byteOrder));
        return buffer.getLong();
    }

    @Override
    public int readInt(ByteOrder byteOrder) throws IOException
    {
        ByteBuffer buffer = readFully(Integer.BYTES).order(requireNonNull(byteOrder));
        return buffer.getInt();
    }

    @Override
    public void close() throws IOException
    {
        // Should not close the jedis client because it is shared by all threads.
    }

    @Override
    public String getPath()
    {
        return new String(this.path, StandardCharsets.UTF_8);
    }

    @Override
    public String getPathUri()
    {
        return "redis://" + getPath();
    }

    @Override
    public String getName()
    {
        return new String(this.path, StandardCharsets.UTF_8);
    }

    @Override
    public long getBlockId()
    {
        return this.id;
    }

    @Override
    public Storage.Scheme getStorageScheme()
    {
        return this.redis.getScheme();
    }

    @Override
    public int getNumReadRequests()
    {
        return this.numRequests.get();
    }
}
