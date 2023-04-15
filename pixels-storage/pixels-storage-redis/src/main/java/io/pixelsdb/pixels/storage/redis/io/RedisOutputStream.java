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
package io.pixelsdb.pixels.storage.redis.io;

import redis.clients.jedis.JedisPooled;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static io.pixelsdb.pixels.common.utils.Constants.REDIS_BUFFER_SIZE;

/**
 * The output stream for Redis.
 *
 * @author hank
 * @date 8/22/22
 */
public class RedisOutputStream extends OutputStream
{
    private final JedisPooled jedis;

    /**
     * The path (key) name in redis
     */
    private final byte[] path;

    /**
     * The temporary buffer used for storing the chunks
     */
    private final byte[] buffer;

    /**
     * The position in the buffer
     */
    private int position;

    /**
     * indicates whether the stream is still open / valid
     */
    private boolean open;

    public RedisOutputStream(JedisPooled jedis, String path)
    {
        this(jedis, path, REDIS_BUFFER_SIZE);
    }

    public RedisOutputStream(JedisPooled jedis, String path, int bufferSize)
    {
        this.jedis = jedis;
        this.path = path.getBytes(StandardCharsets.UTF_8);
        this.buffer = new byte[bufferSize];
        this.position = 0;
        this.open = true;
    }

    @Override
    public void write(int b) throws IOException
    {
        this.assertOpen();
        if (position >= this.buffer.length)
        {
            flushBufferAndRewind();
        }
        this.buffer[position++] = (byte) b;
    }

    /**
     * Write an array to the Redis output stream.
     *
     * @param b the byte-array to append
     */
    @Override
    public void write(byte[] b) throws IOException
    {
        write(b, 0, b.length);
    }

    /**
     * Writes an array to the Redis Output Stream
     *
     * @param buf the array to write
     * @param off the offset into the array
     * @param len the number of bytes to write
     */
    @Override
    public void write(final byte[] buf, final int off, final int len) throws IOException
    {
        this.assertOpen();
        int offsetInBuf = off, remainToRead = len;
        int remainInBuffer;
        while (remainToRead > (remainInBuffer = this.buffer.length - position))
        {
            System.arraycopy(buf, offsetInBuf, this.buffer, this.position, remainInBuffer);
            this.position += remainInBuffer;
            flushBufferAndRewind();
            offsetInBuf += remainInBuffer;
            remainToRead -= remainInBuffer;
        }
        System.arraycopy(buf, offsetInBuf, this.buffer, this.position, remainToRead);
        this.position += remainToRead;
    }

    /**
     * Flushes the buffer by uploading a part to S3.
     */
    @Override
    public synchronized void flush()
    {
        this.assertOpen();
    }

    protected void flushBufferAndRewind()
    {
        byte[] buf = this.buffer;
        if (this.position < this.buffer.length)
        {
            buf = new byte[this.position];
            System.arraycopy(this.buffer, 0, buf, 0, this.position);
        }
        this.jedis.append(this.path, buf);
        this.position = 0;
    }

    @Override
    public void close() throws IOException
    {
        if (this.open)
        {
            if (this.position > 0)
            {
                flushBufferAndRewind();
            }
            this.open = false;
        }
    }

    private void assertOpen()
    {
        if (!this.open)
        {
            throw new IllegalStateException("Closed");
        }
    }

}
