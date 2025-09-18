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
package io.pixelsdb.pixels.storage.sqs.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;

public class SqsInputStream extends InputStream
{
    private static final Logger logger = LogManager.getLogger(SqsInputStream.class);

    @Override
    public int read() throws IOException
    {
        return 0;
    }

    @Override
    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
    }

    /**
     * Attempt to read data with a maximum length of len into the position off of the buffer.
     * @param buf the buffer
     * @param off the position in buffer
     * @param len the length in bytes to read
     * @return actual number of bytes read
     * @throws IOException
     */
    @Override
    public int read(byte[] buf, int off, int len) throws IOException
    {
        return 0;
    }

    @Override
    public void close() throws IOException
    {

    }
}
