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
package io.pixelsdb.pixels.core.utils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;


/**
 * This class is derived from org.apache.hadoop.io.Text.
 * We removed the charset decoding thus only support utf8.
 * <p>
 * Created at: 26/03/2022
 * @author: hank
 */
public class Text
{
    private static final byte[] EMPTY_BYTES = new byte[0];

    private byte[] bytes;
    private int length;

    public Text()
    {
        bytes = EMPTY_BYTES;
    }

    /**
     * Construct from another text.
     */
    public Text(Text utf8)
    {
        set(utf8);
    }

    /**
     * Construct from a byte array.
     */
    public Text(byte[] utf8)
    {
        set(utf8);
    }

    /**
     * Set to a utf8 byte array
     */
    public void set(byte[] utf8)
    {
        set(utf8, 0, utf8.length);
    }

    /**
     * copy a text.
     */
    public void set(Text other)
    {
        set(other.getBytes(), 0, other.getLength());
    }

    /**
     * Set the Text to range of bytes
     *
     * @param utf8  the data to copy from
     * @param start the first position of the new string
     * @param len   the number of bytes of the new string
     */
    public void set(byte[] utf8, int start, int len)
    {
        setCapacity(len, false);
        System.arraycopy(utf8, start, bytes, 0, len);
        this.length = len;
    }

    public byte[] getBytes()
    {
        return bytes;
    }

    public int getLength()
    {
        return length;
    }

    /**
     * Sets the capacity of this Text object to <em>at least</em>
     * <code>len</code> bytes. If the current buffer is longer,
     * then the capacity and existing content of the buffer are
     * unchanged. If <code>len</code> is larger
     * than the current capacity, the Text object's capacity is
     * increased to match.
     *
     * @param len      the number of bytes we need
     * @param keepData should the old data be kept
     */
    private void setCapacity(int len, boolean keepData)
    {
        if (bytes == null || bytes.length < len)
        {
            if (bytes != null && keepData)
            {
                bytes = Arrays.copyOf(bytes, Math.max(len, length << 1));
            } else
            {
                bytes = new byte[len];
            }
        }
    }

    public void append(byte[] utf8, int start, int len)
    {
        setCapacity(length + len, true);
        System.arraycopy(utf8, start, bytes, length, len);
        length += len;
    }

    /**
     * Clear the string to empty.
     *
     * <em>Note</em>: For performance reasons, this call does not clear the
     * underlying byte array that is retrievable via {@link #getBytes()}.
     * In order to free the byte-array memory, call {@link #set(byte[])}
     * with an empty byte array (For example, <code>new byte[0]</code>).
     */
    public void clear()
    {
        length = 0;
    }

    /**
     * Get the string value of the underlying byte array, using UTF8 charset.
     * @return
     */
    @Override
    public String toString()
    {
        return new String(bytes, 0, length, StandardCharsets.UTF_8);
    }
}
