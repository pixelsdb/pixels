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
package io.pixelsdb.pixels.common.physical;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CompletableFuture;

/**
 * Note that PhysicalReader should not be shared by multiple threads.
 * @author guodong
 * @author hank
 */
public interface PhysicalReader extends Closeable
{
    long getFileLength() throws IOException;

    /**
     * Set the current offset to the desired value.
     * @param desired the desired offset
     * @throws IOException
     */
    void seek(long desired) throws IOException;

    /**
     * Read a byte buffer of the given length from the current offset. The byte order
     * of the byte buffer is default byte order of {@link ByteBuffer}. Please check and
     * reset it if necessary.
     * @param length the number of bytes to read
     * @return the byte buffer been read
     * @throws IOException
     */
    ByteBuffer readFully(int length) throws IOException;

    void readFully(byte[] buffer) throws IOException;

    void readFully(byte[] buffer, int offset, int length) throws IOException;

    /**
     * @return true if readAsync is supported.
     */
    default boolean supportsAsync()
    {
        return false;
    }

    /**
     * readAsync does not affect the position of this reader, and is not affected by seek().
     * @param offset
     * @param length
     * @return
     * @throws IOException
     */
    default CompletableFuture<ByteBuffer> readAsync(long offset, int length) throws IOException
    {
        throw new UnsupportedOperationException("asynchronous read is not supported for " + getStorageScheme().name());
    }

    /**
     * Read an eight-byte signed integer from the current offset using the specified byte order.
     * @param byteOrder the byte order
     * @return the integer been read
     * @throws IOException
     */
    long readLong(ByteOrder byteOrder) throws IOException;

    /**
     * Read a four-byte signed integer from the current offset using the specified byte order.
     * @param byteOrder the byte order
     * @return the integer been read
     * @throws IOException
     */
    int readInt(ByteOrder byteOrder) throws IOException;

    void close() throws IOException;

    String getPath();

    /**
     * Get the last domain in path.
     * @return
     */
    String getName();

    /**
     * For a file or object in the storage, it may have one or more
     * blocks. Each block has its unique id. This method returns the
     * block id of the current block that is being reading.
     *
     * For local fs, each file has only one block id, which is also
     * the file id.
     *
     * <p>Note: Storage.getFileId() assumes that each file or object
     * only has one block. In this case, the file id is the same as
     * the block id here.</p>
     * @return
     * @throws IOException
     */
    long getBlockId() throws IOException;

    /**
     * @return the scheme of the backed physical storage.
     */
    Storage.Scheme getStorageScheme();

    /**
     * @return the number of read requests sent to the storage.
     */
    int getNumReadRequests();
}
