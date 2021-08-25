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

/**
 * @author guodong
 */
public interface PhysicalReader
        extends Closeable
{
    long getFileLength() throws IOException;

    void seek(long desired) throws IOException;

    int read(byte[] buffer) throws IOException;

    int read(byte[] buffer, int offset, int length) throws IOException;

    void readFully(byte[] buffer) throws IOException;

    void readFully(byte[] buffer, int offset, int length) throws IOException;

    long readLong() throws IOException;

    int readInt() throws IOException;

    void close() throws IOException;

    String getPath();

    /**
     * Get the last domain in path.
     * @return
     */
    String getName();

    long getCurrentBlockId() throws IOException;
}
