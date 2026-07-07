/*
 * Copyright 2026 PixelsDB.
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

import io.pixelsdb.pixels.common.physical.Storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * The physical reader for COS.
 * 
 * @author Dongyang Geng
 * @create 2026-07-07
 */
public class PhysicalCosReader extends AbstractS3Reader 
{
    public PhysicalCosReader(Storage storage, String path) throws IOException
    {
        super(storage, path);
        this.enableAsync = false;
    }

    @Override
    public CompletableFuture<ByteBuffer> readAsync(long offset, int len) throws IOException
    {
        throw new UnsupportedOperationException("asynchronous read is not supported for COS.");
    }

    @Override
    public void close() throws IOException
    {
        // Should not close the client because it is shared by all threads.
        // this.client.close(); // Closing s3 client may take several seconds.
    }
}
