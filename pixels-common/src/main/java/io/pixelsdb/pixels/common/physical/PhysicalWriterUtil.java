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

import io.pixelsdb.pixels.common.physical.impl.PhysicalHDFSWriter;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author guodong
 */
public class PhysicalWriterUtil
{
    private PhysicalWriterUtil()
    {
    }

    public static PhysicalWriter newPhysicalWriter(Storage storage, String path, long blockSize,
                                                       short replication, boolean addBlockPadding) throws IOException
    {
        checkArgument(storage != null, "storage should not be null");
        checkArgument(path != null, "path should not be null");
        try
        {
            if (storage.getScheme().equals(Storage.Scheme.hdfs))
            {
                return new PhysicalHDFSWriter(storage, path, replication, addBlockPadding, blockSize);
            }
        } catch (IOException e)
        {
            e.printStackTrace();
            throw e;
        }

        return null;
    }

    /**
     * Get a physical file system writer.
     *
     * @param scheme          name of the scheme
     * @param path            write file path
     * @param replication     hdfs block replication num
     * @param addBlockPadding add block padding or not
     * @param blockSize       hdfs block size
     * @return physical writer
     */
    public static PhysicalWriter newPhysicalWriter(
            Storage.Scheme scheme, String path, short replication, boolean addBlockPadding, long blockSize) throws IOException
    {
        checkArgument(scheme != null, "scheme should not be null");
        checkArgument(path != null, "path should not be null");

        try
        {
            if (scheme == Storage.Scheme.hdfs)
            {
                return new PhysicalHDFSWriter(StorageFactory.Instance().
                        getStorage(scheme), path, replication, addBlockPadding, blockSize);
            }
        } catch (IOException e)
        {
            e.printStackTrace();
            throw e;
        }

        return null;
    }
}
