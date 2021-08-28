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

import io.pixelsdb.pixels.common.physical.impl.PhysicalHDFSReader;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author guodong
 */
public class PhysicalReaderUtil
{
    private PhysicalReaderUtil()
    {
    }

    public static PhysicalReader newPhysicalReader(Storage storage, String path) throws IOException
    {
        checkArgument(storage != null, "storage should not be null");
        checkArgument(path != null, "path should not be null");
        PhysicalReader fsReader = null;
        try
        {
            if (storage.getScheme().equals(Storage.Scheme.hdfs))
            {
                fsReader = new PhysicalHDFSReader(storage, path);
            }
        } catch (IOException e)
        {
            e.printStackTrace();
            throw e;
        }

        return fsReader;
    }

    public static PhysicalReader newPhysicalReader(Storage.Scheme scheme, String path) throws IOException
    {
        checkArgument(scheme != null, "scheme should not be null");
        checkArgument(path != null, "path should not be null");
        PhysicalReader fsReader;
        try
        {
            if (scheme == Storage.Scheme.hdfs)
            {
                fsReader = new PhysicalHDFSReader(StorageFactory.Instance().getStorage(scheme), path);
                return fsReader;
            }
        } catch (IOException e)
        {
            e.printStackTrace();
            throw e;
        }

        return null;
    }
}
