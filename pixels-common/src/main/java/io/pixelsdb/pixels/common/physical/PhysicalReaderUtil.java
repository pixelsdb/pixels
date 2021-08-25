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

    public static PhysicalReader newPhysicalReader(String scheme, String path)
    {
        checkArgument(scheme != null, "scheme should not be null");
        checkArgument(path != null, "path should not be null");
        if (scheme.equalsIgnoreCase("hdfs"))
        {
            PhysicalFSReader fsReader;
            try
            {
                fsReader = new PhysicalFSReader(StorageFactory.Instance().getStorage(scheme), path);
                return fsReader;
            } catch (IOException ioException)
            {
                ioException.printStackTrace();
            }
        }

        return null;
    }

    public static PhysicalReader newPhysicalReader(Storage storage, String path)
    {
        checkArgument(storage != null, "storage should not be null");
        checkArgument(path != null, "path should not be null");
        PhysicalFSReader fsReader = null;
        try
        {
            fsReader = new PhysicalFSReader(storage, path);
        } catch (IOException ioException)
        {
            ioException.printStackTrace();
        }

        return fsReader;
    }
}
