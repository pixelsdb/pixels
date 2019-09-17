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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author guodong
 */
public class PhysicalReaderUtil
{
    private PhysicalReaderUtil()
    {
    }

    public static PhysicalFSReader newPhysicalFSReader(FileSystem fs, Path path)
    {
        FSDataInputStream rawReader = null;
        try
        {
            rawReader = fs.open(path);
            if (rawReader != null)
            {
                return new PhysicalFSReader(fs, path, rawReader);
            }
        }
        catch (IOException e)
        {
            if (rawReader != null)
            {
                try
                {
                    rawReader.close();
                }
                catch (IOException e1)
                {
                    e1.printStackTrace();
                }
            }
            e.printStackTrace();
        }

        return null;
    }
}
