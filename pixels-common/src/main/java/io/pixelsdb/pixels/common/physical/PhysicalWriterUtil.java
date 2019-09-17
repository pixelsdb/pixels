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

import io.pixelsdb.pixels.common.utils.Constants;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author guodong
 */
public class PhysicalWriterUtil
{
    private PhysicalWriterUtil()
    {
    }

    /**
     * Get a physical file system writer.
     *
     * @param fs              file system
     * @param path            write file path
     * @param blockSize       hdfs block size
     * @param replication     hdfs block replication num
     * @param addBlockPadding add block padding or not
     * @return physical writer
     */
    public static PhysicalFSWriter newPhysicalFSWriter(
            FileSystem fs, Path path, long blockSize, short replication, boolean addBlockPadding)
    {
        FSDataOutputStream rawWriter = null;
        try
        {
            rawWriter = fs.create(path, false, Constants.HDFS_BUFFER_SIZE, replication, blockSize);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        if (rawWriter == null)
        {
            return null;
        }
        return new PhysicalFSWriter(fs, path, blockSize, replication, addBlockPadding, rawWriter);
    }
}
