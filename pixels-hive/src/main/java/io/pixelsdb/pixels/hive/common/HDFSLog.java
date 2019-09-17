/*
 * Copyright 2019 PixelsDB.
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
package io.pixelsdb.pixels.hive.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * Created at: 19-7-1
 * Author: hank
 */
public class HDFSLog
{
    public static BufferedWriter getLogWriter(Configuration conf, String path) throws IOException
    {
        FileSystem fs = FileSystem.get(conf);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(path))));
        return writer;
    }

    public static BufferedWriter getLogWriter(FileSystem fs, String path) throws IOException
    {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(path))));
        return writer;
    }
}
