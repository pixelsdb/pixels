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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.hive.mapreduce;

import io.pixelsdb.pixels.hive.PixelsSerDe.PixelsRow;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Pixels output format for new MapReduce OutputFormat API.
 * This class is not finished.
 * Created at: 19-6-30
 * Author: hank
 */
public class PixelsOutputFormat extends FileOutputFormat<NullWritable, PixelsRow>
{
    @Override
    public RecordWriter<NullWritable, PixelsRow> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException
    {
        return null;
    }
}
