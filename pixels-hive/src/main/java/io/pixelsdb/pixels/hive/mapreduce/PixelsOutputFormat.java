package io.pixelsdb.pixels.hive.mapreduce;

import io.pixelsdb.pixels.hive.PixelsSerDe;
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
public class PixelsOutputFormat extends FileOutputFormat<NullWritable, PixelsSerDe.PixelsRow>
{
    @Override
    public RecordWriter<NullWritable, PixelsSerDe.PixelsRow> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException
    {
        return null;
    }
}
