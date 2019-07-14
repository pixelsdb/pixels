package cn.edu.ruc.iir.pixels.hive.mapreduce;

import cn.edu.ruc.iir.pixels.hive.PixelsSerDe.PixelsRow;
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
