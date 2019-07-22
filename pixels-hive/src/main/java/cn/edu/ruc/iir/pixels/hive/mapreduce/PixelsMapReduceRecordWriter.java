package cn.edu.ruc.iir.pixels.hive.mapreduce;

import cn.edu.ruc.iir.pixels.hive.PixelsSerDe.PixelsRow;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * This class is not finished, so that write is not supported by pixels-hive.
 * Created at: 19-6-30
 * Author: hank
 */
public class PixelsMapReduceRecordWriter extends RecordWriter<NullWritable, PixelsRow>
{
    /**
     * Writes a key/value pair.
     *
     * @param key   the key to write.
     * @param value the value to write.
     * @throws IOException
     */
    @Override
    public void write(NullWritable key, PixelsRow value) throws IOException, InterruptedException
    {

    }

    /**
     * Close this <code>RecordWriter</code> to future operations.
     *
     * @param context the context of the task
     * @throws IOException
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException
    {

    }
}
