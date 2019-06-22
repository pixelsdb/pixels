package cn.edu.ruc.iir.pixels.hive.mapreduce;

import cn.edu.ruc.iir.pixels.hive.PixelsStruct;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.List;

/**
 * Created at: 19-6-15
 * Author: hank
 */
public class PixelsInputFormat extends FileInputFormat<NullWritable, PixelsStruct>
{
    /**
     * Generate the list of files and make them into FileSplits.
     *
     * @param job the job context
     * @throws IOException
     */
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException
    {
        return super.getSplits(job);
    }

    /**
     * Create a record reader for a given split. The framework will call
     * {@link RecordReader#initialize(InputSplit, TaskAttemptContext)} before
     * the split is used.
     *
     * @param split   the split to be read
     * @param context the information about the task
     * @return a new record reader
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
    {
        Configuration conf = ShimLoader.getHadoopShims()
                .getConfiguration(context);
        return null;
    }
}
