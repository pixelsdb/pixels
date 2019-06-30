package cn.edu.ruc.iir.pixels.hive.mapreduce;

import cn.edu.ruc.iir.pixels.core.PixelsReader;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.reader.PixelsRecordReader;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import cn.edu.ruc.iir.pixels.hive.common.PixelsRW;
import cn.edu.ruc.iir.pixels.hive.common.PixelsStruct;
import cn.edu.ruc.iir.pixels.hive.common.PixelsValue;
import cn.edu.ruc.iir.pixels.hive.mapred.PixelsMapredRecordReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Created at: 19-6-30
 * Author: hank
 */
@SuppressWarnings("Duplicates")
public class PixelsMapReduceRecordReader extends RecordReader<NullWritable, PixelsStruct>
{
    private static Logger log = LogManager.getLogger(PixelsMapredRecordReader.class);

    private final int batchSize;
    private final TypeDescription schema;
    private final PixelsRecordReader batchReader;
    private VectorizedRowBatch batch;
    private int rowIdInBatch;
    private List<Integer> included;
    private List<TypeDescription> columnTypes;
    private int numColumns;
    private final PixelsReader fileReader;
    private final NullWritable currentKey;
    private PixelsStruct currentValue;

    public PixelsMapReduceRecordReader(PixelsReader fileReader,
                                       PixelsRW.ReaderOptions options) throws IOException
    {
        this.schema = fileReader.getFileSchema();
        // schema should be of struct type.
        assert schema.getCategory() == TypeDescription.Category.STRUCT;

        this.fileReader = fileReader;
        this.batchReader = fileReader.read(options.getReaderOption());
        this.columnTypes = schema.getChildren();
        this.numColumns = columnTypes.size();
        this.batchSize = options.getBatchSize();
        this.batch = null; // the first batch will be read in initialize.
        this.rowIdInBatch = 0;
        this.included = options.getIncluded();
        this.currentKey = NullWritable.get();
        this.currentValue = new PixelsStruct(this.numColumns);
    }

    /**
     * If the current batch is empty or exhausted, get a new one.
     *
     * @return true if we have rows available.
     * @throws IOException
     */
    boolean ensureBatch() throws IOException
    {
        if (batch == null || batch.size <= 0 ||
                batch.endOfFile || rowIdInBatch >= batch.size)
        {
            rowIdInBatch = 0;
            batch = batchReader.readBatch(batchSize);
            if (batch == null || this.batch.size <= 0 || this.batch.endOfFile)
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Called once at initialization. The first batch is read in this method.
     *
     * @param split   the split that defines the range of records to read
     * @param context the information about the task
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
    {
        ensureBatch();
    }

    /**
     * Read the next key, value pair.
     *
     * @return true if a key/value pair was read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
        if (!ensureBatch())
        {
            return false;
        }

        if (this.included.size() == 0)
        {
            rowIdInBatch += 1;
            return true;
        }

        int numberOfIncluded = this.included.size();
        for (int i = 0; i < numberOfIncluded; ++i)
        {
            currentValue.setFieldValue(included.get(i), PixelsValue.nextValue(batch.cols[i], rowIdInBatch,
                    columnTypes.get(included.get(i)), currentValue.getFieldValue(included.get(i))));
        }

        rowIdInBatch += 1;
        return true;
    }

    /**
     * Get the current key
     *
     * @return the current key or null if there is no current key
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException
    {
        return currentKey;
    }

    /**
     * Get the current value.
     *
     * @return the object that was read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public PixelsStruct getCurrentValue() throws IOException, InterruptedException
    {
        return currentValue;
    }

    /**
     * The current progress of the record reader through its data.
     *
     * @return a number between 0.0 and 1.0 that is the fraction of the data read
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException
    {
        // TODO: calculate the progress.
        return 0;
    }

    /**
     * Close the record reader.
     */
    @Override
    public void close() throws IOException
    {
        batchReader.close();
        // do not close the fileReader, it is shared by other record readers.
    }
}
