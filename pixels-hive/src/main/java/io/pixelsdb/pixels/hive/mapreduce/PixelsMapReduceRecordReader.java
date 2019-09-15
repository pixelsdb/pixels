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

import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.hive.common.PixelsRW;
import io.pixelsdb.pixels.hive.common.PixelsStruct;
import io.pixelsdb.pixels.hive.common.PixelsValue;
import io.pixelsdb.pixels.hive.mapred.PixelsMapredRecordReader;
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
    private List<Integer> pixelsIncluded;
    private List<Integer> hiveIncluded;
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
        this.pixelsIncluded = options.getPixelsIncluded();
        this.hiveIncluded = options.getHiveIncluded();
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

        if (this.pixelsIncluded.size() == 0)
        {
            rowIdInBatch += 1;
            return true;
        }

        int numberOfIncluded = this.pixelsIncluded.size();
        for (int i = 0; i < numberOfIncluded; ++i)
        {
            currentValue.setFieldValue(hiveIncluded.get(i), PixelsValue.nextValue(batch.cols[i], rowIdInBatch,
                    columnTypes.get(pixelsIncluded.get(i)), currentValue.getFieldValue(hiveIncluded.get(i))));
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
