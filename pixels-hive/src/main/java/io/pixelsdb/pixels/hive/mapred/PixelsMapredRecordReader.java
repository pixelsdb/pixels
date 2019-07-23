/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pixelsdb.pixels.hive.mapred;

import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.hive.common.PixelsRW;
import io.pixelsdb.pixels.hive.common.PixelsStruct;
import io.pixelsdb.pixels.hive.common.PixelsValue;
import org.apache.hadoop.hive.ql.io.StatsProvidingRecordReader;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * This record reader implements the org.apache.hadoop.mapred API.
 * refers to {@link org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl}
 *
 * <p>
 * Created at: 19-6-30
 * Author: hank
 * </p>
 */
@SuppressWarnings("Duplicates")
public class PixelsMapredRecordReader
        implements org.apache.hadoop.mapred.RecordReader<NullWritable, PixelsStruct>, StatsProvidingRecordReader
{
    private static Logger log = LogManager.getLogger(PixelsMapredRecordReader.class);

    private final PixelsRW.ReaderOptions options;
    private final int batchSize;
    private final TypeDescription schema;
    private final PixelsRecordReader batchReader;
    private VectorizedRowBatch batch;
    private int rowIdInBatch;
    private List<Integer> pixelsIncluded;
    private List<Integer> hiveIncluded;
    private List<TypeDescription> columnTypes;
    private int numColumns;
    private final SerDeStats stats;
    private final NullWritable currentKey;
    private PixelsStruct currentValue;

    public PixelsMapredRecordReader(PixelsReader fileReader,
                                    PixelsRW.ReaderOptions options) throws IOException
    {
        this.options = options;
        this.schema = fileReader.getFileSchema();
        // schema should be of struct type.
        assert schema.getCategory() == TypeDescription.Category.STRUCT;

        this.batchReader = fileReader.read(options.getReaderOption());
        this.columnTypes = schema.getChildren();
        this.numColumns = columnTypes.size();
        this.batchSize = options.getBatchSize();
        this.batch = null; // the first batch will be read in next.
        this.rowIdInBatch = 0;
        this.pixelsIncluded = options.getPixelsIncluded();
        this.hiveIncluded = options.getHiveIncluded();
        this.stats = new SerDeStats();
        stats.setRawDataSize(fileReader.getCompressionBlockSize());
        stats.setRowCount(fileReader.getNumberOfRows());
        this.currentKey = NullWritable.get();
        this.currentValue = new PixelsStruct(this.numColumns);
    }

    /**
     * If the current batch is empty, get a new one.
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

    @Override
    public boolean next(NullWritable key, PixelsStruct value) throws IOException
    {
        // value is created by createValue, is should not be null.
        assert value != null;

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
            int proj = batch.projectedColumns[i];
            value.setFieldValue(hiveIncluded.get(i), PixelsValue.nextValue(batch.cols[proj], rowIdInBatch,
                    columnTypes.get(pixelsIncluded.get(i)), value.getFieldValue(hiveIncluded.get(i))));
        }

        rowIdInBatch += 1;
        return true;
    }

    /**
     * Create an object of the appropriate type to be used as a key.
     *
     * @return a new key object.
     */
    @Override
    public NullWritable createKey()
    {
        return currentKey;
    }

    /**
     * Create an object of the appropriate type to be used as a value.
     *
     * @return a new value object.
     */
    @Override
    public PixelsStruct createValue()
    {
        return currentValue;
    }

    /**
     * Returns the current position in the input.
     *
     * @return the current position in the input.
     * @throws IOException
     */
    @Override
    public long getPos() throws IOException
    {
        return 0;
    }

    /**
     * Close this {@link InputSplit} to future operations.
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException
    {
        batchReader.close();
        // do not close the fileReader, it is shared by other record readers.
    }

    /**
     * How much of the input has the {@link RecordReader} consumed i.e.
     * has been processed by?
     *
     * @return progress from <code>0.0</code> to <code>1.0</code>.
     * @throws IOException
     */
    @Override
    public float getProgress() throws IOException
    {
        // TODO: calculate the progress.
        return 0;
    }

    @Override
    public SerDeStats getStats()
    {
        return stats;
    }
}
