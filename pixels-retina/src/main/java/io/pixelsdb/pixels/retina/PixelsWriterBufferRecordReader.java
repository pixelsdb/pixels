/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.retina;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

public class PixelsWriterBufferRecordReader implements PixelsRecordReader
{
    private static final Logger logger = LogManager.getLogger(PixelsWriterBufferRecordReader.class);

    private final TypeDescription schema;
    private VectorizedRowBatch rowBatch;

    private final AtomicLong completedRows = new AtomicLong(0);
    private final AtomicLong completedBytes = new AtomicLong(0);
    private final AtomicLong readTimeNanos = new AtomicLong(0);
    private final AtomicLong memoryUsage = new AtomicLong(0);

    private boolean isValid = true;
    private boolean isEndOfFile = false;

    public PixelsWriterBufferRecordReader(TypeDescription schema)
    {
        this.schema = schema;
        this.rowBatch = schema.createRowBatchWithHiddenColumn();
    }

    @Override
    public int prepareBatch(int batchSize) throws IOException
    {
        return Math.min(batchSize, this.rowBatch.getMaxSize());
    }

    @Override
    public VectorizedRowBatch readBatch(int batchSize, boolean reuse) throws IOException
    {
        long startTime = System.nanoTime();

        if (!isValid || isEndOfFile)
        {
            return createEmptyBatch(reuse);
        }

        VectorizedRowBatch result;
        if (reuse && rowBatch != null)
        {
            result = rowBatch;
            result.reset();
        } else
        {
            result = schema.createRowBatchWithHiddenColumn();
        }

        result.size = 0;
        completedRows.addAndGet(result.size);
        
        long bytesRead = estimateBytesRead(result);
        completedBytes.addAndGet(bytesRead);

        if (result.size < batchSize)
        {
            isEndOfFile = true;
            result.endOfFile = true;
        }

        readTimeNanos.addAndGet(System.nanoTime() - startTime);
        return result;
    }

    private long estimateBytesRead(VectorizedRowBatch batch)
    {
        long bytesPerRow = 0;
        for (int i = 0; i < schema.getChildren().size(); ++i)
        {
            TypeDescription fieldType = schema.getChildren().get(i);
            switch (fieldType.getCategory()) {
                case BOOLEAN:
                case BYTE:
                    bytesPerRow += 1;
                    break;
                case SHORT:
                    bytesPerRow += 2;
                    break;
                case INT:
                case FLOAT:
                case DATE:
                    bytesPerRow += 4;
                    break;
                case LONG:
                case DOUBLE:
                case TIMESTAMP:
                    bytesPerRow += 8;
                    break;
                case DECIMAL:
                    bytesPerRow += 16;
                    break;
                case STRING:
                case BINARY:
                    bytesPerRow += 32;
                    break;
                default:
                    bytesPerRow += 8;
            }
        }
        return bytesPerRow * batch.size;
    }

    private VectorizedRowBatch createEmptyBatch(boolean reuse)
    {
        VectorizedRowBatch result;
        if (reuse && rowBatch != null)
        {
            result = rowBatch;
            result.reset();
        } else
        {
            result = schema.createRowBatchWithHiddenColumn();
        }
        result.size = 0;
        result.endOfFile = true;
        return result;
    }

    @Override
    public VectorizedRowBatch readBatch(int batchSize) throws IOException
    {
        return readBatch(batchSize, false);
    }

    @Override
    public VectorizedRowBatch readBatch(boolean reuse) throws IOException
    {
        return readBatch(VectorizedRowBatch.DEFAULT_SIZE, reuse);
    }

    @Override
    public VectorizedRowBatch readBatch() throws IOException
    {
        return readBatch(VectorizedRowBatch.DEFAULT_SIZE, false);
    }

    @Override
    public TypeDescription getResultSchema()
    {
        return schema;
    }

    @Override
    public boolean isValid()
    {
        return isValid;
    }

    @Override
    public boolean isEndOfFile()
    {
        return isEndOfFile;
    }

    @Override
    public boolean seekToRow(long rowIndex) throws IOException
    {
        throw new UnsupportedOperationException("Seek operation is not supported");
    }

    @Override
    public boolean skip(long rowNum) throws IOException
    {
        throw new UnsupportedOperationException("Skip operation is not supported");
    }

    @Override
    public long getCompletedRows()
    {
        return completedRows.get();
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes.get();
    }

    @Override
    public int getNumReadRequests()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos.get();
    }

    @Override
    public long getMemoryUsage()
    {
        return memoryUsage.get();
    }

    @Override
    public void close() throws IOException
    {
        rowBatch = null;
        isValid = false;
        isEndOfFile = true;
        logger.info("Closed WriterBufferRecordReader");
    }
}
