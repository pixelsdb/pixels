/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.presto;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.type.*;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.pixelsdb.pixels.cache.MemoryMappedFile;
import io.pixelsdb.pixels.cache.PixelsCacheReader;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.predicate.PixelsPredicate;
import io.pixelsdb.pixels.core.predicate.TupleDomainPixelsPredicate;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.*;
import io.pixelsdb.pixels.presto.exception.PixelsErrorCode;
import io.pixelsdb.pixels.presto.impl.PixelsPrestoConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.type.StandardTypes.*;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Created at: 17/02/2022
 * Author: hank
 */
public class PixelsRecordCursor implements RecordCursor
{
    private static final Logger logger = Logger.get(PixelsPageSource.class);
    private final int BatchSize;
    private PixelsSplit split;
    private List<PixelsColumnHandle> columns;
    private Storage storage;
    private boolean closed;
    private PixelsReader pixelsReader;
    private PixelsRecordReader recordReader;
    private PixelsCacheReader cacheReader;
    private PixelsFooterCache footerCache;
    private long completedBytes = 0L;
    private long readTimeNanos = 0L;
    private long memoryUsage = 0L;
    private PixelsReaderOption option;
    private final int numColumnToRead;
    /**
     * If rowBatch == null && rowBatchSize > 0, numColumnToRead must be 0.
     * It means that the query is like select count(*) from table, i.e., it
     * does not read any physical data.
     *
     * If rowBatch == null && rowBatchSize == 0, it means that the first row
     * has not been read.
     */
    private VectorizedRowBatch rowBatch;
    private volatile int rowBatchSize;
    private volatile int rowIndex;

    private final Map<Integer, ByteColumnVector> byteColumnVectors = new HashMap<>();
    private final Map<Integer, BinaryColumnVector> byteArrayColumnVectors = new HashMap<>();
    private final Map<Integer, DateColumnVector> dateColumnVectors = new HashMap<>();
    private final Map<Integer, TimeColumnVector> timeColumnVectors = new HashMap<>();
    private final Map<Integer, TimestampColumnVector> timestampColumnVectors = new HashMap<>();
    private final Map<Integer, DoubleColumnVector> doubleColumnVectors = new HashMap<>();
    private final Map<Integer, LongColumnVector> longColumnVectors = new HashMap<>();

    public PixelsRecordCursor(PixelsSplit split, List<PixelsColumnHandle> columnHandles, Storage storage,
                              MemoryMappedFile cacheFile, MemoryMappedFile indexFile, PixelsFooterCache footerCache,
                              String connectorId)
    {
        this.split = split;
        this.storage = storage;
        this.columns = columnHandles;
        this.numColumnToRead = columnHandles.size();
        this.footerCache = footerCache;
        this.closed = false;

        this.cacheReader = PixelsCacheReader
                .newBuilder()
                .setCacheFile(cacheFile)
                .setIndexFile(indexFile)
                .build();
        getPixelsReaderBySchema(split, cacheReader, footerCache);

        try
        {
            this.recordReader = this.pixelsReader.read(this.option);
        }
        catch (IOException e)
        {
            logger.error("create record reader error: " + e.getMessage());
            closeWithSuppression(e);
            throw new PrestoException(PixelsErrorCode.PIXELS_READER_ERROR, e);
        }
        this.BatchSize = PixelsPrestoConfig.getBatchSize();
        this.rowIndex = -1;
        this.rowBatch = null;
        this.rowBatchSize = 0;
    }

    private void getPixelsReaderBySchema(PixelsSplit split, PixelsCacheReader pixelsCacheReader, PixelsFooterCache pixelsFooterCache)
    {
        String[] cols = new String[columns.size()];
        for (int i = 0; i < columns.size(); i++)
        {
            cols[i] = columns.get(i).getColumnName();
        }

        Map<PixelsColumnHandle, Domain> domains = new HashMap<>();
        if (split.getConstraint().getDomains().isPresent())
        {
            domains = split.getConstraint().getDomains().get();
        }
        List<TupleDomainPixelsPredicate.ColumnReference<PixelsColumnHandle>> columnReferences = new ArrayList<>(domains.size());
        for (Map.Entry<PixelsColumnHandle, Domain> entry : domains.entrySet())
        {
            PixelsColumnHandle column = entry.getKey();
            String columnName = column.getColumnName();
            int columnOrdinal = split.getOrder().indexOf(columnName);
            columnReferences.add(
                    new TupleDomainPixelsPredicate.ColumnReference<>(
                            column,
                            columnOrdinal,
                            column.getColumnType()));
        }
        PixelsPredicate predicate = new TupleDomainPixelsPredicate<>(split.getConstraint(), columnReferences);

        this.option = new PixelsReaderOption();
        this.option.skipCorruptRecords(true);
        this.option.tolerantSchemaEvolution(true);
        this.option.includeCols(cols);
        this.option.predicate(predicate);
        this.option.rgRange(split.getStart(), split.getLen());

        try
        {
            if (this.storage != null)
            {
                this.pixelsReader = PixelsReaderImpl
                        .newBuilder()
                        .setStorage(this.storage)
                        .setPath(split.getPath())
                        .setEnableCache(split.getCached())
                        .setCacheOrder(split.getCacheOrder())
                        .setPixelsCacheReader(pixelsCacheReader)
                        .setPixelsFooterCache(pixelsFooterCache)
                        .build();
            } else
            {
                logger.error("pixelsReader error: storage handler is null");
                throw new IOException("pixelsReader error: storage handler is null.");
            }
        } catch (IOException e)
        {
            logger.error("pixelsReader error: " + e.getMessage());
            closeWithSuppression(e);
            throw new PrestoException(PixelsErrorCode.PIXELS_READER_ERROR, e);
        }
    }

    private boolean readNextPath ()
    {
        try
        {
            if (this.split.nextPath())
            {
                closeReader();
                if (this.storage != null)
                {
                    this.pixelsReader = PixelsReaderImpl
                            .newBuilder()
                            .setStorage(this.storage)
                            .setPath(split.getPath())
                            .setEnableCache(split.getCached())
                            .setCacheOrder(split.getCacheOrder())
                            .setPixelsCacheReader(this.cacheReader)
                            .setPixelsFooterCache(this.footerCache)
                            .build();
                    this.recordReader = this.pixelsReader.read(this.option);
                } else
                {
                    logger.error("pixelsReader error: storage handler is null");
                    throw new IOException("pixelsReader error: storage handler is null");
                }
                return true;
            } else
            {
                return false;
            }
        } catch (Exception e)
        {
            logger.error("pixelsReader error: " + e.getMessage());
            closeWithSuppression(e);
            throw new PrestoException(PixelsErrorCode.PIXELS_READER_ERROR, e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        if (closed)
        {
            return this.completedBytes;
        }
        return this.completedBytes + recordReader.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        if (closed)
        {
            return readTimeNanos;
        }
        return this.readTimeNanos + recordReader.getReadTimeNanos();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        if (closed)
        {
            return memoryUsage;
        }
        return this.memoryUsage + recordReader.getMemoryUsage();
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field >= 0 && field < columns.size(), "Invalid field index");
        return this.columns.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (++this.rowIndex < this.rowBatchSize)
        {
            return true;
        }

        if (this.numColumnToRead > 0)
        {
            try
            {
                VectorizedRowBatch newRowBatch = this.recordReader.readBatch(BatchSize);
                if (newRowBatch.size <= 0)
                {
                    // reach the end of the file
                    if (readNextPath())
                    {
                        // open and start reading the next file (path).
                        newRowBatch = this.recordReader.readBatch(BatchSize);
                    } else
                    {
                        // no more files (paths) to read, close.
                        close();
                        return false;
                    }
                }
                if (this.rowBatch != newRowBatch)
                {
                    // VectorizedRowBatch may be reused by PixelsRecordReader.
                    this.rowBatch = newRowBatch;
                    this.setColumnVectors();
                }
                this.rowBatchSize = this.rowBatch.size;
                this.rowIndex = -1;
                return advanceNextPosition();
            } catch (IOException e)
            {
                closeWithSuppression(e);
                throw new PrestoException(PixelsErrorCode.PIXELS_BAD_DATA, e);
            }
        } else
        {
            // No column to read.
            try
            {
                int size = this.recordReader.prepareBatch(BatchSize);
                if (size <= 0)
                {
                    if (readNextPath())
                    {
                        size = this.recordReader.prepareBatch(BatchSize);
                    } else
                    {
                        close();
                        return false;
                    }
                }
                this.rowBatchSize = size;
                this.rowIndex = -1;
                return advanceNextPosition();
            } catch (IOException e)
            {
                closeWithSuppression(e);
                throw new PrestoException(PixelsErrorCode.PIXELS_BAD_DATA, e);
            }
        }
    }

    private void setColumnVectors()
    {
        requireNonNull(this.rowBatch, "this.rowBatch is null");
        requireNonNull(this.columns, "this.columns is null");

        this.byteColumnVectors.clear();
        this.byteArrayColumnVectors.clear();
        this.dateColumnVectors.clear();
        this.timeColumnVectors.clear();
        this.timestampColumnVectors.clear();
        this.doubleColumnVectors.clear();
        this.longColumnVectors.clear();

        for (int i = 0; i < this.numColumnToRead; ++i)
        {
            Type type = this.columns.get(i).getColumnType();
            switch (type.getDisplayName())
            {
                case INTEGER:
                case BIGINT:
                    this.longColumnVectors.put(i, (LongColumnVector) this.rowBatch.cols[i]);
                    break;
                case DOUBLE:
                case REAL:
                    this.doubleColumnVectors.put(i, (DoubleColumnVector) this.rowBatch.cols[i]);
                    break;
                case VARCHAR:
                    this.byteArrayColumnVectors.put(i, (BinaryColumnVector) this.rowBatch.cols[i]);
                    break;
                case BOOLEAN:
                    this.byteColumnVectors.put(i, (ByteColumnVector) this.rowBatch.cols[i]);
                    break;
                case DATE:
                    this.dateColumnVectors.put(i, (DateColumnVector) this.rowBatch.cols[i]);
                    break;
                case TIME:
                    this.timeColumnVectors.put(i, (TimeColumnVector) this.rowBatch.cols[i]);
                    break;
                case TIMESTAMP:
                    this.timestampColumnVectors.put(i, (TimestampColumnVector) this.rowBatch.cols[i]);
                    break;
                default:
                    throw new PrestoException(PixelsErrorCode.PIXELS_CURSOR_ERROR,
                            "Column type '" + type.getDisplayName() + "' is not supported.");
            }
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        return this.byteColumnVectors.get(field).vector[this.rowIndex] > 0;
    }

    @Override
    public long getLong(int field)
    {
        Type type = this.columns.get(field).getColumnType();
        if (type.equals(IntegerType.INTEGER) || type.equals(BigintType.BIGINT))
        {
            return this.longColumnVectors.get(field).vector[this.rowIndex];
        }
        else if (type.equals(DateType.DATE))
        {
            return this.dateColumnVectors.get(field).dates[this.rowIndex];
        }
        else if (type.equals(TimeType.TIME))
        {
            return this.timeColumnVectors.get(field).times[this.rowIndex];
        }
        else if (type.equals(TimestampType.TIMESTAMP))
        {
            return this.timestampColumnVectors.get(field).times[this.rowIndex];
        }
        throw new PrestoException(PixelsErrorCode.PIXELS_CURSOR_ERROR,
                "Column type '" + type.getDisplayName() + "' is not Long/Integer based.");
    }

    @Override
    public double getDouble(int field)
    {
        return Double.longBitsToDouble(this.doubleColumnVectors.get(field).vector[this.rowIndex]);
    }

    @Override
    public Slice getSlice(int field)
    {
        Type type = this.columns.get(field).getColumnType();
        checkArgument (type.equals(VarcharType.VARCHAR),
                "Column type '" + type.getDisplayName() + "' is not Slice based.");
        BinaryColumnVector columnVector = this.byteArrayColumnVectors.get(field);
        return Slices.wrappedBuffer(columnVector.vector[this.rowIndex],
                columnVector.start[this.rowIndex], columnVector.lens[this.rowIndex]);
    }

    @Override
    public Object getObject(int field)
    {
        throw new PrestoException(PixelsErrorCode.PIXELS_CURSOR_ERROR,
                "Array or Map type is not supported.");
    }

    @Override
    public boolean isNull(int field)
    {

        if (this.rowBatch == null)
        {
            return this.rowBatchSize > 0;
        }
        checkArgument(field < this.rowBatch.cols.length);
        return this.rowBatch.cols[field].isNull[this.rowIndex];
    }

    @Override
    public void close()
    {
        if (closed)
        {
            return;
        }

        closeReader();

        this.byteColumnVectors.clear();
        this.byteArrayColumnVectors.clear();
        this.longColumnVectors.clear();
        this.doubleColumnVectors.clear();
        this.dateColumnVectors.clear();
        this.timeColumnVectors.clear();
        this.timeColumnVectors.clear();

        closed = true;
    }

    private void closeReader()
    {
        try
        {
            if (pixelsReader != null)
            {
                if (recordReader != null)
                {
                    this.completedBytes += recordReader.getCompletedBytes();
                    this.readTimeNanos += recordReader.getReadTimeNanos();
                    this.memoryUsage += recordReader.getMemoryUsage();
                }
                pixelsReader.close();
                recordReader = null;
                pixelsReader = null;
            }
        } catch (Exception e)
        {
            logger.error("close error: " + e.getMessage());
            throw new PrestoException(PixelsErrorCode.PIXELS_READER_CLOSE_ERROR, e);
        }
    }

    private void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try
        {
            close();
        } catch (RuntimeException e)
        {
            // Self-suppression not permitted
            logger.error(e, e.getMessage());
            if (throwable != e)
            {
                throwable.addSuppressed(e);
            }
            throw new PrestoException(PixelsErrorCode.PIXELS_CLIENT_ERROR, e);
        }
    }

}
