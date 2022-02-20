/*
 * Copyright 2018-2019 PixelsDB.
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

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.*;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;
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
import io.pixelsdb.pixels.presto.block.TimeArrayBlock;
import io.pixelsdb.pixels.presto.block.VarcharArrayBlock;
import io.pixelsdb.pixels.presto.exception.PixelsErrorCode;
import io.pixelsdb.pixels.presto.impl.PixelsPrestoConfig;

import java.io.IOException;
import java.util.*;

import static com.facebook.presto.spi.type.StandardTypes.*;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @author guodong
 * @author tao
 */
class PixelsPageSource implements ConnectorPageSource
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
    private int batchId;

    public PixelsPageSource(PixelsSplit split, List<PixelsColumnHandle> columnHandles, Storage storage,
                            MemoryMappedFile cacheFile, MemoryMappedFile indexFile, PixelsFooterCache pixelsFooterCache,
                            String connectorId)
    {
        this.split = split;
        this.storage = storage;
        this.columns = columnHandles;
        this.numColumnToRead = columnHandles.size();
        this.footerCache = pixelsFooterCache;
        this.batchId = 0;
        this.closed = false;

        this.cacheReader = PixelsCacheReader
                .newBuilder()
                .setCacheFile(cacheFile)
                .setIndexFile(indexFile)
                .build();
        getPixelsReaderBySchema(split, cacheReader, pixelsFooterCache);

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
        this.option.transInfo(split.getTransInfo());

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
        /**
         * Issue #113:
         * I am still not sure show the result of this method are used by Presto.
         * Currently, we return the cumulative memory usage. However this may be
         * inappropriate.
         * I tested about ten queries on test_1187, there was no problem, but
         * TODO: we still need to be careful about this method in the future.
         */
        if (closed)
        {
            return memoryUsage;
        }
        return this.memoryUsage + recordReader.getMemoryUsage();
    }

    @Override
    public boolean isFinished()
    {
        return this.closed;
    }

    @Override
    public Page getNextPage()
    {
        this.batchId++;
        VectorizedRowBatch rowBatch;
        int rowBatchSize;

        Block[] blocks = new Block[this.numColumnToRead];

        if (this.numColumnToRead > 0)
        {
            try
            {
                rowBatch = recordReader.readBatch(BatchSize, false);
                rowBatchSize = rowBatch.size;
                if (rowBatchSize <= 0)
                {
                    if (readNextPath())
                    {
                        return getNextPage();
                    } else
                    {
                        close();
                        return null;
                    }
                }
                for (int fieldId = 0; fieldId < blocks.length; ++fieldId)
                {
                    Type type = columns.get(fieldId).getColumnType();
                    ColumnVector vector = rowBatch.cols[fieldId];
                    blocks[fieldId] = new LazyBlock(rowBatchSize, new PixelsBlockLoader(vector, type, rowBatchSize));
                }
            } catch (IOException e)
            {
                closeWithSuppression(e);
                throw new PrestoException(PixelsErrorCode.PIXELS_BAD_DATA, e);
            }
        }
        else
        {
            // No column to read.
            try
            {
                rowBatchSize = this.recordReader.prepareBatch(BatchSize);
                if (rowBatchSize <= 0)
                {
                    if (readNextPath())
                    {
                        return getNextPage();
                    } else
                    {
                        close();
                        return null;
                    }
                }
            } catch (IOException e)
            {
                closeWithSuppression(e);
                throw new PrestoException(PixelsErrorCode.PIXELS_BAD_DATA, e);
            }
        }

        return new Page(rowBatchSize, blocks);
    }

    @Override
    public void close()
    {
        if (closed)
        {
            return;
        }

        closeReader();

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
                /**
                 * Issue #114:
                 * Must set pixelsReader and recordReader to null,
                 * close() may be called multiple times by Presto.
                 */
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

    /**
     * Lazy Block Implementation for the Pixels
     */
    private final class PixelsBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final int expectedBatchId = batchId;
        private final ColumnVector vector;
        private final Type type;
        private final int batchSize;

        public PixelsBlockLoader(ColumnVector vector, Type type, int batchSize)
        {
            this.vector = vector;
            this.type = requireNonNull(type, "type is null");
            this.batchSize = batchSize;
        }

        @Override
        public final void load(LazyBlock lazyBlock)
        {
            checkState(batchId == expectedBatchId);
            Block block;

            switch (type.getDisplayName())
            {
                case INTEGER:
                case BIGINT:
                    LongColumnVector lcv = (LongColumnVector) vector;
                    block = new LongArrayBlock(batchSize, Optional.ofNullable(lcv.isNull), lcv.vector);
                    break;
                case DOUBLE:
                case REAL:
                    /**
                     * According to TypeDescription.createColumn(),
                     * both float and double type use DoubleColumnVector, while they use
                     * FloatColumnReader and DoubleColumnReader respectively according to
                     * io.pixelsdb.pixels.reader.ColumnReader.newColumnReader().
                     * TODO: these two column should also support reading to LongColumnVector,
                     * without conversions like longBitsToDouble, so that we can directly create
                     * LongArrayBlock here without writeDouble (in which double is converted to long).
                     * With this optimization, CPU and GC pressure can be greatly reduced.
                     */
                    DoubleColumnVector dcv = (DoubleColumnVector) vector;
                    block = new LongArrayBlock(batchSize, Optional.ofNullable(dcv.isNull), dcv.vector);
                    break;
                case VARCHAR:
                    BinaryColumnVector scv = (BinaryColumnVector) vector;
                    /*
                    int vectorContentLen = 0;
                    byte[] vectorContent;
                    int[] vectorOffsets = new int[rowBatch.size + 1];
                    int curVectorOffset = 0;
                    for (int i = 0; i < rowBatch.size; ++i)
                    {
                        vectorContentLen += scv.lens[i];
                    }
                    vectorContent = new byte[vectorContentLen];
                    for (int i = 0; i < rowBatch.size; ++i)
                    {
                        int elementLen = scv.lens[i];
                        if (!scv.isNull[i])
                        {
                            System.arraycopy(scv.vector[i], scv.start[i], vectorContent, curVectorOffset, elementLen);
                        }
                        vectorOffsets[i] = curVectorOffset;
                        curVectorOffset += elementLen;
                    }
                    vectorOffsets[rowBatch.size] = vectorContentLen;
                    block = new VariableWidthBlock(rowBatch.size,
                            Slices.wrappedBuffer(vectorContent, 0, vectorContentLen),
                            vectorOffsets,
                            scv.isNull);
                            */
                    block = new VarcharArrayBlock(batchSize, scv.vector, scv.start, scv.lens, scv.isNull);
                    break;
                case BOOLEAN:
                    ByteColumnVector bcv = (ByteColumnVector) vector;
                    block = new ByteArrayBlock(batchSize, Optional.ofNullable(bcv.isNull), bcv.vector);
                    break;
                case DATE:
                    // Issue #94: add date type.
                    DateColumnVector dtcv = (DateColumnVector) vector;
                    // In pixels and Presto, date is stored as the number of days from UTC 1970-1-1 0:0:0.
                    block = new IntArrayBlock(batchSize, Optional.ofNullable(dtcv.isNull), dtcv.dates);
                    break;
                case TIME:
                    // Issue #94: add time type.
                    TimeColumnVector tcv = (TimeColumnVector) vector;
                    /**
                     * In Presto, LongArrayBlock is used for time type. However, in Pixels,
                     * Time value is stored as int, so here we use TimeArrayBlock, which
                     * accepts int values but provides getLong method same as LongArrayBlock.
                     */
                    block = new TimeArrayBlock(batchSize, tcv.isNull, tcv.times);
                    break;
                case TIMESTAMP:
                    TimestampColumnVector tscv = (TimestampColumnVector) vector;
                    /**
                     * Issue #94: we have confirmed that LongArrayBlock is used for timestamp
                     * type in Presto.
                     *
                     * com.facebook.presto.spi.type.TimestampType extends
                     * com.facebook.presto.spi.type.AbstractLongType, which creates a LongArrayBlockBuilder.
                     * And this block builder builds a LongArrayBlock.
                     */
                    block = new LongArrayBlock(batchSize, Optional.ofNullable(tscv.isNull), tscv.times);
                    break;
                default:
                    BlockBuilder blockBuilder = type.createBlockBuilder(null, batchSize);
                    for (int i = 0; i < batchSize; ++i)
                    {
                        blockBuilder.appendNull();
                    }
                    block = blockBuilder.build();
                    break;
            }

            lazyBlock.setBlock(block);
        }
    }

}