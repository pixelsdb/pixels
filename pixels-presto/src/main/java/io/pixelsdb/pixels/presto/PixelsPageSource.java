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
 * License along with Foobar.  If not, see
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
import io.pixelsdb.pixels.common.physical.FSFactory;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.*;
import io.pixelsdb.pixels.presto.block.VarcharArrayBlock;
import io.pixelsdb.pixels.presto.exception.PixelsErrorCode;
import io.pixelsdb.pixels.presto.impl.PixelsPrestoConfig;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @author guodong
 * @author tao
 */
class PixelsPageSource implements ConnectorPageSource
{
    private static Logger logger = Logger.get(PixelsPageSource.class);
    private final int BatchSize;
    private List<PixelsColumnHandle> columns;
    private FSFactory fsFactory;
    private boolean closed;
    private boolean endOfFile;
    private PixelsReader pixelsReader;
    private PixelsRecordReader recordReader;
    private long sizeOfData = 0L;
    private PixelsReaderOption option;
    private int numColumnToRead;
    private int batchId;
    private VectorizedRowBatch rowBatch;
    private boolean rowBatchRead;

    public PixelsPageSource(PixelsSplit split, List<PixelsColumnHandle> columnHandles, FSFactory fsFactory,
                            MemoryMappedFile cacheFile, MemoryMappedFile indexFile, PixelsFooterCache pixelsFooterCache,
                            String connectorId)
    {
        this.fsFactory = fsFactory;
        this.columns = columnHandles;
        this.numColumnToRead = columnHandles.size();

        PixelsCacheReader pixelsCacheReader = PixelsCacheReader
                .newBuilder()
                .setCacheFile(cacheFile)
                .setIndexFile(indexFile)
                .build();
        getPixelsReaderBySchema(split, pixelsCacheReader, pixelsFooterCache);

        this.recordReader = this.pixelsReader.read(this.option);
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
//            logger.debug("column: " + column.getColumnName() + " " + column.getColumnType() + " " + columnOrdinal);
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
            if (this.fsFactory.getFileSystem().isPresent())
            {
                this.pixelsReader = PixelsReaderImpl
                        .newBuilder()
                        .setFS(this.fsFactory.getFileSystem().get())
                        .setPath(new Path(split.getPath()))
                        .setEnableCache(split.getCached())
                        .setCacheOrder(split.getCacheOrder())
                        .setPixelsCacheReader(pixelsCacheReader)
                        .setPixelsFooterCache(pixelsFooterCache)
                        .build();
            } else
            {
                logger.error("pixelsReader error: getFileSystem() returns null");
            }
        } catch (IOException e)
        {
            logger.error("pixelsReader error: " + e.getMessage());
            closeWithSuppression(e);
            throw new PrestoException(PixelsErrorCode.PIXELS_READER_ERROR, e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return recordReader.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return recordReader.getReadTimeNanos();
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
        int batchSize = 0;
        try
        {
            batchSize = this.recordReader.prepareBatch(BatchSize);
        } catch (IOException e)
        {
            closeWithSuppression(e);
            throw new PrestoException(PixelsErrorCode.PIXELS_BAD_DATA, e);
        }
        this.rowBatchRead = false;

        if (batchSize <= 0 || (endOfFile && batchId > 1))
        {
            close();
            return null;
        }
        Block[] blocks = new Block[this.numColumnToRead];

        for (int fieldId = 0; fieldId < blocks.length; ++fieldId)
        {
            Type type = columns.get(fieldId).getColumnType();
            blocks[fieldId] = new LazyBlock(batchSize, new PixelsBlockLoader(fieldId, type, batchSize));
        }
        sizeOfData += batchSize;

        return new Page(batchSize, blocks);
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return sizeOfData;
    }

    @Override
    public void close()
    {
        try
        {
            if (pixelsReader != null)
            {
                pixelsReader.close();
            }
            rowBatch = null;
        } catch (Exception e)
        {
            logger.error("close error: " + e.getMessage());
            throw new PrestoException(PixelsErrorCode.PIXELS_READER_CLOSE_ERROR, e);
        }

        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed)
        {
            return;
        }
        closed = true;
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
        private final int columnIndex;
        private final Type type;
        private final int batchSize;
        private boolean loaded = false;

        public PixelsBlockLoader(int columnIndex, Type type, int batchSize)
        {
            this.columnIndex = columnIndex;
            this.type = requireNonNull(type, "type is null");
            this.batchSize = batchSize;
        }

        @Override
        public final void load(LazyBlock lazyBlock)
        {
            if (loaded)
            {
                return;
            }
            checkState(batchId == expectedBatchId);

            if (rowBatchRead == false)
            {
                try
                {
                    // TODO: to reduce GC pressure, not read all the columns at a time.
                    rowBatch = recordReader.readBatch(batchSize);
                } catch (IOException e)
                {
                    closeWithSuppression(e);
                    throw new PrestoException(PixelsErrorCode.PIXELS_BAD_DATA, e);
                }
                rowBatchRead = true;

                if (rowBatch.endOfFile)
                {
                    endOfFile = true;
                }
            }

            Block block;

            String typeName = type.getDisplayName();
            int projIndex = rowBatch.projectedColumns[columnIndex];
            ColumnVector cv = rowBatch.cols[projIndex];
            BlockBuilder blockBuilder = type.createBlockBuilder(
                    new BlockBuilderStatus(), rowBatch.size);

            switch (typeName)
            {
                case "integer":
                case "bigint":
                    LongColumnVector lcv = (LongColumnVector) cv;
                    block = new LongArrayBlock(rowBatch.size, lcv.isNull, lcv.vector);
                    break;
                case "double":
                case "real":
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
                    DoubleColumnVector dcv = (DoubleColumnVector) cv;
//                    for (int i = 0; i < rowBatch.size; ++i)
//                    {
//                        if (dcv.isNull[i])
//                        {
//                            blockBuilder.appendNull();
//                        } else
//                        {
//                            // type is DoubleType for double, not sure for float.
//                            type.writeDouble(blockBuilder, dcv.vector[i]);
//                        }
//                    }
//                    block = blockBuilder.build();
                    block = new LongArrayBlock(rowBatch.size, dcv.isNull, dcv.vector);
                    break;
                case "varchar":
                    BinaryColumnVector scv = (BinaryColumnVector) cv;
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
                    block = new VarcharArrayBlock(rowBatch.size, scv.vector, scv.start, scv.lens, scv.isNull);
                    break;
                case "boolean":
                    ByteColumnVector bcv = (ByteColumnVector) cv;
//                    for (int i = 0; i < rowBatch.size; ++i)
//                    {
//                        if (bcv.isNull[i])
//                        {
//                            blockBuilder.appendNull();
//                        } else
//                        {
//                            type.writeBoolean(blockBuilder, bcv.vector[i] == 1);
//                        }
//                    }
//                    block = blockBuilder.build();
                    block = new ByteArrayBlock(rowBatch.size, bcv.isNull, bcv.vector);
                    break;
                case "timestamp":
                    // TODO: optimization needed for timestamp
                    TimestampColumnVector tcv = (TimestampColumnVector) cv;
                    for (int i = 0; i < rowBatch.size; ++i)
                    {
                        if (tcv.isNull[i])
                        {
                            blockBuilder.appendNull();
                        } else
                        {
                            type.writeLong(blockBuilder, tcv.time[i]);
                        }
                    }
                    block = blockBuilder.build();
                    break;
                default:
                    for (int i = 0; i < rowBatch.size; ++i)
                    {
                        blockBuilder.appendNull();
                    }
                    block = blockBuilder.build();
                    break;
            }

            lazyBlock.setBlock(block);
            loaded = true;
        }
    }

}