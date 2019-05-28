package cn.edu.ruc.iir.pixels.presto;

import cn.edu.ruc.iir.pixels.cache.MemoryMappedFile;
import cn.edu.ruc.iir.pixels.cache.PixelsCacheReader;
import cn.edu.ruc.iir.pixels.common.physical.FSFactory;
import cn.edu.ruc.iir.pixels.core.*;
import cn.edu.ruc.iir.pixels.core.reader.PixelsReaderOption;
import cn.edu.ruc.iir.pixels.core.reader.PixelsRecordReader;
import cn.edu.ruc.iir.pixels.core.vector.*;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.*;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.edu.ruc.iir.pixels.presto.exception.PixelsErrorCode.*;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

class PixelsPageSource implements ConnectorPageSource
{
    private static Logger logger = Logger.get(PixelsPageSource.class);
    // TODO: make this parameter configurable.
    private static final int BATCH_SIZE = 10000;
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
            throw new PrestoException(PIXELS_READER_ERROR, e);
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
            batchSize = this.recordReader.prepareBatch(BATCH_SIZE);
        } catch (IOException e)
        {
            closeWithSuppression(e);
            throw new PrestoException(PIXELS_BAD_DATA, e);
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
            throw new PrestoException(PIXELS_READER_CLOSE_ERROR, e);
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
            throw new PrestoException(PIXELS_CLIENT_ERROR, e);
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
                    // TODO: not read all the columns at a time.
                    rowBatch = recordReader.readBatch(batchSize);
                } catch (IOException e)
                {
                    closeWithSuppression(e);
                    throw new PrestoException(PIXELS_BAD_DATA, e);
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
            // TODO: read column lazily.
            ColumnVector cv = rowBatch.cols[projIndex];
            BlockBuilder blockBuilder = type.createBlockBuilder(
                    new BlockBuilderStatus(), rowBatch.size);

            switch (typeName)
            {
                case "integer":
                case "bigint":
                case "long":
                case "int":
                    LongColumnVector lcv = (LongColumnVector) cv;
                    for (int i = 0; i < rowBatch.size; ++i)
                    {
                        if (lcv.isNull[i])
                        {
                            blockBuilder.appendNull();
                        } else
                        {
                            type.writeLong(blockBuilder, lcv.vector[i]);
                        }
                    }
                    block = blockBuilder.build();
                    break;
                case "double":
                case "float":
                    DoubleColumnVector dcv = (DoubleColumnVector) cv;
                    for (int i = 0; i < rowBatch.size; ++i)
                    {
                        if (dcv.isNull[i])
                        {
                            blockBuilder.appendNull();
                        } else
                        {
                            type.writeDouble(blockBuilder, dcv.vector[i]);
                        }
                    }
                    block = blockBuilder.build();
                    break;
                case "varchar":
                case "string":
                    BytesColumnVector scv = (BytesColumnVector) cv;
                    int vectorContentLen = 0;
                    byte[] vectorContent = null;
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
                            // TODO: remove this memory copy by implementing new Block and BlockBuilder.
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
                    break;
                case "boolean":
                    LongColumnVector bcv = (LongColumnVector) cv;
                    for (int i = 0; i < rowBatch.size; ++i)
                    {
                        if (bcv.isNull[i])
                        {
                            blockBuilder.appendNull();
                        } else
                        {
                            type.writeBoolean(blockBuilder, bcv.vector[i] == 1);
                        }
                    }
                    block = blockBuilder.build();
                    break;
                case "timestamp":
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