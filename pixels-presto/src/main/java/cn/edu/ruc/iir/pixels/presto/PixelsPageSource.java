package cn.edu.ruc.iir.pixels.presto;

import cn.edu.ruc.iir.pixels.cache.PixelsCacheReader;
import cn.edu.ruc.iir.pixels.common.physical.FSFactory;
import cn.edu.ruc.iir.pixels.core.PixelsPredicate;
import cn.edu.ruc.iir.pixels.core.PixelsReader;
import cn.edu.ruc.iir.pixels.core.PixelsReaderImpl;
import cn.edu.ruc.iir.pixels.core.TupleDomainPixelsPredicate;
import cn.edu.ruc.iir.pixels.core.reader.PixelsReaderOption;
import cn.edu.ruc.iir.pixels.core.reader.PixelsRecordReader;
import cn.edu.ruc.iir.pixels.core.vector.BytesColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.DoubleColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.TimestampColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.block.VariableWidthBlock;
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

import static cn.edu.ruc.iir.pixels.presto.exception.PixelsErrorCode.PIXELS_BAD_DATA;
import static cn.edu.ruc.iir.pixels.presto.exception.PixelsErrorCode.PIXELS_CLIENT_ERROR;
import static cn.edu.ruc.iir.pixels.presto.exception.PixelsErrorCode.PIXELS_READER_CLOSE_ERROR;
import static cn.edu.ruc.iir.pixels.presto.exception.PixelsErrorCode.PIXELS_READER_ERROR;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

class PixelsPageSource implements ConnectorPageSource {
    private static Logger logger = Logger.get(PixelsPageSource.class);
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
    private volatile long nanoStart = 0L;
    private volatile long nanoEnd;

    public PixelsPageSource(PixelsSplit split, List<PixelsColumnHandle> columnHandles, FSFactory fsFactory,
                            PixelsCacheReader pixelsCacheReader, String connectorId)
    {
        this.fsFactory = fsFactory;
        this.columns = columnHandles;
        this.numColumnToRead = columnHandles.size();

        logger.info("Create page source for split: " + split.toString());
        getPixelsReaderBySchema(split, pixelsCacheReader);

        this.recordReader = this.pixelsReader.read(this.option);
    }

    private void getPixelsReaderBySchema(PixelsSplit split, PixelsCacheReader pixelsCacheReader) {
        String[] cols = new String[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            cols[i] = columns.get(i).getColumnName();
        }

        Map<PixelsColumnHandle, Domain> domains = new HashMap<>();
        if (split.getConstraint().getDomains().isPresent()) {
            domains = split.getConstraint().getDomains().get();
        }
        List<TupleDomainPixelsPredicate.ColumnReference<PixelsColumnHandle>> columnReferences = new ArrayList<>(domains.size());
        for (Map.Entry<PixelsColumnHandle, Domain> entry : domains.entrySet()) {
            PixelsColumnHandle column = entry.getKey();
            String columnName = column.getColumnName();
            int columnOrdinal = split.getOrder().indexOf(columnName);
            logger.debug("column: " + column.getColumnName() + " " + column.getColumnType() + " " + columnOrdinal);
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

        try {
            if (this.fsFactory.getFileSystem().isPresent())
            {
                this.pixelsReader = PixelsReaderImpl
                        .newBuilder()
                        .setFS(this.fsFactory.getFileSystem().get())
                        .setPath(new Path(split.getPath()))
                        .setEnableCache(split.isCached())
                        .setCacheOrder(split.getCacheOrder())
                        .setPixelsCacheReader(pixelsCacheReader)
                        .build();
            }
            else {
                logger.error("pixelsReader error: getFileSystem() null");
            }
        } catch (IOException e) {
            logger.error("pixelsReader error: " + e.getMessage());
            closeWithSuppression(e);
            throw new PrestoException(PIXELS_READER_ERROR, e);
        }
    }

    @Override
    public long getCompletedBytes() {
        return recordReader.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos() {
        return ((this.nanoStart > 0L) ? ((this.nanoEnd == 0L) ? System.nanoTime() : this.nanoEnd) - this.nanoStart : 0L);
    }

    @Override
    public boolean isFinished() {
        return this.closed;
    }

    @Override
    public Page getNextPage() {
        if (this.nanoStart == 0L) {
            this.nanoStart = System.nanoTime();
        }
        try {
            this.batchId++;
            this.rowBatch = this.recordReader.readBatch(BATCH_SIZE);
            int batchSize = this.rowBatch.size;
            if (batchSize <= 0 || (endOfFile && batchId >1) ) {
                close();
                return null;
            }
            Block[] blocks = new Block[this.numColumnToRead];

            for (int fieldId = 0; fieldId < blocks.length; ++fieldId)
            {
                Type type = columns.get(fieldId).getColumnType();
                String typeName = type.getDisplayName();
                int projIndex = this.rowBatch.projectedColumns[fieldId];
                ColumnVector cv = this.rowBatch.cols[projIndex];
                BlockBuilder blockBuilder = type.createBlockBuilder(
                        new BlockBuilderStatus(), batchSize);

                switch (typeName)
                {
                    case "integer":
                    case "bigint":
                    case "long":
                    case "int":
                        LongColumnVector lcv = (LongColumnVector) cv;
                        for (int i = 0; i < batchSize; ++i)
                        {
                            if (lcv.isNull[i])
                            {
                                blockBuilder.appendNull();
                            }
                            else
                            {
                                type.writeLong(blockBuilder, lcv.vector[i]);
                            }
                        }
                        blocks[fieldId] = blockBuilder.build();
                        break;
                    case "double":
                    case "float":
                        DoubleColumnVector dcv = (DoubleColumnVector) cv;
                        for (int i = 0; i < batchSize; ++i)
                        {
                            if (dcv.isNull[i])
                            {
                                blockBuilder.appendNull();
                            }
                            else
                            {
                                type.writeDouble(blockBuilder, dcv.vector[i]);
                            }
                        }
                        blocks[fieldId] = blockBuilder.build();
                        break;
                    case "varchar":
                    case "string":
                        BytesColumnVector scv = (BytesColumnVector) cv;
                        int vectorContentLen = 0;
                        for (int i = 0; i < batchSize; ++i)
                        {
                            vectorContentLen += scv.lens[i];
                        }
                        byte[] vectorContent = new byte[vectorContentLen];
                        int[] vectorOffsets = new int[batchSize+1];
                        int curVectorOffset = 0;
                        for (int i = 0; i < batchSize; ++i)
                        {
                            int elementLen = scv.lens[i];
                            if (!scv.isNull[i])
                            {
                                System.arraycopy(scv.vector[i], scv.start[i], vectorContent, curVectorOffset, elementLen);
                            }
                            vectorOffsets[i] = curVectorOffset;
                            curVectorOffset += elementLen;
                        }
                        vectorOffsets[batchSize] = vectorContentLen;
                        blocks[fieldId] = new VariableWidthBlock(batchSize,
                                Slices.wrappedBuffer(vectorContent, 0, vectorContentLen),
                                vectorOffsets,
                                scv.isNull);
                        break;
                    case "boolean":
                        LongColumnVector bcv = (LongColumnVector) cv;
                        for (int i = 0; i < this.rowBatch.size; ++i)
                        {
                            if (bcv.isNull[i])
                            {
                                blockBuilder.appendNull();
                            }
                            else
                            {
                                type.writeBoolean(blockBuilder, bcv.vector[i] == 1);
                            }
                        }
                        blocks[fieldId] = blockBuilder.build();
                        break;
                    case "timestamp":
                        TimestampColumnVector tcv = (TimestampColumnVector) cv;
                        for (int i = 0; i < this.rowBatch.size; ++i)
                        {
                            if (tcv.isNull[i])
                            {
                                blockBuilder.appendNull();
                            }
                            else
                            {
                                type.writeLong(blockBuilder, tcv.time[i]);
                            }
                        }
                        blocks[fieldId] = blockBuilder.build();
                        break;
                    default:
                        for (int i = 0; i < this.rowBatch.size; ++i)
                        {
                            blockBuilder.appendNull();
                        }
                        blocks[fieldId] = blockBuilder.build();
                        break;
                }
            }
            sizeOfData += batchSize;
            if (this.rowBatch.endOfFile) {
                endOfFile = true;
            }
            return new Page(batchSize, blocks);
        } catch (IOException e) {
            closeWithSuppression(e);
            throw new PrestoException(PIXELS_BAD_DATA, e);
        }
    }

    @Override
    public long getSystemMemoryUsage() {
        return sizeOfData;
    }

    @Override
    public void close() {
        try {
            if (pixelsReader != null) {
                pixelsReader.close();
            }
            rowBatch = null;
            nanoEnd = System.nanoTime();
        } catch (Exception e) {
            logger.error("close error: " + e.getMessage());
            throw new PrestoException(PIXELS_READER_CLOSE_ERROR, e);
        }

        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;
    }

    private void closeWithSuppression(Throwable throwable) {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        } catch (RuntimeException e) {
            // Self-suppression not permitted
            logger.error(e, e.getMessage());
            if (throwable != e) {
                throwable.addSuppressed(e);
            }
            throw new PrestoException(PIXELS_CLIENT_ERROR, e);
        }
    }

    /**
     * Lazy Block Implementation for the Pixels
     */
    private final class PixelsBlockLoader
            implements LazyBlockLoader<LazyBlock> {

        private Logger logger = Logger.get(PixelsBlockLoader.class);

        private final int expectedBatchId = batchId;
        private final int columnIndex;
        private final Type type;
        private boolean loaded;

        public PixelsBlockLoader(int columnIndex, Type type) {
            this.columnIndex = columnIndex;
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        public final void load(LazyBlock lazyBlock) {
            if (loaded) {
                return;
            }
            checkState(batchId == expectedBatchId);
            BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), BATCH_SIZE);
            Block block = builder.build();
            lazyBlock.setBlock(block);
            loaded = true;
        }
    }

}