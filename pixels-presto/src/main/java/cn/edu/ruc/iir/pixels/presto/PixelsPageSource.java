package cn.edu.ruc.iir.pixels.presto;

import cn.edu.ruc.iir.pixels.core.PixelsPredicate;
import cn.edu.ruc.iir.pixels.core.PixelsReader;
import cn.edu.ruc.iir.pixels.core.PixelsReaderImpl;
import cn.edu.ruc.iir.pixels.core.TupleDomainPixelsPredicate;
import cn.edu.ruc.iir.pixels.core.reader.PixelsReaderOption;
import cn.edu.ruc.iir.pixels.core.reader.PixelsRecordReader;
import cn.edu.ruc.iir.pixels.core.vector.*;
import cn.edu.ruc.iir.pixels.presto.impl.FSFactory;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.*;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

class PixelsPageSource implements ConnectorPageSource {
    private static Logger logger = Logger.get(PixelsPageSource.class);
    private static final int BATCH_SIZE = 1000;
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
    private long nanoStart;
    private long nanoEnd;

    public PixelsPageSource(PixelsSplit split, List<PixelsColumnHandle> columnHandles, FSFactory fsFactory, String connectorId) {
        this.fsFactory = fsFactory;
        this.columns = columnHandles;
        this.numColumnToRead = columnHandles.size();

        getPixelsReaderBySchema(split);

        this.recordReader = this.pixelsReader.read(this.option);
    }

    private void getPixelsReaderBySchema(PixelsSplit split) {
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
            logger.debug("column: " + column.getColumnName() + " " + column.getColumnType() + " " + column.getOrdinalPosition());
            columnReferences.add(
                    new TupleDomainPixelsPredicate.ColumnReference<>(
                            column,
                            column.getOrdinalPosition(),
                            column.getColumnType()));
        }
        PixelsPredicate predicate = new TupleDomainPixelsPredicate<>(split.getConstraint(), columnReferences);

        this.option = new PixelsReaderOption();
        this.option.skipCorruptRecords(true);
        this.option.tolerantSchemaEvolution(true);
        this.option.includeCols(cols);
        this.option.predicate(predicate);

        try {
            if (this.fsFactory.getFileSystem().isPresent())
            {
                this.pixelsReader = PixelsReaderImpl.newBuilder()
                        .setFS(this.fsFactory
                                .getFileSystem().get())
                        .setPath(new Path(split.getPath()))
                        .build();
            }
            else {
                logger.info("pixelsReader error: getFileSystem() null");
            }
        } catch (IOException e) {
            logger.info("pixelsReader error: " + e.getMessage());
            closeWithSuppression(e);
        }
    }

    public long getCompletedBytes() {
        return recordReader.getCompletedBytes();
    }

    public long getReadTimeNanos() {
        return ((this.nanoStart > 0L) ? ((this.nanoEnd == 0L) ? System.nanoTime() : this.nanoEnd) - this.nanoStart : 0L);
    }

    public boolean isFinished() {
        return this.closed;
    }

    public Page getNextPage() {
        if (this.nanoStart == 0L)
            this.nanoStart = System.nanoTime();
        try {
            this.batchId++;
            this.rowBatch = this.recordReader.readBatch(BATCH_SIZE);
            int batchSize = this.rowBatch.size;
            if (batchSize <= 0 || (endOfFile && batchId >1) ) {
                close();
                logger.info("getNextPage close");
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
                boolean isSlice = false;

                switch (typeName)
                {
                    case "integer":
                        LongColumnVector lcv = (LongColumnVector) cv;
                        for (int i = 0; i < this.rowBatch.size; ++i)
                        {
                            type.writeLong(blockBuilder, lcv.vector[i]);
                        }
                        break;
                    case "double":
                        DoubleColumnVector dcv = (DoubleColumnVector) cv;
                        for (int i = 0; i < this.rowBatch.size; ++i)
                        {
                            type.writeDouble(blockBuilder, dcv.vector[i]);
                        }
                        break;
                    case "varchar":
                    case "string":
                        BytesColumnVector scv = (BytesColumnVector) cv;
                        int size = 0;
                        for (int i = 0; i < this.rowBatch.size; ++i)
                        {
                            size += scv.vector[i].length;
                        }
                        Slice slice = Slices.allocate(size);
                        int positions = this.rowBatch.size;
                        boolean[] valueIsNull = new boolean[positions];
                        int[] offsets = new int[positions+1];
                        int index = 0;
                        for (int i = 0; i < positions; ++i)
                        {
                            if (scv.vector[i] == null)
                            {
                                valueIsNull[i] = true;
                                offsets[i+1] = offsets[i];
                            }
                            else
                            {
                                valueIsNull[i] = false;
                                offsets[i+1] = scv.vector[i].length;
                                slice.setBytes(index, scv.vector[i]);
                                index += scv.vector[i].length;
                            }
                        }
                        isSlice = true;
                        blocks[fieldId] = new VariableWidthBlock(positions, slice, offsets, valueIsNull);

                        /*
                        String sample = Slices.wrappedBuffer(scv.vector[0]).toByteBuffer().toString() + "******" +
                                new String(scv.vector[0]);
                        long start = System.nanoTime();
                        long size = 0;
                        for (int i = 0; i < this.rowBatch.size; ++i)
                        {
                            Slice slice = Slices.wrappedBuffer(scv.vector[i]);
                            size += scv.vector[i].length;
                            //blockBuilder.appendNull();
                            type.writeSlice(blockBuilder, slice);
                        }
                        long cost = System.nanoTime()-start;
                        logger.error("write slice cost: " + cost);
                        logger.error("row batch size in bytes: " + size + ", batch size: " + batchSize);
                        logger.error("slice sample: " + sample);
                        //logger.error("block builder class: " + blockBuilder.getClass().getName() + ", type class: " + type.getClass().getName() + ", type name: " + typeName);
                        */
                        break;
                    case "boolean":
                        LongColumnVector bcv = (LongColumnVector) cv;
                        for (int i = 0; i < this.rowBatch.size; ++i)
                        {
                            type.writeBoolean(blockBuilder, bcv.vector[i] == 1);
                        }
                        break;
                    default:
                        for (int i = 0; i < this.rowBatch.size; ++i)
                        {
                            blockBuilder.appendNull();
                        }
                        break;
                }
                if (!isSlice)
                {
                    blocks[fieldId] = blockBuilder.build().getRegion(0, batchSize);
                }
            }
            sizeOfData += batchSize;
            if (this.rowBatch.endOfFile) {
                endOfFile = true;
                logger.info("End of file");
            }
            return new Page(batchSize, blocks);
        } catch (IOException e) {
            closeWithSuppression(e);
        }
        return null;
    }

    @Override
    public long getSystemMemoryUsage() {
        return sizeOfData;
    }

    @Override
    public void close() {
        try {
            pixelsReader.close();
            rowBatch = null;
            nanoEnd = System.nanoTime();
        } catch (Exception e) {
            logger.info("close error: " + e.getMessage());
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