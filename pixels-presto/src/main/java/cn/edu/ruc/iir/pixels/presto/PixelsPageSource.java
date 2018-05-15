package cn.edu.ruc.iir.pixels.presto;

import cn.edu.ruc.iir.pixels.core.PixelsPredicate;
import cn.edu.ruc.iir.pixels.core.PixelsReader;
import cn.edu.ruc.iir.pixels.core.PixelsReaderImpl;
import cn.edu.ruc.iir.pixels.core.TupleDomainPixelsPredicate;
import cn.edu.ruc.iir.pixels.core.reader.PixelsReaderOption;
import cn.edu.ruc.iir.pixels.core.reader.PixelsRecordReader;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.DoubleColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import cn.edu.ruc.iir.pixels.presto.impl.FSFactory;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.*;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

class PixelsPageSource implements ConnectorPageSource {
    private static Logger logger = Logger.get(PixelsPageSource.class);
    private final int MAX_BATCH_SIZE = 1024;
//    private final int NULL_ENTRY_SIZE = 0;
    private List<PixelsColumnHandle> columns;
    private List<Type> types;
    private FSFactory fsFactory;
//    private String path;
    private boolean closed;
    private boolean endOfFile;
    private PixelsReader pixelsReader;
    private PixelsRecordReader recordReader;
    private long sizeOfData = 0L;
    private PixelsReaderOption option;
//    private Block[] constantBlocks;
    private final String connectorId;
    private int numColumnToRead;
    private int batchId;
    private VectorizedRowBatch rowBatch;
    private long nanoStart;
    private long nanoEnd;

    public PixelsPageSource(PixelsSplit split, List<PixelsColumnHandle> columnHandles, FSFactory fsFactory, String connectorId) {
        this.connectorId = connectorId;
        ArrayList<Type> columnTypes = new ArrayList<>();
        for (PixelsColumnHandle column : columnHandles) {
            logger.info("column type: " + column.getColumnType());
            columnTypes.add(column.getColumnType());
        }
        this.types = columnTypes;
        this.fsFactory = fsFactory;
        this.columns = columnHandles;
//        this.path = split.getPath();

        getPixelsReaderBySchema(split);

        this.numColumnToRead = columnHandles.size();
//        this.constantBlocks = new Block[size];
//        this.pixelsColumnIndexes = new int[size];
        this.recordReader = this.pixelsReader.read(this.option);
    }

    private void getPixelsReaderBySchema(PixelsSplit split) {
        StringBuilder colStr = new StringBuilder();
        for (PixelsColumnHandle columnHandle : this.columns) {
            String name = columnHandle.getColumnName();
            colStr.append(name).append(",");
        }
        String colsStr = colStr.toString();
        String[] cols = new String[0];
        if(colsStr.length() > 0){
            String col = colsStr.substring(0, colsStr.length() - 1);
            cols = col.split(",");
            logger.info("getPixelsReaderBySchema col: " + col);
        }

        Map<PixelsColumnHandle, Domain> domains = split.getConstraint().getDomains().get();
        List<TupleDomainPixelsPredicate.ColumnReference<PixelsColumnHandle>> columnReferences = new ArrayList<>();
        for (Map.Entry<PixelsColumnHandle, Domain> entry : domains.entrySet()) {
            PixelsColumnHandle column = entry.getKey();
            logger.info("column: " + column.getColumnName() + " " + column.getColumnType() + " " + column.getOrdinalPosition());
            columnReferences.add(new TupleDomainPixelsPredicate.ColumnReference<>(column, column.getOrdinalPosition(), column.getColumnType()));
        }
        PixelsPredicate predicate = new TupleDomainPixelsPredicate<>(split.getConstraint(), columnReferences);

        this.option = new PixelsReaderOption();
        this.option.skipCorruptRecords(true);
        this.option.tolerantSchemaEvolution(true);
        this.option.includeCols(cols);
        this.option.predicate(predicate);

        try {
            if(this.fsFactory.getFileSystem().isPresent()){
                this.pixelsReader = PixelsReaderImpl.newBuilder()
                        .setFS(this.fsFactory
                                .getFileSystem().get())
                        .setPath(new Path(split.getPath()))
                        .build();
            }else {
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
            this.rowBatch = this.recordReader.readBatch(10000);
            int batchSize = this.rowBatch.size;
            if (batchSize <= 0 || (endOfFile && batchId >1) ) {
                close();
                logger.info("getNextPage close");
                return null;
            }
            Block[] blocks = new Block[this.numColumnToRead];

            for (int fieldId = 0; fieldId < blocks.length; ++fieldId)
            {
                Type type = types.get(fieldId);
                String typeName = type.getDisplayName();
                int projIndex = this.rowBatch.projectedColumns[fieldId];
                ColumnVector cv = this.rowBatch.cols[projIndex];
                BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), batchSize, 0);
                if (typeName.equals("integer"))
                {
                    LongColumnVector lcv = (LongColumnVector) cv;
                    for (int i = 0; i < this.rowBatch.size; ++i)
                    {
                        type.writeLong(blockBuilder, lcv.vector[i]);
                    }
                }
                else if (typeName.equals("double"))
                {
                    DoubleColumnVector dcv = (DoubleColumnVector) cv;
                    for (int i = 0; i < this.rowBatch.size; ++i)
                    {
                        type.writeDouble(blockBuilder, dcv.vector[i]);
                    }
                }
                else
                {
                    for (int i = 0; i < this.rowBatch.size; ++i)
                    {
                        blockBuilder.appendNull();
                    }
                }
                blocks[fieldId] = blockBuilder.build().getRegion(0, batchSize);
//                blocks[fieldId] = new LazyBlock(batchSize, new LazyBlockLoader<LazyBlock>() {
//                    @Override
//                    public void load(LazyBlock block) {
//                        block.setBlock(blockBuilder.build());
//                    }
//                });
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

    protected void closeWithSuppression(Throwable throwable) {
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
            BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), MAX_BATCH_SIZE);
            Block block = builder.build();
            lazyBlock.setBlock(block);
            loaded = true;
        }
    }

}