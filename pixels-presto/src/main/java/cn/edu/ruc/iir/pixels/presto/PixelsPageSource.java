package cn.edu.ruc.iir.pixels.presto;

import cn.edu.ruc.iir.pixels.core.*;
import cn.edu.ruc.iir.pixels.core.reader.PixelsReaderOption;
import cn.edu.ruc.iir.pixels.core.reader.PixelsRecordReader;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.DoubleColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.LongColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import cn.edu.ruc.iir.pixels.presto.impl.FSFactory;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.*;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
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
    private final int NULL_ENTRY_SIZE = 0;
    private List<PixelsColumnHandle> columns;
    private List<Type> types;
    private FSFactory fsFactory;
    private PageBuilder pageBuilder;
    private String path;
    private boolean closed;
    private PixelsReader pixelsReader;
    private PixelsRecordReader recordReader;
    private long sizeOfData = 0L;
    private TypeDescription schema;
    private PixelsReaderOption option;
    private Block[] constantBlocks;
    private int[] pixelsColumnIndexes;
    private final String connectorId;
    private int size;
    private int batchId;
    private VectorizedRowBatch rowBatch;
    private long nanoStart;
    private long nanoEnd;

    //    @Inject
    public PixelsPageSource(PixelsConnectorId connectorId) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    }

    public PixelsPageSource(PixelsSplit split, List<PixelsColumnHandle> columnHandles, FSFactory fsFactory, String connectorId) {
        this.connectorId = connectorId;
        ArrayList<Type> columnTypes = new ArrayList<>();
        for (PixelsColumnHandle column : columnHandles) {
            logger.info("column type: " + column.getColumnType());
            columnTypes.add(column.getColumnType());
        }
        this.types = columnTypes;
        this.pageBuilder = new PageBuilder(this.types);
        this.fsFactory = fsFactory;
        this.columns = columnHandles;
//        this.path = split.getPath();

        getPixelsReaderBySchema(split);

        this.size = columnHandles.size();
//        this.constantBlocks = new Block[size];
//        this.pixelsColumnIndexes = new int[size];
        this.recordReader = this.pixelsReader.read(this.option);
    }

    private void getPixelsReaderBySchema(PixelsSplit split) {
        StringBuffer colStr = new StringBuffer();
        for (PixelsColumnHandle columnHandle : this.columns) {
            String name = columnHandle.getColumnName();
            String type = columnHandle.getColumnType().toString();
            colStr.append(new StringBuilder().append(name).append(",").toString());
            if (type.equals("integer")) {
                type = "int";
            }
        }
        String colsStr = colStr.toString();
        logger.info("getPixelsReaderBySchema col: " + colsStr);
        logger.info("getPixelsReaderBySchema col: " + colsStr.length());

        String[] cols = new String[0];
        if(colsStr.length() > 0){
            String col = colsStr.substring(0, colsStr.length() - 1);
            cols = col.split(",");
            logger.info("getPixelsReaderBySchema col: " + col);
        }

//        Map<PixelsColumnHandle, Domain> domains = split.getConstraint().getDomains().get();
//        List<TupleDomainPixelsPredicate.ColumnReference<String>> columnReferences = ImmutableList.<TupleDomainPixelsPredicate.ColumnReference<String>>builder().build();
//        int num = 0;
//        for (Map.Entry<PixelsColumnHandle, Domain> entry : domains.entrySet()) {
//            PixelsColumnHandle column = entry.getKey();
//            logger.info("column: " + column.getColumnName() + " " + column.getColumnType());
//            columnReferences.add(new TupleDomainPixelsPredicate.ColumnReference<>(column.getColumnName(), num++, column.getColumnType()));
//        }
//        PixelsPredicate predicate = new TupleDomainPixelsPredicate(split.getConstraint(), columnReferences);

        this.option = new PixelsReaderOption();
        this.option.skipCorruptRecords(true);
        this.option.tolerantSchemaEvolution(true);
        this.option.includeCols(cols);
//        this.option.predicate(predicate);

        try {
            this.pixelsReader = PixelsReaderImpl.newBuilder()
                    .setFS(this.fsFactory
                            .getFileSystem().get())
                    .setPath(new Path(split.getPath()))
                    .build();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long getCompletedBytes() {
        return recordReader.getCompletedBytes();
    }

    public long getReadTimeNanos() {
        return ((this.nanoStart > 0L) ? ((this.nanoEnd == 0L) ? System.nanoTime() : this.nanoEnd) - this.nanoStart : 0L);
    }

    public boolean isFinished() {
        return ((this.closed) && (this.pageBuilder.isEmpty()));
    }

    public Page getNextPage() {
        if (this.nanoStart == 0L)
            this.nanoStart = System.nanoTime();
        try {
            this.batchId += 1;
            this.rowBatch = this.recordReader.readBatch(10000);
            int batchSize = this.rowBatch.size;
            if (batchSize <= 0 || (batchSize > 10000 && batchId >1) ) {
                close();
                logger.info("getNextPage close");
                return null;
            }
            Block[] blocks = new Block[this.size];

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
                System.out.println("End of file");
            }
            return new Page(batchSize, blocks);
        } catch (IOException e) {
            e.printStackTrace();
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