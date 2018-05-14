package cn.edu.ruc.iir.pixels.presto;

import cn.edu.ruc.iir.pixels.core.PixelsReader;
import cn.edu.ruc.iir.pixels.core.PixelsReaderImpl;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
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
import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
    private int batchId;
    private VectorizedRowBatch rowBatch;
    private long nanoStart;
    private long nanoEnd;

    //    @Inject
    public PixelsPageSource(PixelsConnectorId connectorId) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    }

    public PixelsPageSource(PixelsTable pixelsTable, List<PixelsColumnHandle> columnHandles, FSFactory fsFactory, String path, String connectorId) {
        this.connectorId = connectorId;
        ArrayList<Type> columnTypes = new ArrayList<>();
        for (PixelsColumnHandle column : columnHandles) {
            columnTypes.add(column.getColumnType());
        }
        this.types = columnTypes;
        this.pageBuilder = new PageBuilder(this.types);
        this.fsFactory = fsFactory;
        this.columns = columnHandles;
        this.path = path;
        getPixelsReaderBySchema();

        int size = columnHandles.size();
        this.constantBlocks = new Block[size];
        this.pixelsColumnIndexes = new int[size];
        this.recordReader = this.pixelsReader.read(this.option);
    }

    private void getPixelsReaderBySchema() {
        StringBuffer colStr = new StringBuffer();
        String schemaStr = "struct<";
        for (PixelsColumnHandle columnHandle : this.columns) {
            String name = columnHandle.getColumnName();
            String type = columnHandle.getColumnType().toString();
            colStr.append(new StringBuilder().append(name).append(",").toString());
            if (type.equals("integer")) {
                type = "int";
            }
            schemaStr = new StringBuilder().append(schemaStr).append(name).append(":").append(type).append(",").toString();
        }
        String colsStr = colStr.toString();
        logger.info(new StringBuilder().append("getPixelsReaderBySchema colStr: ").append(colsStr).toString());
        String[] cols = colsStr.substring(0, colsStr.length() - 1).split(",");

        schemaStr = new StringBuilder().append(schemaStr.substring(0, schemaStr.length() - 1)).append(">").toString();
        this.schema = TypeDescription.fromString(schemaStr);

        this.option = new PixelsReaderOption();
        this.option.skipCorruptRecords(true);
        this.option.tolerantSchemaEvolution(true);
        this.option.includeCols(cols);

        schemaStr = new StringBuilder().append(schemaStr.substring(0, schemaStr.length() - 1)).append(">").toString();
        logger.info(new StringBuilder().append("PixelsPageResource Schema: ").append(schemaStr).toString());
        this.schema = TypeDescription.fromString(schemaStr);
        try {
            this.pixelsReader = PixelsReaderImpl.newBuilder()
                    .setFS(this.fsFactory
                            .getFileSystem().get())
                    .setPath(new Path(path))
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
            if (batchSize <= 0) {
                close();
                logger.info("getNextPage close");
                return null;
            }
            logger.info(new StringBuilder().append("getNextPage batchId: ").append(this.batchId).toString());
            logger.info(new StringBuilder().append("getNextPage rowBatch: ").append(this.rowBatch.size).toString());
            logger.info(new StringBuilder().append("getNextPage batchSize: ").append(batchSize).toString());
            Block[] blocks = new Block[this.pixelsColumnIndexes.length];
//            logger.info(new StringBuilder().append("getNextPage blocks: ").append(blocks.length).toString());

            for (int fieldId = 0; fieldId < blocks.length; ++fieldId)
            {
                Type type = types.get(fieldId);
                BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), 1024, 0);
                String typeName = type.getDisplayName();
                int projIndex = this.rowBatch.projectedColumns[fieldId];
                ColumnVector cv = this.rowBatch.cols[projIndex];
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
//                blocks[fieldId] = blockBuilder.build().getRegion(0, batchSize);
                blocks[fieldId] = new LazyBlock(batchSize, new LazyBlockLoader<LazyBlock>() {
                    @Override
                    public void load(LazyBlock block) {
                        block.setBlock(blockBuilder.build());
                    }
                });
            }

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