package cn.edu.ruc.iir.pixels.presto;

import cn.edu.ruc.iir.pixels.core.PixelsReader;
import cn.edu.ruc.iir.pixels.core.PixelsReaderImpl;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.reader.PixelsReaderOption;
import cn.edu.ruc.iir.pixels.core.reader.PixelsRecordReader;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import cn.edu.ruc.iir.pixels.presto.impl.FSFactory;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.*;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Pixels Page Source class for custom Pixels RecordSet Iteration.
 */
class PixelsPageSource implements ConnectorPageSource {

    private static Logger logger = Logger.get(PixelsPageSource.class);
    private final int MAX_BATCH_SIZE = 1024;
    private final int NULL_ENTRY_SIZE = 0;
    private List<PixelsColumnHandle> columns;
    private List<Type> types;
    private FSFactory fsFactory;
    private PageBuilder pageBuilder;
    private static String path;
    private boolean closed;
    private PixelsReader pixelsReader;
    private PixelsRecordReader recordReader;
    private long sizeOfData = 0;
    private TypeDescription schema;
    private PixelsReaderOption option;
    private Block[] constantBlocks;
    private int[] pixelsColumnIndexes;
    private final String connectorId;
    private int batchId;
    private VectorizedRowBatch rowBatch;
    private long nanoStart;
    private long nanoEnd;

    @Inject
    public PixelsPageSource(PixelsConnectorId connectorId) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    }

    public PixelsPageSource(PixelsTable pixelsTable, List<PixelsColumnHandle> columnHandles, FSFactory fsFactory, String path, String connectorId) {
        this.connectorId = connectorId;
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        for (PixelsColumnHandle column : columnHandles) {

            columnTypes.add(column.getColumnType());
        }
        this.types = new ArrayList<>(columnTypes.build());
        this.pageBuilder = new PageBuilder(this.types);
        this.fsFactory = fsFactory;
        this.columns = columnHandles;
        this.path = path;
        getPixelsReaderBySchema();
        this.rowBatch = this.schema.createRowBatch();

        int size = columnHandles.size();
        this.constantBlocks = new Block[size];
        this.pixelsColumnIndexes = new int[size];
        this.recordReader = pixelsReader.read(option);

        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            pixelsColumnIndexes[columnIndex] = columnIndex;

            BlockBuilder blockBuilder = this.types.get(columnIndex).createBlockBuilder(new BlockBuilderStatus(), MAX_BATCH_SIZE, NULL_ENTRY_SIZE);
            StringBuilder b = new StringBuilder();
            for (int i = 0; i < this.rowBatch.size; i++) {
                int projIndex = this.rowBatch.projectedColumns[columnIndex];
                ColumnVector cv = this.rowBatch.cols[projIndex];
                if (cv != null) {
                    cv.stringifyValue(b, i);
                }
                blockBuilder.writeLong(Long.parseLong(b.toString()));
            }

//            for (int i = 0; i < MAX_BATCH_SIZE; i++) {
//                blockBuilder.appendNull();
//            }
            constantBlocks[columnIndex] = blockBuilder.build();
        }
    }

    private void getPixelsReaderBySchema() {
        StringBuffer colStr = new StringBuffer();
        String schemaStr = "struct<";
        for (PixelsColumnHandle columnHandle : this.columns) {
            String name = columnHandle.getColumnName();
            String type = columnHandle.getColumnType().toString();
            colStr.append(name + ",");
            if (type.equals("integer")) {
                type = "int";
            }
            schemaStr += name + ":" + type + ",";
        }
        String colsStr = colStr.toString();
        logger.info("getPixelsReaderBySchema colStr: " + colsStr);
        String[] cols = colsStr.substring(0, colsStr.length() - 1).split(",");

        schemaStr = schemaStr.substring(0, schemaStr.length() - 1) + ">";
        this.schema = TypeDescription.fromString(schemaStr);

        this.option = new PixelsReaderOption();
        this.option.skipCorruptRecords(true);
        this.option.tolerantSchemaEvolution(true);
        this.option.includeCols(cols);

        schemaStr = schemaStr.substring(0, schemaStr.length() - 1) + ">";
        logger.info("PixelsPageResource Schema: " + schemaStr);
        this.schema = TypeDescription.fromString(schemaStr);

        try {
            this.pixelsReader = PixelsReaderImpl.newBuilder()
                    .setFS(fsFactory.getFileSystem().get())
                    .setPath(new Path(path))
                    .setSchema(schema)
                    .build();
        } catch (IOException e) {
            e.printStackTrace();

        }
    }

    @Override
    public long getCompletedBytes() {
        return sizeOfData;
    }

    @Override
    public long getReadTimeNanos() {
        return nanoStart > 0L ? (nanoEnd == 0 ? System.nanoTime() : nanoEnd) - nanoStart : 0L;
    }

    @Override
    public boolean isFinished() {
        return closed && pageBuilder.isEmpty();
    }


    @Override
    public Page getNextPage() {
        if (nanoStart == 0) {
            nanoStart = System.nanoTime();
        }
        try {
            batchId++;
            recordReader.readBatch(rowBatch);
            int batchSize = rowBatch.size;
            if (batchSize <= 0) {
                close();
                return null;
            }
            logger.info("getNextPage batchId: " + batchId);
            logger.info("getNextPage rowBatch: " + rowBatch.size);
            logger.info("getNextPage pixelsColumnIndexes: " + pixelsColumnIndexes.length);
            Block[] blocks = new Block[pixelsColumnIndexes.length];
            logger.info("getNextPage blocks: " + blocks.length);

            for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
                Type type = types.get(fieldId);
                if (constantBlocks[fieldId] != null) {
                    logger.info("constantBlocks[fieldId] != null");
                    blocks[fieldId] = constantBlocks[fieldId].getRegion(0, batchSize);
                } else {
                    logger.info("constantBlocks[fieldId] == null");
                    blocks[fieldId] = new LazyBlock(batchSize, new PixelsBlockLoader(pixelsColumnIndexes[fieldId], type));
                }
            }
            logger.info("getNextPage batchSize: " + batchSize);
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
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;
        try {
            pixelsReader.close();
            nanoEnd = System.nanoTime();
        } catch (Exception e) {
            logger.info("close error: " + e.getMessage());
        }

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