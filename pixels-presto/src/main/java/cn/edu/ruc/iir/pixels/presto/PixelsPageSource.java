package cn.edu.ruc.iir.pixels.presto;

import cn.edu.ruc.iir.pixels.core.PixelsReader;
import com.facebook.presto.hadoop.$internal.com.google.common.base.Throwables;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;
import org.apache.pixels.presto.readers.StreamReader;
import org.apache.pixels.presto.readers.StreamReaders;
import org.apache.pixels.processing.loading.exception.CarbonDataLoadingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

/**
 * Pixels Page Source class for custom Pixels RecordSet Iteration.
 */
class PixelsPageSource implements ConnectorPageSource {

    private static Logger logger = Logger.get(PixelsPageSource.class);
    private final RecordCursor cursor;
    private final List<Type> types;
    private final PageBuilder pageBuilder;
    private boolean closed;
    private PixelsReader vectorReader;
    private long sizeOfData = 0;

    private final StreamReader[] readers;
    private int batchId;

    private long nanoStart;
    private long nanoEnd;

    PixelsPageSource(RecordSet recordSet) {
        this(requireNonNull(recordSet, "recordSet is null").getColumnTypes(),
                recordSet.cursor());
    }

    private PixelsPageSource(List<Type> types, RecordCursor cursor) {
        this.cursor = requireNonNull(cursor, "cursor is null");
        this.types = unmodifiableList(new ArrayList<>(requireNonNull(types, "types is null")));
        this.pageBuilder = new PageBuilder(this.types);
        this.vectorReader = ((PixelsRecordCursor) cursor).getPixelsReader();
        this.readers = createStreamReaders();
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
        CarbonVectorBatch columnarBatch = null;
        int batchSize = 0;
        try {
            batchId++;
            if (vectorReader.nextKeyValue()) {
                Object vectorBatch = vectorReader.getCurrentValue();
                if (vectorBatch != null && vectorBatch instanceof CarbonVectorBatch) {
                    columnarBatch = (CarbonVectorBatch) vectorBatch;
                    batchSize = columnarBatch.numRows();
                    if (batchSize == 0) {
                        close();
                        return null;
                    }
                }
            } else {
                close();
                return null;
            }
            if (columnarBatch == null) {
                return null;
            }

            Block[] blocks = new Block[types.size()];
            for (int column = 0; column < blocks.length; column++) {
                Type type = types.get(column);
                readers[column].setBatchSize(columnarBatch.numRows());
                readers[column].setVectorReader(true);
                readers[column].setVector(columnarBatch.column(column));
                blocks[column] = new LazyBlock(batchSize, new PixelsBlockLoader(column, type));
            }
            Page page = new Page(batchSize, blocks);
            sizeOfData += columnarBatch.capacity();
            return page;
        } catch (PrestoException e) {
            closeWithSuppression(e);
            throw e;
        } catch (RuntimeException | InterruptedException | IOException e) {
            closeWithSuppression(e);
        }
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
            vectorReader.close();
            cursor.close();
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
            try {
                Block block = readers[columnIndex].readBlock(type);
                lazyBlock.setBlock(block);
            } catch (IOException e) {
            }
            loaded = true;
        }
    }


    /**
     * Create the Stream Reader for every column based on their type
     * This method will be initialized only once based on the types.
     *
     * @return
     */
    private StreamReader[] createStreamReaders() {
        requireNonNull(types);
        StreamReader[] readers = new StreamReader[types.size()];
        for (int i = 0; i < types.size(); i++) {
            readers[i] = StreamReaders.createStreamReader(types.get(i), readSupport
                    .getSliceArrayBlock(i), readSupport.getDictionaries()[i]);
        }
        return readers;
    }

}