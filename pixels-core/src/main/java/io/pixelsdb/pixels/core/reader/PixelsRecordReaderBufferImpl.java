/*
 * Copyright 2025 PixelsDB.
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

package io.pixelsdb.pixels.core.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.retina.RetinaProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PixelsRecordReaderBufferImpl implements PixelsRecordReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PixelsRecordReaderBufferImpl.class);
    private final byte[] activeMemtableData;
    private final String retinaHost;
    /**
     * Columns included by reader option; if included, set true
     */
    private boolean[] includedColumns;
    /**
     * The ith element in resultColumns is the column id (column's index in the file schema)
     * of ith included column in the read option. The order of columns in the read option's
     * includedCols may be arbitrary, not related to the column order in schema.
     */
    private int[] resultColumns;
    /**
     * The target columns to read after matching reader option.
     * Each element represents a column id (column's index in the file schema).
     * Different from resultColumns, the ith column id in targetColumns
     * corresponds to the ith true value in this.includedColumns, i.e.,
     * The elements in targetColumns and resultColumns are in different order,
     * but they are all the index of the columns in the file schema.
     */
    private int[] targetColumns;

    private final List<Long> fileIds;
    private int fileIdIndex = 0;
    private final PixelsReaderOption option;
    private final Storage storage;

    private final long tableId;
    private final String retinaBufferStorageFolder;
    private final boolean retinaEnabled;

    private final TypeDescription typeDescription;
    private final int colNum;
    private int includedColumnNum = 0;
    private long readTimeNanos = 0L;
    private boolean checkValid = false;
    private boolean activeMemtableDataEverRead = false;
    private boolean everRead;
    private boolean endOfFile = false;
    private final int typeMode = TypeDescription.Mode.CREATE_INT_VECTOR_FOR_INT;
    private static final Long POLL_INTERVAL_MILLS = 200L;

    private final ExecutorService prefetchExecutor; // Thread pool for I/O and deserialization
    private final BlockingQueue<VectorizedRowBatch> prefetchQueue; // Queue for completed batches
    private final AtomicInteger pendingTasks = new AtomicInteger(0); // Counter for submitted but unfinished tasks
    private final AtomicBoolean initialMemtableSubmitted = new AtomicBoolean(false); // Flag for active memtable
    private final int maxPrefetchTasks; // Max concurrent tasks
    private final int prefetchQueueCapacity; // Queue capacity
    private static final int DEFAULT_QUEUE_CAPACITY = 16;

    private final boolean shouldReadHiddenColumn;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final List<RetinaProto.VisibilityBitmap> visibilityBitmap;
    private TypeDescription resultSchema = null;
    private final List<PixelsProto.Type> includedColumnTypes;
    private long dataReadBytes = 0L;
    private long dataReadRow = 0L;

    private static boolean checkBit(RetinaProto.VisibilityBitmap bitmap, int k)
    {
        long bitmap_ = bitmap.getBitmap(k / 64);
        return (bitmap_ & (1L << (k % 64))) != 0;
    }

    public PixelsRecordReaderBufferImpl(PixelsReaderOption option,
                                        String retinaHost,
                                        byte[] activeMemtableData, List<Long> fileIds,  // read version
                                        List<RetinaProto.VisibilityBitmap> visibilityBitmap,
                                        Storage storage,
                                        long tableId, // to locate file with file id
                                        TypeDescription typeDescription
    ) throws IOException
    {
        ConfigFactory configFactory = ConfigFactory.Instance();
        this.retinaBufferStorageFolder = configFactory.getProperty("retina.buffer.object.storage.folder");
        this.retinaHost = retinaHost;
        this.retinaEnabled = Boolean.parseBoolean(configFactory.getProperty("retina.enable"));

        this.option = option;
        this.activeMemtableData = activeMemtableData;
        this.fileIds = fileIds;
        this.storage = storage;
        this.tableId = tableId;
        this.typeDescription = typeDescription;
        this.colNum = typeDescription.getChildrenWithHiddenColumn().size();
        this.shouldReadHiddenColumn = option.hasValidTransTimestamp();
        this.visibilityBitmap = visibilityBitmap;
        this.includedColumnTypes = new ArrayList<>();
        this.everRead = false;

        this.maxPrefetchTasks = Integer.parseInt(configFactory.getProperty("retina.reader.prefetch.threads"));
        this.prefetchQueueCapacity = DEFAULT_QUEUE_CAPACITY;

        this.prefetchExecutor = Executors.newFixedThreadPool(maxPrefetchTasks, r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("Pixels-Buffer-Reader-Prefetch");
            t.setDaemon(true);
            return t;
        });
        this.prefetchQueue = new LinkedBlockingQueue<>(prefetchQueueCapacity);

        checkBeforeRead();
    }
    private void startPrefetching()
    {
        // Submit Active Memtable Task (executed only once)
        if (activeMemtableData != null && activeMemtableData.length != 0 && initialMemtableSubmitted.compareAndSet(false, true))
        {
            pendingTasks.incrementAndGet();
            prefetchExecutor.submit(() -> {
                try {
                    ByteBuffer buffer = ByteBuffer.wrap(activeMemtableData);
                    VectorizedRowBatch batch = VectorizedRowBatch.deserialize(buffer);
                    prefetchQueue.put(batch);
                } catch (Exception e) {
                    LOGGER.error("Failed to deserialize active memtable data", e);
                } finally {
                    pendingTasks.decrementAndGet();
                }
            });
        }

        // Submit File ID Tasks (while under max concurrency and not EOF)
        // This task will continue to run in the background, submitting more tasks
        // as the pendingTasks count drops.
        prefetchExecutor.submit(() -> {
            while (fileIdIndex < fileIds.size()) {
                if (pendingTasks.get() >= maxPrefetchTasks) {
                    // Backpressure: Wait if the thread pool is full
                    try {
                        Thread.sleep(POLL_INTERVAL_MILLS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    continue;
                }

                pendingTasks.incrementAndGet();
                final int currentIndex = fileIdIndex++;
                final long fileId = fileIds.get(currentIndex);

                prefetchExecutor.submit(() -> {
                    ByteBuffer buffer = null;
                    try {
                        // I/O Blocking Operation
                        String path = getRetinaBufferStoragePathFromId(fileId);
                        buffer = getMemtableDataFromStorage(path);

                        // CPU Intensive Operation
                        VectorizedRowBatch batch = VectorizedRowBatch.deserialize(buffer);

                        // Put result into the queue (blocks if queue is full)
                        prefetchQueue.put(batch);
                    } catch (Exception e) {
                        LOGGER.error("Failed to prefetch and deserialize file ID: " + fileId, e);
                    } finally {
                        pendingTasks.decrementAndGet();
                    }
                });
            }
        });
    }
    private void checkBeforeRead() throws IOException
    {
        // filter included columns
        includedColumnNum = 0;
        String[] optionIncludedCols = option.getIncludedCols();
        // if size of cols is 0, create an empty row batch

        List<Integer> optionColsIndices = new ArrayList<>();
        this.includedColumns = new boolean[colNum];
        for (String col : optionIncludedCols)
        {
            for (int j = 0; j < colNum; j++)
            {
                if (col.equalsIgnoreCase(typeDescription.getFieldNames().get(j)))
                {
                    optionColsIndices.add(j);
                    includedColumns[j] = true;
                    includedColumnNum++;
                    break;
                }
            }
        }

        // check included columns
        if (includedColumnNum != optionIncludedCols.length && !option.isTolerantSchemaEvolution())
        {
            checkValid = false;
            throw new IOException("includedColumnsNum is " + includedColumnNum +
                    " whereas optionIncludedCols.length is " + optionIncludedCols.length);
        }

        // check retina
        if (retinaEnabled && visibilityBitmap != null && visibilityBitmap.size() != fileIds.size() + 1)
        {
            checkValid = false;
            throw new IOException("visibilityBitmap.getSize is " + visibilityBitmap.size() +
                    "except: " + fileIds.size() + 1);
        }

        // create result columns storing result column ids in user specified order
        this.resultColumns = new int[optionIncludedCols.length];
        for (int i = 0; i < optionIncludedCols.length; i++)
        {
            this.resultColumns[i] = optionColsIndices.get(i);
        }
        // assign target columns, ordered by original column order in schema
        int targetColumnNum = new HashSet<>(optionColsIndices).size();
        targetColumns = new int[targetColumnNum];
        int targetColIdx = 0;
        for (int i = 0; i < includedColumns.length; i++)
        {
            if (includedColumns[i])
            {
                targetColumns[targetColIdx] = i;
                targetColIdx++;
            }
        }
        checkValid = true;
    }

    /**
     * read() is now non-blocking and only triggers the submission of prefetch tasks.
     * It does not perform I/O or deserialization.
     */
    private boolean read() throws IOException
    {
        if(!everRead)
        {
            startPrefetching();
            everRead = true;
        }
        if (fileIdIndex >= fileIds.size() && pendingTasks.get() == 0)
        {
            endOfFile = true;
        }
        return checkValid;
        // Always return true to signal the loop to check the prefetch queue
    }

    @Override
    public int prepareBatch(int batchSize) throws IOException
    {
        return batchSize;
    }

    /**
     * Create a row batch without any data, only sets the number of rows (size) and OEF.
     * Such a row batch is used for queries such as select count(*).
     *
     * @param size the number of rows in the row batch.
     * @return the empty row batch.
     */
    private VectorizedRowBatch createEmptyRowBatch(int size)
    {
        TypeDescription resultSchema = TypeDescription.createSchema(new ArrayList<>());
        VectorizedRowBatch resultRowBatch = resultSchema.createRowBatch(0, this.typeMode);
        resultRowBatch.projectionSize = 0;
        resultRowBatch.endOfFile = this.endOfFile;
        resultRowBatch.size = size;
        return resultRowBatch;
    }

    @Override
    public VectorizedRowBatch readBatch() throws IOException
    {
        long start = System.nanoTime();
        if (!read())
        {
            return createEmptyRowBatch(0);
        }

        VectorizedRowBatch curRowBatch = null;
        try
        {
            // Block and wait for the next batch to be available
            curRowBatch = prefetchQueue.poll(POLL_INTERVAL_MILLS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for prefetched batch.", e);
        }

        // Check for EOF condition
        if (curRowBatch == null)
        {
            return createEmptyRowBatch(0);
        }

        LongColumnVector hiddenTimestampVector = (LongColumnVector) curRowBatch.cols[this.colNum - 1];
        /**
         * construct the selected rows bitmap, size is curBatchSize
         * the i-th bit presents the curRowInRG + i row in chunkBuffers is selected or not.
         */
        int curBatchSize = curRowBatch.size;
        Bitmap selectedRows = new Bitmap(curBatchSize, false);
        int addedRows = 0;
        for (int i = 0; i < curBatchSize; i++)
        {
            if ((hiddenTimestampVector == null || hiddenTimestampVector.vector[i] <= this.option.getTransTimestamp())
                    && (!retinaEnabled || visibilityBitmap == null || !checkBit(visibilityBitmap.get(fileIdIndex), i)))
            {
                selectedRows.set(i);
                addedRows++;
            }
        }
        curRowBatch.applyFilter(selectedRows);
        dataReadRow += curRowBatch.size;
        readTimeNanos += System.nanoTime() - start;
        return curRowBatch;
    }

    @Override
    public TypeDescription getResultSchema()
    {
        // TODO(AntiO2): Schema evolution is currently not supported in Retina.
        return typeDescription;
    }

    @Override
    public boolean isValid()
    {
        return false;
    }

    @Override
    public boolean isEndOfFile()
    {
        return endOfFile;
    }

    @Override
    public boolean seekToRow(long rowIndex) throws IOException
    {
        return false;
    }

    @Override
    public boolean skip(long rowNum) throws IOException
    {
        return false;
    }

    @Override
    public long getCompletedRows()
    {
        return dataReadRow;
    }

    @Override
    public long getCompletedBytes()
    {
        return dataReadBytes;
    }

    @Override
    public int getNumReadRequests()
    {
        return fileIdIndex;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close() throws IOException
    {
        prefetchExecutor.shutdownNow();
    }

    private String getRetinaBufferStoragePathFromId(long entryId)
    {
        return this.retinaBufferStorageFolder + String.format("%d/%s_%d", tableId, retinaHost, entryId);
    }

    private ByteBuffer getMemtableDataFromStorage(String path) throws IOException
    {
        // Polling loop for file existence runs inside the prefetchExecutor's worker thread.
        // This loop will continue indefinitely until the file is successfully read.
        while (true)
        {

            if (storage.exists(path))
            {
                try (PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(storage, path))
                {
                    int length = (int) reader.getFileLength();
                    dataReadBytes += length;
                    return reader.readFully(length);
                }
            }

            try
            {
                Thread.sleep(POLL_INTERVAL_MILLS);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for file existence: " + path, e);
            }
        }
    }


    @Override
    public VectorizedRowBatch readBatch(int batchSize, boolean reuse) throws IOException
    {
        return readBatch();
    }

    @Override
    public VectorizedRowBatch readBatch(int batchSize) throws IOException
    {
        return readBatch();
    }

    @Override
    public VectorizedRowBatch readBatch(boolean reuse) throws IOException
    {
        return readBatch();
    }
}
