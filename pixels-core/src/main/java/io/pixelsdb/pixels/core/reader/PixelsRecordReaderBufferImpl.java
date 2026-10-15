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

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.retina.RetinaProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class PixelsRecordReaderBufferImpl implements PixelsRecordReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PixelsRecordReaderBufferImpl.class);
    private static final Long POLL_INTERVAL_MILLS = 200L;
    private static final int DEFAULT_QUEUE_CAPACITY = 16;
    private final byte[] activeMemtableData;
    private final String retinaHost;
    private final List<Long> fileIds;
    private final PixelsReaderOption option;
    private final Storage storage;
    private final long tableId;
    private final String retinaBufferStorageFolder;
    private final boolean retinaEnabled;
    private final TypeDescription typeDescription;
    private final int colNum;
    private final int vectorLayout;
    private static ExecutorService prefetchExecutor; // Thread pool for I/O and deserialization
    private static int maxPrefetchTasks;
    private final boolean shouldReadHiddenColumn;
    private final List<RetinaProto.VisibilityBitmap> visibilityBitmap;
    private final AtomicLong memoryUsage = new AtomicLong(0L);
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
    private int fileIdIndex = 0;
    private int includedColumnNum = 0;
    private long readTimeNanos = 0L;
    private boolean checkValid = false;
    private boolean endOfFile = false;
    private long dataReadBytes = 0L;
    private long dataReadRow = 0L;
    private int vNodeId;
    private static final class LoadedBatch
    {
        final VectorizedRowBatch batch;
        final int bitmapIndex;
        LoadedBatch(VectorizedRowBatch batch, int bitmapIndex)
        { this.batch = batch; this.bitmapIndex = bitmapIndex; }
    }
    private final java.util.ArrayDeque<java.util.concurrent.Future<LoadedBatch>> orderedPrefetch = new java.util.ArrayDeque<>();
    private int nextSource;
    private volatile boolean closed;

    public PixelsRecordReaderBufferImpl(PixelsReaderOption option,
                                        String retinaHost,
                                        byte[] activeMemtableData, List<Long> fileIds,  // read version
                                        List<RetinaProto.VisibilityBitmap> visibilityBitmap,
                                        Storage storage,
                                        long tableId, // to locate file with file id
                                        int vNodeId,
                                        TypeDescription typeDescription
    ) throws IOException
    {
        ConfigFactory configFactory = ConfigFactory.Instance();
        this.retinaBufferStorageFolder = normalizeRetinaBufferStorageFolder(
                configFactory.getProperty("retina.buffer.object.storage.folder"));
        this.retinaHost = retinaHost;
        this.retinaEnabled = Boolean.parseBoolean(configFactory.getProperty("retina.enable"));

        this.option = option;
        this.vectorLayout = (option.isReadIntColumnAsLongVector() ? TypeDescription.VectorLayout.INT_AS_LONG : 0) |
                (option.isReadShortColumnAsLongVector() ? TypeDescription.VectorLayout.SHORT_AS_LONG : 0) |
                (option.isReadTimeColumnAsLongTimeVector() ? TypeDescription.VectorLayout.TIME_AS_LONG_TIME : 0);
        this.activeMemtableData = activeMemtableData;
        this.fileIds = fileIds;
        this.storage = storage;
        this.tableId = tableId;
        this.typeDescription = typeDescription;
        this.colNum = typeDescription.getChildrenWithHiddenColumn().size();
        this.shouldReadHiddenColumn = option.hasValidTransTimestamp();
        this.visibilityBitmap = visibilityBitmap;
        this.vNodeId = vNodeId;
        initInternalExecutor();
        checkBeforeRead();
    }

    private static synchronized void initInternalExecutor()
    {
        if (prefetchExecutor == null)
        {
            ConfigFactory configFactory = ConfigFactory.Instance();
            String threadProp = configFactory.getProperty("retina.reader.prefetch.threads");
            maxPrefetchTasks = (threadProp != null) ? Integer.parseInt(threadProp) : DEFAULT_QUEUE_CAPACITY;
            prefetchExecutor = Executors.newFixedThreadPool(maxPrefetchTasks, r ->
            {
                Thread t = new Thread(r);
                t.setName("Pixels-Buffer-Reader-Prefetch-Shared");
                t.setDaemon(true);
                return t;
            });
            LOGGER.info("Initialized shared Pixels-Buffer-Reader-Prefetch pool with {} threads", maxPrefetchTasks);
        }
    }

    private static boolean checkBit(RetinaProto.VisibilityBitmap bitmap, int k)
    {
        long bitmap_ = bitmap.getBitmap(k / 64);
        return (bitmap_ & (1L << (k % 64))) != 0;
    }

    private void startPrefetching()
    {
        while (!closed && orderedPrefetch.size() < maxPrefetchTasks && nextSource <= fileIds.size())
        {
            final int source = nextSource++;
            if (source == 0 && (activeMemtableData == null || (activeMemtableData == null || activeMemtableData.length == 0))) { continue; }
            orderedPrefetch.add(prefetchExecutor.submit(() -> {
                ByteBuffer bytes;
                if (source == 0) { bytes = ByteBuffer.wrap(activeMemtableData); }
                else
                {
                    bytes = getMemtableDataFromStorage(getRetinaBufferStoragePathFromId(fileIds.get(source - 1), vNodeId));
                }
                VectorizedRowBatch batch = VectorizedRowBatch.deserialize(bytes, vectorLayout);
                if (!closed) { memoryUsage.addAndGet(batch.getMemoryUsage()); }
                return new LoadedBatch(batch, source);
            }));
        }
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
            for (int j = 0; j < typeDescription.getChildren().size(); j++)
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
        if (retinaEnabled && visibilityBitmap != null && visibilityBitmap.size() != fileIds.size() + 1 && !(visibilityBitmap.isEmpty() && fileIds.isEmpty() && (activeMemtableData == null || activeMemtableData.length == 0)))
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
        if (!checkValid || closed) { return false; }
        startPrefetching();
        if (orderedPrefetch.isEmpty() && nextSource > fileIds.size()) { endOfFile = true; return false; }
        return true;
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
        VectorizedRowBatch resultRowBatch = resultSchema.createRowBatch(0, vectorLayout);
        resultRowBatch.projectionSize = 0;
        resultRowBatch.endOfFile = this.endOfFile;
        resultRowBatch.size = size;
        return resultRowBatch;
    }

    @Override
    public VectorizedRowBatch readBatch() throws IOException
    {
        long start = System.nanoTime();
        if (!read()) { return createEmptyRowBatch(0); }
        LoadedBatch loaded;
        try { loaded = orderedPrefetch.removeFirst().get(); }
        catch (InterruptedException e)
        { Thread.currentThread().interrupt(); throw new IOException("Interrupted reading Retina buffer", e); }
        catch (java.util.concurrent.ExecutionException | java.util.concurrent.CancellationException e)
        { throw new IOException("Failed to read a selected Retina buffer segment", e); }
        VectorizedRowBatch batch = loaded.batch;
        memoryUsage.addAndGet(-batch.getMemoryUsage());
        startPrefetching();
        LongColumnVector timestamps = (LongColumnVector) batch.cols[colNum - 1];
        Bitmap selected = new Bitmap(batch.size, false);
        for (int i = 0; i < batch.size; i++)
        {
            boolean born = !shouldReadHiddenColumn || timestamps.vector[i] <= option.getTransTimestamp();
            boolean live = !retinaEnabled || visibilityBitmap == null || !checkBit(visibilityBitmap.get(loaded.bitmapIndex), i);
            if (born && live) { selected.set(i); }
        }
        batch.applyFilter(selected);
        // EOF is returned by the next empty read, never attached to a nonempty final batch.
        batch.endOfFile = false;
        dataReadRow += batch.size;
        readTimeNanos += System.nanoTime() - start;
        return batch;
    }

    @Override
    public TypeDescription getResultSchema()
    {
        // Return storage-order vectors; the PageSource applies its requested projection.
        return typeDescription;
    }

    @Override
    public boolean isValid()
    {
        return checkValid && !closed;
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
        return memoryUsage.get();
    }

    @Override
    public void close() throws IOException
    {
        closed = true;
        for (java.util.concurrent.Future<LoadedBatch> future : orderedPrefetch) { future.cancel(true); }
        orderedPrefetch.clear();
        memoryUsage.set(0);
    }

    static String normalizeRetinaBufferStorageFolder(String folder)
    {
        if (folder == null || folder.isEmpty())
        {
            throw new IllegalArgumentException("retina.buffer.object.storage.folder must not be empty");
        }
        return folder.endsWith("/") ? folder : folder + "/";
    }

    private String getRetinaBufferStoragePathFromId(long entryId, int virtualId)
    {
        return this.retinaBufferStorageFolder + String.format("%d/%d/%s_%d", tableId, virtualId, retinaHost, entryId);
    }

    private ByteBuffer getMemtableDataFromStorage(String path) throws IOException
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!closed && System.nanoTime() < deadline)
        {

            if (storage.exists(path))
            {
                try (PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(storage, path))
                {
                    long objectLength = reader.getFileLength();
                    if (objectLength < 0 || objectLength > Integer.MAX_VALUE)
                    { throw new IOException("Selected buffer object exceeds supported batch size: " + path); }
                    int length = (int) objectLength;
                    synchronized (this) { dataReadBytes += length; fileIdIndex++; }

                    // Local physical readers own their direct buffers and free them on close.
                    // Keep an independent payload alive until asynchronous deserialization ends.
                    ByteBuffer input = reader.readFully(length);
                    if (input.remaining() < length)
                    { throw new IOException("Truncated selected buffer object: " + path); }
                    byte[] owned = new byte[length];
                    input.get(owned);
                    return ByteBuffer.wrap(owned);
                }
            }

            try
            {
                Thread.sleep(POLL_INTERVAL_MILLS);
            } catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for file existence: " + path, e);
            }
        }
        throw new IOException("Selected buffer object unavailable within read deadline: " + path);
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
