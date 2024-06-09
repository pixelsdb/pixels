/*
 * Copyright 2023 PixelsDB.
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.PixelsWriterStreamImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.utils.BlockingMap;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.turbo.StreamProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;

/**
 * PixelsRecordReaderStreamImpl is the variant of {@link PixelsRecordReaderImpl} for streaming mode.
 * <p>
 * TODO: Large row group support: Currently, we assume the row groups fit in the size of a single ByteBuf, i.e. 2GB
 *  because a ByteBuf's index is a 32-bit integer. Implement a mechanism to handle large row groups
 *  (e.g. using Netty's `ChunkedWriteHandler`).
 */
@NotThreadSafe
public class PixelsRecordReaderStreamImpl implements PixelsRecordReader
{
    private static final Logger logger = LogManager.getLogger(PixelsRecordReaderStreamImpl.class);
    StreamProto.StreamHeader streamHeader;
    private final PixelsReaderOption option;
    private final long transId;
    private int RGLen;  // In streaming mode, RGLen means max number of row groups to read. But currently unused.
    private final List<PixelsProto.Type> includedColumnTypes;

    private final boolean partitioned;

    /**
     * If the curRowGroupByteBuf is no longer readable, we take the next ByteBuf from the byteBufSharedQueue or
     *  byteBufBlockingMap;
     *  otherwise, we just read from curRowGroupByteBuf.
     * Note that this blocking queue or map is created in the stream reader
     * {@link io.pixelsdb.pixels.core.PixelsReaderStreamImpl#byteBufSharedQueue}, and only its reference is passed here.
     */
    private ByteBuf curRowGroupByteBuf;
    private final BlockingQueue<ByteBuf> byteBufSharedQueue;
    private final BlockingMap<Integer, ByteBuf> byteBufBlockingMap;  // hash value -> ByteBuf

    StreamProto.StreamRowGroupFooter curRowGroupStreamFooter;
    private TypeDescription fileSchema = null;
    private TypeDescription resultSchema = null;

    private boolean checkValid = false;
    private boolean everPrepared = false;
    private boolean everRead = false;
    private long completedRows = 0L;
    private VectorizedRowBatch resultRowBatch;
    /**
     * Columns included by reader option; if included, set true
     */
    private boolean[] includedColumns;
    /**
     * The target columns to read after matching reader option.
     * Each element represents a column id (column's index in the file schema).
     * Different from resultColumns, the ith column id in targetColumns
     * corresponds to the ith true value in this.includedColumns, i.e.,
     * The elements in targetColumns and resultColumns are in different order,
     * but they are all the index of the columns in the file schema.
     */
    private int[] targetColumns;
    /**
     * The ith element in resultColumns is the column id (column's index in the file schema)
     * of ith included column in the read option. The order of columns in the read option's
     * includedCols may be arbitrary, not related to the column order in schema.
     */
    private int[] resultColumns;
    /**
     * The ith element is true if the ith column in the resultSchema should use encoded column vectors.
     */
    private boolean[] resultColumnsEncoded;
    private int includedColumnNum = 0; // the number of columns to read.
    private int qualifiedRowNum = 0; // the number of qualified rows in this split.
    private boolean endOfFile = false;

    private int targetRGNum = 0;         // number of target row groups. Unused in streaming mode.
    private int curRGIdx = 0;            // index of current reading row group in targetRGs
    private int curRowInRG = 0;          // starting index of values to read by reader in current row group

    // buffers of each chunk in this file, arranged by chunk's row group id and column id
    private ColumnReader[] readers;      // column readers for each target columns
    private final boolean enableEncodedVector;

    private long diskReadBytes = 0L;
    private long readTimeNanos = 0L;
    private long memoryUsage = 0L;

    public PixelsRecordReaderStreamImpl(boolean partitioned,
                                        BlockingQueue<ByteBuf> byteBufSharedQueue,
                                        BlockingMap<Integer, ByteBuf> byteBufBlockingMap,
                                        StreamProto.StreamHeader streamHeader,
                                        PixelsReaderOption option) throws IOException
    {
        this.partitioned = partitioned;
        this.byteBufSharedQueue = byteBufSharedQueue;
        this.byteBufBlockingMap = byteBufBlockingMap;
        this.streamHeader = streamHeader;
        this.option = option;
        this.transId = option.getTransId();
        this.RGLen = option.getRGLen();
        this.enableEncodedVector = option.isEnableEncodedColumnVector();
        this.includedColumnTypes = new ArrayList<>();
        this.curRowGroupByteBuf = Unpooled.buffer();
        // Issue #175: this check is currently not necessary.
        // requireNonNull(TransContextCache.Instance().getQueryTransInfo(this.transId),
        //         "The transaction context does not contain query (trans) id '" + this.transId + "'");
        if (this.streamHeader != null) checkBeforeRead();
    }

    /**
     * We allow creating a RecordReader instance first anywhere and
     *  initialize it with `checkBeforeRead()` later in HTTP server's serve method in Reader,
     * because the first package of the stream (which contains the StreamHeader) might not have arrived
     *  by the time we create the RecordReader instance.
     * Also, because we put the byteBuf into the blocking queue only after initializing the `streamHeader`,
     *  it is safe to assume that the `streamHeader` has been initialized by the time
     *  we first call `readBatch()` in the recordReader.
     *
     * @param streamHeader
     * @throws IOException
     */
    public void lateInitialization(StreamProto.StreamHeader streamHeader) throws IOException
    {
        this.streamHeader = streamHeader;
        checkBeforeRead();
    }

    /**
     * In streaming mode, we do the following pre-read checks on reception of the stream header, or if the stream header
     *  has already been received, on construction of the PixelsRecordReaderStreamImpl object.
     *
     * @throws IOException
     */
    void checkBeforeRead() throws IOException
    {
        // get file schema
        List<PixelsProto.Type> fileColTypes = streamHeader.getTypesList();
        if (fileColTypes == null || fileColTypes.isEmpty())
        {
            checkValid = false;
            throw new IOException("streamHeader type list is empty.");
        }
        fileSchema = TypeDescription.createSchema(fileColTypes);
        if (fileSchema.getChildren() == null || fileSchema.getChildren().isEmpty())
        {
            checkValid = false;
            throw new IOException("file schema derived from streamHeader is empty.");
        }

        // filter included columns
        includedColumnNum = 0;
        String[] optionIncludedCols = option.getIncludedCols();
        // if size of cols is 0, create an empty row batch
        if (optionIncludedCols.length == 0)
        {
            checkValid = true;
            // Issue #103: init the following members as null.
            this.includedColumns = null;
            this.resultColumns = null;
            this.targetColumns = null;
            this.readers = null;
            //throw new IOException("ISSUE-103: included columns is empty.");
            return;
        }
        List<Integer> optionColsIndices = new ArrayList<>();
        this.includedColumns = new boolean[fileColTypes.size()];
        for (String col : optionIncludedCols)
        {
            for (int j = 0; j < fileColTypes.size(); j++)
            {
                if (col.equalsIgnoreCase(fileColTypes.get(j).getName()))
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

        // create result columns storing result column ids in user specified order
        this.resultColumns = new int[includedColumnNum];
        for (int i = 0; i < includedColumnNum; i++)
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

        // create column readers
        List<TypeDescription> columnSchemas = fileSchema.getChildren();
        readers = new ColumnReader[resultColumns.length];
        for (int i = 0; i < resultColumns.length; i++)
        {
            int index = resultColumns[i];
            readers[i] = ColumnReader.newColumnReader(columnSchemas.get(index));
        }

        // create result vectorized row batch
        for (int resultColumn : resultColumns)
        {
            includedColumnTypes.add(fileColTypes.get(resultColumn));
        }

        resultSchema = TypeDescription.createSchema(includedColumnTypes);
        checkValid = true;
    }

    /**
     * This method is to prepare the internal status for read operations.
     * It should only return false when there is an error. Special cases
     * must be processed correctly and return true.
     *
     * @return
     * @throws IOException
     */
    private boolean prepareRead(ByteBuf byteBuf) throws IOException
    {
        // The byteBuf holds the first packet from the HTTP stream, i.e. the stream header + the first row group
        if (!checkValid)
        {
            everPrepared = false;
            return false;
        }

        if (RGLen == 0)
        {
            everPrepared = false;
            return false;
        }

        if (includedColumnNum == 0)
        {
            /**
             * Issue #105:
             * project nothing, must be count(*).
             * includedColumnNum should only be set in checkBeforeRead().
             */
            qualifiedRowNum = 0;  // includedRowNum;
            endOfFile = true;
            // init the following members as null or 0.
            targetRGNum = 0;

            everPrepared = true;
            return true;
        }

        targetRGNum = RGLen;

        if (targetRGNum == 0)
        {
            /**
             * Issue #388:
             * No need to continue preparing row group footers and encoded flags for the column vectors of each column.
             * However, this is a normal case, hence we should return true for {@link #read()}
             *
             */
            everPrepared = true;
            return true;
        }

        /**
         * Different from {@link PixelsRecordReaderImpl#prepareRead()}, we do not read all row group footers at once
         *  here.
         * In streaming mode, row group footers are interleaved with row groups, instead of
         *  residing in an array at the end of the file. So, we will read them one by one in {@link #readBatch()},
         *  each preceding reading the corresponding row group.
         * If in the future we need this collection of row group footers, we can also scan the buffer twice,
         *  first time to read all row group footers.
         */
        int rowGroupsPosition = byteBuf.readerIndex();
        long rowGroupDataLength = byteBuf.readLong();
        logger.debug("In prepareRead(), rowGroupsPosition = " + rowGroupsPosition +
                ", rowGroupDataLength = " + rowGroupDataLength +
                ", firstRgFooterPosition = " + (rowGroupsPosition + rowGroupDataLength));
        // We do not need the row group footer here for now
        // byteBuf.readerIndex(rowGroupsPosition + rowGroupDataLength);  // skip row group data and row group data
        // length, and get to row group footer
        // byte[] firstRgFooterBytes = new byte[byteBuf.readInt()];
        // byteBuf.readBytes(firstRgFooterBytes);
        // StreamProto.StreamRowGroupFooter firstRgFooter = StreamProto.StreamRowGroupFooter.parseFrom(firstRgFooterBytes);

        byteBuf.readerIndex(rowGroupsPosition);
        this.resultColumnsEncoded = new boolean[includedColumnNum];

        everPrepared = true;
        return true;
    }

    /**
     * In streaming mode, we do not read all row group footers at once in prepareRead().
     * Workers should call readBatch() directly rather than read(), as the read() does not function like in
     * {@link PixelsRecordReaderImpl#read()}.
     * But keep the read() method for compatibility.
     *
     * @return true if there is row group to read and the row groups are read
     * successfully.
     */
    private boolean read(ByteBuf byteBuf) throws IOException
    {
        if (!checkValid)
        {
            return false;
        }

        if (!everPrepared)
        {
            if (!prepareRead(byteBuf))
            {
                throw new IOException("failed to prepare for read.");
            }
        }

        everRead = true;

        /**
         * Issue #105:
         * project nothing, must be count(*).
         * qualifiedRowNum and endOfFile have been set in prepareRead();
         */
        if (includedColumnNum == 0)
        {
            if (!endOfFile)
            {
                throw new IOException("EOF should be set in case of none projection columns");
            }
            return true;
        }

        if (targetRGNum == 0)
        {
            /**
             * Issue #105:
             * No row groups to read, set EOF and return. EOF will be checked in readBatch().
             */
            qualifiedRowNum = 0;
            endOfFile = true;
            return true;
        }

        return true;
    }

    /**
     * Issue #105:
     * We use preRowInRG instead of curRowInRG to deal with queries like:
     * <b>select ... from t where f = null</b>.
     * Such query is invalid but Presto does not reject it. For such query,
     * Presto will call PageSource.getNextPage() but will not call load() on
     * the lazy blocks inside the returned page.
     * <p>
     * Unfortunately, we have no way to distinguish such type from the normal
     * queries by the predicates and projection columns from Presto.
     * <p>
     * preRowInRG will keep in sync with curRowInRG if readBatch() is actually
     * called from LazyBlock.load().
     */
    private int preRowInRG = 0;
    /**
     * Issue #105:
     * Similar to preRowInRG.
     */
    private int preRGIdx = 0;

    /**
     * Prepare for the next row batch. Not needed in streaming mode.
     * But keep the code for compatibility with the interface.
     */
    @Override
    public int prepareBatch(int batchSize)
    {
        return batchSize;
    }

    /**
     * Create a row batch without any data, only sets the number of rows (size) and EOF.
     * Such a row batch is used for queries such as select count(*).
     *
     * @param size the number of rows in the row batch.
     * @return the empty row batch.
     */
    private VectorizedRowBatch createEmptyEOFRowBatch(int size)
    {
        TypeDescription resultSchema = TypeDescription.createSchema(new ArrayList<>());
        VectorizedRowBatch resultRowBatch = resultSchema.createRowBatch(0);
        resultRowBatch.projectionSize = 0;
        resultRowBatch.endOfFile = true;
        resultRowBatch.size = size;
        return resultRowBatch;
    }

    /**
     * In the streaming version, the readBatch() method returns row batches read online from the stream.
     * This corresponds to the Next() method in the pipelining model of databases.
     * ReadBatch() would block until any (new) batch is available or until timeout (60s - as defined in
     * {@link BlockingMap}),
     *  and returns an empty row batch if the end of the stream is reached.
     */
    @Override
    public VectorizedRowBatch readBatch(int batchSize, boolean reuse)
            throws IOException
    {
        if (!curRowGroupByteBuf.isReadable() && !endOfFile)
        {
            acquireNewRowGroup(reuse);
            if (endOfFile) return createEmptyEOFRowBatch(0);
            logger.debug("In readBatch(), new row group " + curRGIdx);
        }

        if (!checkValid || endOfFile)
        {
            this.endOfFile = true;
            return createEmptyEOFRowBatch(0);
        }

        // project nothing, must be count(*)
        if (includedColumnNum == 0)
        {
            /**
             * Issue #105:
             * It should be EOF. And the batch size should have been set in prepareRead() and
             * checked in read().
             */
            if (!endOfFile)
            {
                throw new IOException("EOF should be set in case of none projection columns");
            }
            checkValid = false; // Issue #105: to reject continuous read.
            // endOfFile is already true.
            return createEmptyEOFRowBatch(qualifiedRowNum);
        }

        VectorizedRowBatch resultRowBatch;
        if (reuse)
        {
            if (this.resultRowBatch == null || this.resultRowBatch.projectionSize != includedColumnNum)
            {
                this.resultRowBatch = resultSchema.createRowBatch(batchSize, resultColumnsEncoded);
                this.resultRowBatch.projectionSize = includedColumnNum;
            }
            this.resultRowBatch.reset();
            this.resultRowBatch.ensureSize(batchSize, false);
            resultRowBatch = this.resultRowBatch;
        } else
        {
            resultRowBatch = resultSchema.createRowBatch(batchSize, resultColumnsEncoded);
            resultRowBatch.projectionSize = includedColumnNum;
        }

        int rgRowCount = (int) curRowGroupStreamFooter.getNumberOfRows();
        int curBatchSize;
        ColumnVector[] columnVectors = resultRowBatch.cols;

        /**
         * row group size ~200MB; packet size ~2MB; row batch size ~1MB (?)
         * One packet == one curByteBuf <= one row group.
         * <p>
         * Note that this implementation could return a row batch spanning multiple row groups. But for now,
         *  sender ({@link PixelsWriterStreamImpl#writeRowGroup()}) guarantees that readBatch() will never encounter
         *  a row group boundary.
         * </p>
         */
        while (resultRowBatch.size < batchSize && curRowInRG < rgRowCount)
        {
            // update current batch size
            curBatchSize = rgRowCount - curRowInRG;
            if (curBatchSize + resultRowBatch.size >= batchSize)
            {
                curBatchSize = batchSize - resultRowBatch.size;
            }

            // read vectors
            // Can parallelize the following loop, but it is not much beneficial as the bottleneck lies with HTTP I/O.
            // Arrays.stream(resultColumns).parallel().forEach(i ->
            for (int i = 0; i < resultColumns.length; i++)
            {
                if (!columnVectors[i].duplicated)
                {
                    PixelsProto.ColumnEncoding encoding = curRowGroupStreamFooter.getRowGroupEncoding()
                            .getColumnChunkEncodings(resultColumns[i]);
                    // int index = curRGIdx * includedColumns.length + resultColumns[i];
                    PixelsProto.ColumnChunkIndex chunkIndex = curRowGroupStreamFooter.getRowGroupIndexEntry()
                            .getColumnChunkIndexEntries(resultColumns[i]);
                    ByteBuf chunkBuffer = curRowGroupByteBuf.slice((int) chunkIndex.getChunkOffset(),
                            chunkIndex.getChunkLength());
                    readers[i].read(chunkBuffer.nioBuffer(), encoding, curRowInRG, curBatchSize,
                            streamHeader.getPixelStride(), resultRowBatch.size, columnVectors[i], chunkIndex);
                    // don't update statistics in whenComplete as it may be executed in other threads.
                    diskReadBytes += chunkIndex.getChunkLength();
                    memoryUsage += chunkIndex.getChunkLength();
                }
            }
            // );

            // update current row index in the row group
            curRowInRG += curBatchSize;
            //preRowInRG = curRowInRG; // keep in sync with curRowInRG.
            completedRows += curBatchSize;
            resultRowBatch.size += curBatchSize;

            // If current row index exceeds max row count in the row group, prepare for the next row group
            if (curRowInRG >= rgRowCount)
            {
                curRGIdx++;
                acquireNewRowGroup(reuse);
                if (endOfFile)
                {
                    checkValid = false;
                    resultRowBatch.endOfFile = true;
                    break;
                }

                rgRowCount = (int) curRowGroupStreamFooter.getNumberOfRows();
                //preRowInRG = curRowInRG = 0; // keep in sync with curRowInRG.
                curRowInRG = 0;
            }
            if (this.enableEncodedVector)
            {
                /**
                 * Issue #374:
                 * Dictionary column vector can not contain data from multiple column chunks,
                 * hence we do not pad the row batch with rows from the next row group.
                 */
                break;
            }
        }

        for (ColumnVector cv : columnVectors)
        {
            if (cv.duplicated)
            {
                // copyFrom() is actually a shallow copy
                // rename copyFrom() to duplicate(), so it is more readable
                cv.duplicate(columnVectors[cv.originVecId]);
            }
        }

        // Must have already been released in acquireNewRowGroup(); not necessary to release again.
        // if (curRowGroupByteBuf.refCnt() > 0) {
        //     curRowGroupByteBuf.release();
        // }
        return resultRowBatch;
    }

    private void acquireNewRowGroup(boolean reuse) throws IOException
    {
        logger.debug("In acquireNewRowGroup(), curRGIdx = " + curRGIdx);
        if (!endOfFile)
        {
            try
            {
                curRowGroupByteBuf.release();
                curRowGroupByteBuf = partitioned ? byteBufBlockingMap.get(curRGIdx) : byteBufSharedQueue.take();
            } catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }

        //preRGIdx = curRGIdx;
        // if not end of file, update row count
        if (curRowGroupByteBuf.isReadable())
        {
            if (!everRead)
            {
                long start = System.nanoTime();
                if (!read(curRowGroupByteBuf))
                {
                    throw new IOException("failed to read stream.");
                }
                readTimeNanos += System.nanoTime() - start;
                if (endOfFile)
                {
                    /**
                     * Issue #388:
                     * Check EOF again after reading.
                     * As client may directly call this method without firstly calling {@link #prepareBatch(int)},
                     * EOF may have not been set at the beginning of this method.
                     *
                     * Note that endOfFile == true means no row batches can be read from the chunk buffers. It does
                     * not mean whether there is remaining data to be read from the file. We always read all the
                     * column chunks into chunk buffers at once (in  {@link #read()}).
                     */
                    return;
                }
            }

            curRowGroupByteBuf.markReaderIndex();

            // skip row group data and process row group footer first
            curRowGroupByteBuf.readerIndex((int) (curRowGroupByteBuf.readerIndex() + curRowGroupByteBuf.readLong()));
            byte[] curRowGroupFooterBytes = new byte[curRowGroupByteBuf.readInt()];
            curRowGroupByteBuf.readBytes(curRowGroupFooterBytes);
            curRowGroupByteBuf.resetReaderIndex();
            curRowGroupStreamFooter = StreamProto.StreamRowGroupFooter.parseFrom(curRowGroupFooterBytes);

            PixelsProto.RowGroupEncoding rgEncoding = curRowGroupStreamFooter.getRowGroupEncoding();
            // refresh resultColumnsEncoded for reading the column vectors in the next row group.
            for (int i = 0; i < includedColumnNum; i++)
            {
                this.resultColumnsEncoded[i] = rgEncoding.getColumnChunkEncodings(targetColumns[i]).getKind() !=
                        PixelsProto.ColumnEncoding.Kind.NONE && enableEncodedVector;
            }
        } else
        // incoming byteBuf unreadable, must be end of stream
        {
            // checkValid = false; // Issue #105: to reject continuous read.
            if (reuse && resultRowBatch != null)
                // The close() below might be called before our readBatch() is complete,
                //  i.e. the resultRowBatch might be null.
                resultRowBatch.endOfFile = true;
            this.endOfFile = true;  // In streaming mode, EOF indicates the end of stream
            curRowGroupByteBuf.release();
        }
    }

    @Override
    public VectorizedRowBatch readBatch(int batchSize) throws IOException
    {
        return readBatch(batchSize, false);
    }

    @Override
    public VectorizedRowBatch readBatch(boolean reuse) throws IOException
    {
        return readBatch(VectorizedRowBatch.DEFAULT_SIZE, reuse);
    }

    @Override
    public VectorizedRowBatch readBatch() throws IOException
    {
        return readBatch(VectorizedRowBatch.DEFAULT_SIZE, false);
    }

    @Override
    public TypeDescription getResultSchema()
    {
        return this.resultSchema;
    }

    @Override
    public boolean isValid()
    {
        return this.checkValid;
    }

    @Override
    public boolean isEndOfFile()
    {
        return endOfFile;
    }

    /**
     * @return number of the row currently being read
     */
    @Override
    public long getCompletedRows()
    {
        return completedRows;
    }

    /**
     * Seek to specified row
     * Currently not supported
     *
     * @param rowIndex row number
     * @return seek success
     */
    @Override
    public boolean seekToRow(long rowIndex)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean skip(long rowNum)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getCompletedBytes()
    {
        return diskReadBytes;
    }

    @Override
    public int getNumReadRequests()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public long getMemoryUsage()
    {
        // Memory usage in the row batch returned by readBatch() is not counted.
        return memoryUsage;
    }

    /**
     * Cleanup and release resources
     */
    @Override
    public void close() throws IOException
    {
        diskReadBytes = 0L;
        if (readers != null)
        {
            for (int i = 0; i < readers.length; ++i)
            {
                try
                {
                    if (readers[i] != null)
                    {
                        readers[i].close();
                    }
                } catch (IOException e)
                {
                    logger.error("Failed to close column reader.", e);
                    throw new IOException("Failed to close column reader.", e);
                } finally
                {
                    readers[i] = null;
                }
            }
        }

        includedColumnTypes.clear();
        // no need to close resultRowBatch
        resultRowBatch = null;
        endOfFile = true;
        // write out read performance metrics
//        if (enableMetrics)
//        {
//            String metrics = JSON.toJSONString(readPerfMetrics);
//            Path metricsFilePath = Paths.get(metricsDir,
//                    String.valueOf(System.nanoTime()) +
//                    physicalFSReader.getPath().getName() +
//                    ".json");
//            try {
//                RandomAccessFile raf = new RandomAccessFile(metricsFilePath.toFile(), "rw");
//                raf.seek(0L);
//                raf.writeChars(metrics);
//                raf.close();
//            }
//            catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
        // reset read performance metrics
//        readPerfMetrics.clear();
    }
}
