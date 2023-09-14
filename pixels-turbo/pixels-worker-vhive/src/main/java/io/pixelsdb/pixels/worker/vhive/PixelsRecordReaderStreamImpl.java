package io.pixelsdb.pixels.worker.vhive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.ColumnReader;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.reader.PixelsRecordReaderImpl;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;

public class PixelsRecordReaderStreamImpl implements PixelsRecordReader {

    private static final Logger logger = LogManager.getLogger(PixelsRecordReaderImpl.class);
    PixelsProto.StreamHeader streamHeader;
    private final PixelsReaderOption option;
    private final long transId;
    private int RGLen;
    private final List<PixelsProto.Type> includedColumnTypes;

    BlockingQueue<ByteBuf> byteBufSharedQueue;
    private TypeDescription fileSchema = null;
    private TypeDescription resultSchema = null;
    private boolean everChecked = false;
    private boolean checkValid = false;
    private boolean everPrepared = false;
    private boolean everRead = false;
    private long rowIndex = 0L;
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

    private int targetRGNum = 0;         // number of target row groups
    private int curRGIdx = 0;            // index of current reading row group in targetRGs
    private int curRowInRG = 0;          // starting index of values to read by reader in current row group

    // buffers of each chunk in this file, arranged by chunk's row group id and column id
    private ColumnReader[] readers;      // column readers for each target columns
    private final boolean enableEncodedVector;

    private long diskReadBytes = 0L;
    private long readTimeNanos = 0L;
    private long memoryUsage = 0L;

    public PixelsRecordReaderStreamImpl(BlockingQueue<ByteBuf> byteBufSharedQueue,
                                        PixelsProto.StreamHeader streamHeader,
                                  PixelsReaderOption option) throws IOException
    {
        this.byteBufSharedQueue = byteBufSharedQueue;
        this.streamHeader = streamHeader;
        this.option = option;
        this.transId = option.getTransId();
        this.RGLen = option.getRGLen();  // In streaming mode, RGLen means max number of row groups to read
        this.enableEncodedVector = option.isEnableEncodedColumnVector();
        this.includedColumnTypes = new ArrayList<>();
        // Issue #175: this check is currently not necessary.
        // requireNonNull(TransContextCache.Instance().getQueryTransInfo(this.transId),
        //         "The transaction context does not contain query (trans) id '" + this.transId + "'");
        if (this.streamHeader != null) checkBeforeRead();
    }

    void checkBeforeRead() throws IOException
    {
        // get file schema
        List<PixelsProto.Type> fileColTypes = streamHeader.getTypesList();
        if (fileColTypes == null || fileColTypes.isEmpty())
        {
            checkValid = false;
            throw new IOException("type list is empty.");
        }
        fileSchema = TypeDescription.createSchema(fileColTypes);
        if (fileSchema.getChildren() == null || fileSchema.getChildren().isEmpty())
        {
            checkValid = false;
            throw new IOException("file schema is empty.");
        }

        // // check RGStart and RGLen are within the range of actual number of row groups (deprecated in streaming mode)
        // int rgNum = pipeliningFooter.getRowGroupInfosCount();

        // if (rgNum == 0)
        // {
        //     checkValid = true;
        //     endOfFile = true;
        //     return;
        // }

        // if (RGStart >= rgNum)
        // {
        //     checkValid = false;
        //     throw new IOException("row group start (" + RGStart + ") is out of bound (" + rgNum + ").");
        // }
        // if (RGLen == -1 || RGStart + RGLen > rgNum)
        // {
        //     RGLen = rgNum - RGStart;
        // }

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
     * @return
     * @throws IOException
     */
    private boolean prepareRead(ByteBuf byteBuf) throws IOException
    {
        // The byteBuf holds the first packet from the HTTP stream, i.e. the stream header + the first row group
//        if (!everChecked)
//        {
//            checkBeforeRead();
//            everChecked = true;
//        }

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
             * However, this is a normal case, hence we should return true, {@link #prepareBatch(int)} or {@link #read()}
             *
             */
            everPrepared = true;
            return true;
        }

        // read row group footer
        // In streaming mode, row group footers are interleaved with row groups, instead of
        //  residing in an array at the end of the file. So, we have to read them one by one,
        //  each followed by reading the corresponding row group.
        // If in the future we need it again, we can also scan the buffer twice, first time to
        //  read all row group footers.
        // PixelsProto.RowGroupFooter curRowGroupFooter;
        // /**
        //  * Issue #114:
        //  * Use request batch and read scheduler to execute the read requests.
        //  *
        //  * Here, we create an empty batch as footer cache is very likely to be hit in
        //  * the subsequent queries on the same table.
        //  */
        // try
        // {
        //     for (int i = 0; i < targetRGNum; i++)
        //     {
        //         int rgId = i;
        //         PixelsProto.RowGroupInformation rowGroupInformation =
        //                 pipeliningFooter.getRowGroupInfos(rgId);
        //         long footerOffset = rowGroupInformation.getFooterOffset();
        //         long footerLength = rowGroupInformation.getFooterLength();
        //         int fi = i;
        //         ByteBuf resp = Unpooled.buffer((int) footerLength);
        //         bufReader.readerIndex((int) footerOffset);
        //         bufReader.readBytes(resp, (int) footerLength); // getBytes()
        //             PixelsProto.RowGroupFooter parsed = PixelsProto.RowGroupFooter.parseFrom(resp.nioBuffer());
        //             rowGroupFooters[fi] = parsed;
        //     }
        // } catch (Exception e)
        // {
        //     throw new IOException("Failed to read row group footers, " +
        //             "only the last error is thrown, check the logs for more information.", e);
        // }

        int rowGroupsPosition = byteBuf.readerIndex();
        byteBuf.readerIndex(rowGroupsPosition + byteBuf.readInt() + Integer.BYTES);  // skip row group data and row group data length
        byte[] firstRgFooterBytes = new byte[byteBuf.readInt()];
        byteBuf.readBytes(firstRgFooterBytes);
        PixelsProto.StreamRowGroupFooter firstRgFooter = PixelsProto.StreamRowGroupFooter.parseFrom(firstRgFooterBytes);
        byteBuf.readerIndex(rowGroupsPosition);
        this.resultColumnsEncoded = new boolean[includedColumnNum];
        PixelsProto.RowGroupEncoding firstRgEncoding = firstRgFooter.getRowGroupEncoding();
        for (int i = 0; i < includedColumnNum; i++)
        {
            this.resultColumnsEncoded[i] = firstRgEncoding.getColumnChunkEncodings(targetColumns[i]).getKind() !=
                    PixelsProto.ColumnEncoding.Kind.NONE && enableEncodedVector;
        }

        everPrepared = true;
        return true;
    }

    /**
     * Comments added in Issue #67 (patch):
     * In this method, if the cache is enabled, we can support reading
     * the cache using Direct ByteBuffer, without memory copies and thus also
     * reduces the GC pressure.
     *
     * By optimizations in this method in Issue #67 (patch), end-to-end query
     * performance on full cache is improved by about 5% - 10%.
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

        // In streaming mode, as is explained before, we do not read all row group footers at once.
        // Therefore, callers are expected to call readBatch() directly, and now the read() does not do much.

        return true;
    }

    /**
     * Issue #105:
     * We use preRowInRG instead of curRowInRG to deal with queries like:
     * <b>select ... from t where f = null</b>.
     * Such query is invalid but Presto does not reject it. For such query,
     * Presto will call PageSource.getNextPage() but will not call load() on
     * the lazy blocks inside the returned page.
     *
     * Unfortunately, we have no way to distinguish such type from the normal
     * queries by the predicates and projection columns from Presto.
     *
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
     * Prepare for the next row batch. This method is independent from {@link #readBatch(int, boolean)}.
     *
     * @param batchSize the willing batch size.
     * @return the real batch size.
     */
    // This function is never used. Why is it here???
    @Override
    public int prepareBatch(int batchSize) throws IOException
    {
        if (endOfFile)
        {
            return 0;
        }

        if (!everPrepared)
        {
            if (!prepareRead(null))
            {
                throw new IOException("Failed to prepare for read.");
            }
        }

        if (includedColumnNum == 0)
        {
            /**
             * Issue #105:
             * project nothing, must be count(*).
             * qualifiedRowNum and endOfFile have been set in prepareRead();
             */
            if (!endOfFile)
            {
                throw new IOException("EOF should be set in case of none projection columns");
            }
            return qualifiedRowNum;
        }

        if (targetRGNum == 0)
        {
            /**
             * Issue #388:
             * No row groups to read, set EOF and return 0. Client should not continue reading row batches.
             * {@link #readBatch(int, boolean)} also checks endOfFile and targetRGNum.
             */
            qualifiedRowNum = 0;
            endOfFile = true;
            return 0;
        }

        // curBatchSize is the available size of the next batch.
        int curBatchSize = -preRowInRG;
        for (int rgIdx = preRGIdx; rgIdx < targetRGNum; ++rgIdx)
        {
            int rgRowCount = 0;  // (int) pipeliningFooter.getRowGroupInfos(targetRGs[rgIdx]).getNumberOfRows();
            curBatchSize += rgRowCount;
            if (curBatchSize <= 0)
            {
                // continue for the next row group if we reach the empty last row batch.
                curBatchSize = 0;
                preRGIdx++;
                preRowInRG = 0;
                continue;
            }
            if (curBatchSize >= batchSize)
            {
                curBatchSize = batchSize;
                preRowInRG += curBatchSize;
            } else
            {
                // Prepare for reading the next row group.
                preRGIdx++;
                preRowInRG = 0;
            }
            break;
        }

        // Check if this is the end of the file.
        if (curBatchSize <= 0)
        {
            endOfFile = true;
            return 0;
        }

        return curBatchSize;
    }

    /**
     * Create a row batch without any data, only sets the number of rows (size) and OEF.
     * Such a row batch is used for queries such as select count(*).
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

    // Now, the readBatch() method returns row batches from the data read online from the stream.
    // This corresponds to the Next() method in the pipelining model of databases.
    // Also, readBatch() has to block until any (new) data is available, and returns an empty rowBatch if the end of the stream is reached.
    @Override
    public VectorizedRowBatch readBatch(int batchSize, boolean reuse)
            throws IOException
    {
        ByteBuf byteBuf = null;
        try {
            byteBuf = byteBufSharedQueue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (!byteBuf.isReadable()) { this.endOfFile = true; }
        // Currently, the client sends a separate CLOSE package to close the stream, inducing an empty ByteBuf

//        if (!everChecked)
//        {
//            checkBeforeRead();
//            everChecked = true;
//        }

        if (!checkValid || endOfFile)
        {
            this.endOfFile = true;
            return createEmptyEOFRowBatch(0);
        }

        if (!everRead)
        {
            long start = System.nanoTime();
            if (!read(byteBuf))
            {
                throw new IOException("failed to read file.");
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
                return createEmptyEOFRowBatch(qualifiedRowNum);
            }
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

        int rgRowCount = 0;
        int curBatchSize = 0;
        ColumnVector[] columnVectors = resultRowBatch.cols;

        int rowGroupStartOffset = byteBuf.readerIndex();
        byteBuf.readerIndex(rowGroupStartOffset + byteBuf.readInt() + Integer.BYTES);  // skip row group data and row group data length
//        System.out.println("rowGroupFooter position: " + bufReader.readerIndex());
        byte[] rgFooterBytes = new byte[byteBuf.readInt()];
        byteBuf.readBytes(rgFooterBytes);
        PixelsProto.StreamRowGroupFooter rowGroupFooter = PixelsProto.StreamRowGroupFooter.parseFrom(rgFooterBytes);
//        System.out.println("parsed rowGroupFooter: ");
//        System.out.println(rowGroupFooter);
        rgRowCount = (int) rowGroupFooter.getNumberOfRows();

//        System.out.println(resultRowBatch.size + " " + batchSize);
//        System.out.println(curRowInRG + " " + rgRowCount);
        while (resultRowBatch.size < batchSize && curRowInRG < rgRowCount)
        {
            // update current batch size
            curBatchSize = rgRowCount - curRowInRG;
            if (curBatchSize + resultRowBatch.size >= batchSize)
            {
                curBatchSize = batchSize - resultRowBatch.size;
            }

            // read vectors
            for (int i = 0; i < resultColumns.length; i++)
            {
                if (!columnVectors[i].duplicated)
                {
                    PixelsProto.ColumnEncoding encoding = rowGroupFooter.getRowGroupEncoding()
                            .getColumnChunkEncodings(resultColumns[i]);
                    int index = curRGIdx * includedColumns.length + resultColumns[i];
                    PixelsProto.ColumnChunkIndex chunkIndex = rowGroupFooter.getRowGroupIndexEntry()
                            .getColumnChunkIndexEntries(resultColumns[i]);
                    ByteBuf chunkBuffer = Unpooled.buffer(chunkIndex.getChunkLength());
//                    System.out.println("chunkOffset: " + chunkIndex.getChunkOffset() + ", chunkLength: " + chunkIndex.getChunkLength());
                    byteBuf.getBytes((int) (chunkIndex.getChunkOffset()), chunkBuffer);
                    readers[i].read(chunkBuffer.nioBuffer(), encoding, curRowInRG, curBatchSize,
                            streamHeader.getPixelStride(), resultRowBatch.size, columnVectors[i], chunkIndex);
                    // don't update statistics in whenComplete as it may be executed in other threads.
                    diskReadBytes += chunkIndex.getChunkLength();
                    memoryUsage += chunkIndex.getChunkLength();
                }
            }

            // update current row index in the row group
            curRowInRG += curBatchSize;
            //preRowInRG = curRowInRG; // keep in sync with curRowInRG.
            rowIndex += curBatchSize;
            resultRowBatch.size += curBatchSize;
            // update row group index if current row index exceeds max row count in the row group
            if (curRowInRG >= rgRowCount)
            {
                curRGIdx++;
                if (byteBuf.isReadable()) byteBuf.readerIndex(byteBuf.readerIndex() + byteBuf.readInt() + Integer.BYTES);  // skip row group data and row group data length
                //preRGIdx = curRGIdx; // keep in sync with curRGIdx
                // if not end of file, update row count
                // if (curRGIdx < targetRGNum)
                if (byteBuf.isReadable())  // In streaming mode, we stop the loop when the buffer is fully read
                {
                    byte[] curRowGroupFooterBytes = new byte[byteBuf.readInt()];
                    byteBuf.readBytes(curRowGroupFooterBytes);
                    PixelsProto.StreamRowGroupFooter curRowGroupFooter = PixelsProto.StreamRowGroupFooter.parseFrom(curRowGroupFooterBytes);
                    // XXX: duplicate code fragment, can be refactored into a function.

                    rgRowCount = (int) curRowGroupFooter.getNumberOfRows();
                    PixelsProto.RowGroupEncoding rgEncoding = curRowGroupFooter.getRowGroupEncoding();
                    // refresh resultColumnsEncoded for reading the column vectors in the next row group.
                    for (int i = 0; i < includedColumnNum; i++)
                    {
                        this.resultColumnsEncoded[i] = rgEncoding.getColumnChunkEncodings(targetColumns[i]).getKind() !=
                                PixelsProto.ColumnEncoding.Kind.NONE && enableEncodedVector;
                    }
                }
                // if end of file, set result vectorized row batch endOfFile
                else
                {
                    // checkValid = false; // Issue #105: to reject continuous read.
                    // resultRowBatch.endOfFile = true;
                    // this.endOfFile = true;
                    // In streaming mode, there is never an EOF, instead only closing of the stream. So commented out the above lines.
                    curRowInRG = 0;
                    break;
                }
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

        byteBuf.clear();  // todo: only clear the buffer and send the HTTP response after this row group is fully read (because segmentation is possible)
        byteBuf.release();  // todo: use ResourceLeakDetector
        return resultRowBatch;
    }

    @Override
    public VectorizedRowBatch readBatch(int batchSize)
            throws IOException
    {
        return readBatch(batchSize, false);
    }

    @Override
    public VectorizedRowBatch readBatch(boolean reuse)
            throws IOException
    {
        return readBatch(VectorizedRowBatch.DEFAULT_SIZE, reuse);
    }

    @Override
    public VectorizedRowBatch readBatch()
            throws IOException
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
    public boolean isEndOfFile ()
    {
        return endOfFile;
    }

    /**
     * @return number of the row currently being read
     */
    @Override
    public long getCompletedRows()
    {
        return rowIndex;
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

    public class ChunkId
    {
        public final int rowGroupId;
        public final int columnId;
        public final long offset;
        public final int length;

        public ChunkId(int rowGroupId, int columnId, long offset, int length)
        {
            this.rowGroupId = rowGroupId;
            this.columnId = columnId;
            this.offset = offset;
            this.length = length;
        }
    }
}
