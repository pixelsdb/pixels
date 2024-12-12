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

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.core.PixelsStreamProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * PixelsRecordReaderStreamImpl is the variant of {@link PixelsRecordReaderImpl} for streaming mode.
 * <p>
 * DESIGN: Read a row group and a row group footer from physical reader at a time.
 * </p>
 */
@NotThreadSafe
public class PixelsRecordReaderStreamImpl implements PixelsRecordReader
{
    private static final Logger logger = LogManager.getLogger(PixelsRecordReaderStreamImpl.class);
    private static final ByteOrder READER_ENDIAN;

    PixelsStreamProto.StreamHeader streamHeader;
    PixelsStreamProto.StreamRowGroupFooter curRowGroupStreamFooter;
    private final PixelsReaderOption option;
    private final List<PixelsProto.Type> includedColumnTypes;
    private TypeDescription resultSchema = null;
    private boolean checkValid = false;
    private boolean endOfFile = false;
    private int includedColumnNum = 0; // the number of columns to read.
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
    private int curRowInRG = -1;            // starting index of values to read by reader in current row group
    private int curRGCount = -1;            // number of rows in current row group
    private ByteBuffer curRGBuffer;        // current read row group
    private ByteBuffer curRGFooterBuffer;  // current read row group footer
    private ByteBuffer[] chunkBuffers;     // buffers of each chunk in current row group, arranged by chunk's column id
    private ColumnReader[] readers;        // column readers for each target columns
    private final boolean enableEncodedVector = false;
    private long diskReadBytes = 0L;
    private long readTimeNanos = 0L;
    private long memoryUsage = 0L;
    private long completedRows = 0L;

    private final PhysicalReader physicalReader;

    static
    {
        boolean littleEndian = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("column.chunk.little.endian"));
        if (littleEndian)
        {
            READER_ENDIAN = ByteOrder.LITTLE_ENDIAN;
        }
        else
        {
            READER_ENDIAN = ByteOrder.BIG_ENDIAN;
        }
    }

    public PixelsRecordReaderStreamImpl(PhysicalReader physicalReader,
                                        PixelsStreamProto.StreamHeader streamHeader,
                                        PixelsReaderOption option) throws IOException
    {
        requireNonNull(physicalReader, "physicalReader must not be null");
        requireNonNull(streamHeader, "streamHeader must not be null");
        requireNonNull(option, "option must not be null");
        this.physicalReader = physicalReader;
        this.curRGBuffer = ByteBuffer.allocate(Constants.STREAM_READER_RG_BUFFER_SIZE);
        this.curRGFooterBuffer = ByteBuffer.allocate(Constants.STREAM_READER_RG_FOOTER_BUFFER_SIZE);
        this.streamHeader = streamHeader;
        this.option = option;
        this.includedColumnTypes = new ArrayList<>();
        checkBeforeRead();
    }

    /**
     * Check if the file schema  and option included schema is valid.
     * @throws IOException
     */
    void checkBeforeRead() throws IOException
    {
        // get file schema
        List<PixelsProto.Type> fileColTypes = streamHeader.getTypesList();
        if (fileColTypes.isEmpty())
        {
            checkValid = false;
            throw new IOException("streamHeader type list is empty.");
        }
        TypeDescription fileSchema = TypeDescription.createSchema(fileColTypes);
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
            readers[i] = ColumnReader.newColumnReader(columnSchemas.get(index), option);
        }

        // create result vectorized row batch
        for (int resultColumn : resultColumns)
        {
            includedColumnTypes.add(fileColTypes.get(resultColumn));
        }

        resultSchema = TypeDescription.createSchema(includedColumnTypes);
        resultColumnsEncoded = new boolean[includedColumns.length];
        checkValid = true;
    }

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
        VectorizedRowBatch resultRowBatch = resultSchema.createRowBatch(0, TypeDescription.Mode.NONE);
        resultRowBatch.projectionSize = 0;
        resultRowBatch.endOfFile = true;
        resultRowBatch.size = size;
        return resultRowBatch;
    }

    public VectorizedRowBatch readBatch(int batchSize, boolean reuse) throws IOException
    {
        long start = System.nanoTime();
        readRowGroup();
        if (curRGCount == 0)
        {
            return createEmptyEOFRowBatch(0);
        }

        readTimeNanos = System.nanoTime() - start;
        VectorizedRowBatch resultRowBatch;
        if (reuse)
        {
            if (this.resultRowBatch == null || this.resultRowBatch.projectionSize != includedColumnNum)
            {
                this.resultRowBatch = resultSchema.createRowBatch(batchSize,
                        TypeDescription.Mode.NONE, resultColumnsEncoded);
                this.resultRowBatch.projectionSize = includedColumnNum;
            }
            this.resultRowBatch.reset();
            this.resultRowBatch.ensureSize(batchSize, false);
            resultRowBatch = this.resultRowBatch;
        }
        else
        {
            resultRowBatch = resultSchema.createRowBatch(batchSize, TypeDescription.Mode.NONE, resultColumnsEncoded);
            resultRowBatch.projectionSize = includedColumnNum;
        }

        ColumnVector[] columnVectors = resultRowBatch.cols;
        int curBatchSize = 0;
        while (resultRowBatch.size < batchSize)
        {
            // the number of rows read from this row group
            curBatchSize = Math.min(curRGCount - curRowInRG, batchSize - resultRowBatch.size);

            // read from row group to result row batch
            for (int i = 0; i < resultColumns.length; i++)
            {
                if (!columnVectors[i].duplicated)
                {
                    PixelsProto.ColumnEncoding encoding = curRowGroupStreamFooter.getRowGroupEncoding()
                            .getColumnChunkEncodings(resultColumns[i]);
                    PixelsProto.ColumnChunkIndex chunkIndex = curRowGroupStreamFooter.getRowGroupIndexEntry()
                            .getColumnChunkIndexEntries(resultColumns[i]);
                    readers[i].read(chunkBuffers[resultColumns[i]], encoding, curRowInRG, curBatchSize,
                            streamHeader.getPixelStride(), resultRowBatch.size, columnVectors[i], chunkIndex);
                    // don't update statistics in whenComplete as it may be executed in other threads.
                    diskReadBytes += chunkIndex.getChunkLength();
                    memoryUsage += chunkIndex.getChunkLength();
                }
            }

            // update current row index in the row group
            curRowInRG += curBatchSize;
            //preRowInRG = curRowInRG; // keep in sync with curRowInRG.
            completedRows += curBatchSize;
            resultRowBatch.size += curBatchSize;

            if (curRowInRG >= curRGCount)
            {
                readRowGroup();
                if (curRGCount == 0)
                {
                    resultRowBatch.endOfFile = true;
                    break;
                }
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

        return resultRowBatch;
    }

    private void readRowGroup() throws IOException
    {
        if (curRowInRG < curRGCount)
        {
            return;
        }

        int rowGroupDataLen = 0;
        try
        {
            rowGroupDataLen = physicalReader.readInt(READER_ENDIAN);
        } catch (IOException e)
        {
            if (e instanceof EOFException)
            {
                curRGCount = 0;
                curRowInRG = 0;
                return;
            } else
            {
                throw e;
            }
        }

        if (rowGroupDataLen > Constants.STREAM_READER_RG_BUFFER_SIZE)
        {
            curRGBuffer = ByteBuffer.allocate(rowGroupDataLen);
        }
        physicalReader.readFully(curRGBuffer.array(), 0, rowGroupDataLen);
        curRGBuffer.position(0);
        curRGBuffer.limit(rowGroupDataLen);

        int rowGroupFooterLen = physicalReader.readInt(READER_ENDIAN);
        if (rowGroupFooterLen > Constants.STREAM_READER_RG_FOOTER_BUFFER_SIZE)
        {
            curRGFooterBuffer = ByteBuffer.allocate(rowGroupFooterLen);
        }
        physicalReader.readFully(curRGFooterBuffer.array(), 0, rowGroupFooterLen);
        curRGFooterBuffer.position(0);
        curRGFooterBuffer.limit(rowGroupFooterLen);

        curRowGroupStreamFooter = PixelsStreamProto.StreamRowGroupFooter.parseFrom(curRGFooterBuffer);
        PixelsProto.RowGroupEncoding rgEncoding = curRowGroupStreamFooter.getRowGroupEncoding();

        // update curRowInRG and curRGCount
        curRowInRG = 0;
        curRGCount = (int) curRowGroupStreamFooter.getNumberOfRows();

        // refresh resultColumnsEncoded for reading the column vectors in the next row group.
        for (int i = 0; i < includedColumnNum; i++)
        {
            this.resultColumnsEncoded[i] = rgEncoding.getColumnChunkEncodings(targetColumns[i]).getKind() !=
                    PixelsProto.ColumnEncoding.Kind.NONE && enableEncodedVector;
        }

        // divide the row group data into chunks
        if (this.chunkBuffers != null)
        {
            Arrays.fill(this.chunkBuffers, null);
        }
        this.chunkBuffers = new ByteBuffer[includedColumns.length];
        List<ChunkId> diskChunks = new ArrayList<>(targetColumns.length);
        PixelsProto.RowGroupIndex rowGroupIndex =
                curRowGroupStreamFooter.getRowGroupIndexEntry();
        for (int colId : targetColumns)
        {
            PixelsProto.ColumnChunkIndex chunkIndex =
                    rowGroupIndex.getColumnChunkIndexEntries(colId);
            ChunkId chunk = new ChunkId(0, colId,
                    chunkIndex.getChunkOffset(),
                    chunkIndex.getChunkLength());
            diskChunks.add(chunk);
        }

        if (!diskChunks.isEmpty())
        {
            for (ChunkId chunk : diskChunks)
            {
                /**
                 * Comments added in Issue #103:
                 * chunk.rowGroupId does not mean the row group id in the Pixels file,
                 * it is the index of group that is to be read (some row groups in the file
                 * may be filtered out by the predicate and will not be read) and it is
                 * used to calculate the index of chunkBuffers.
                 */
                int rgIdx = chunk.rowGroupId;
                int numCols = includedColumns.length;
                int colId = chunk.columnId;
                /**
                 * Issue #114:
                 * The old code segment of chunk reading is remove in this issue.
                 * Now, if enableMetrics == true, we can add the read performance metrics here.
                 *
                 * Examples of how to add performance metrics:
                 *
                 * BytesMsCost seekCost = new BytesMsCost();
                 * seekCost.setBytes(seekDistanceInBytes);
                 * seekCost.setMs(seekTimeMs);
                 * readPerfMetrics.addSeek(seekCost);
                 *
                 * BytesMsCost readCost = new BytesMsCost();
                 * readCost.setBytes(bytesRead);
                 * readCost.setMs(readTimeMs);
                 * readPerfMetrics.addSeqRead(readCost);
                 */
                ByteBuffer temp = ByteBuffer.allocate(chunk.length);
                curRGBuffer.position((int) chunk.offset);
                curRGBuffer.get(temp.array(), 0, chunk.length);
                chunkBuffers[rgIdx * numCols + colId] = temp;
                // don't update statistics in whenComplete as it may be executed in other threads.
                diskReadBytes += chunk.length;
                memoryUsage += chunk.length;
            }
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
        // release chunk buffer
        if (chunkBuffers != null)
        {
            Arrays.fill(chunkBuffers, null);
        }
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
                }
                catch (IOException e)
                {
                    logger.error("Failed to close column reader.", e);
                    throw new IOException("Failed to close column reader.", e);
                }
                finally
                {
                    readers[i] = null;
                }
            }
        }

        includedColumnTypes.clear();
        // no need to close resultRowBatch
        resultRowBatch = null;
        endOfFile = true;
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
