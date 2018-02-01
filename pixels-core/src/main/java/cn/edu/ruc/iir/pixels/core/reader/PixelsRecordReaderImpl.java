package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.ChunkSeq;
import cn.edu.ruc.iir.pixels.core.ChunkId;
import cn.edu.ruc.iir.pixels.core.PhysicalFSReader;
import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsRecordReaderImpl
        implements PixelsRecordReader
{
    private final PhysicalFSReader physicalFSReader;
    private final PixelsProto.Footer footer;
    private final PixelsReaderOption option;

    private TypeDescription readerSchema;
    private boolean checkValid = false;
    private boolean everRead = false;
    private long rowIndex = 0L;
    private boolean[] includedColumns;   // columns included by reader option; if included, set true
    private int[] targetRGs;             // target row groups to read after matching reader option, each element represents row group id
    private int[] targetColumns;         // target columns to read after matching reader option, each element represents column id

    private int readerCurRGIdx = 0;      // index of current reading row group in targetRGs
    private int readerCurRGOffset = 0;   // starting index of values to read by reader in current row group

    private PixelsProto.RowGroupFooter[] rowGroupFooters;
    private ByteBuf[][] chunkBuffers;    // buffers of each chunk in this file, arranged by chunk's row group id and column id
    private ColumnReader[] readers;      // column readers for each target columns

    public PixelsRecordReaderImpl(PhysicalFSReader physicalFSReader,
                                  PixelsProto.Footer footer,
                                  PixelsReaderOption option)
    {
        this.physicalFSReader = physicalFSReader;
        this.footer = footer;
        this.option = option;
        checkBeforeRead();
    }

    private void checkBeforeRead()
    {
        // get file schema
        List<PixelsProto.Type> colTypes = footer.getTypesList();
        if (colTypes == null || colTypes.isEmpty()) {
            checkValid = false;
            return;
        }
        TypeDescription fileSchema = TypeDescription.createSchema(colTypes);
        if (fileSchema.getChildren() == null || fileSchema.getChildren().isEmpty()) {
            checkValid = false;
            return;
        }

        // check if predicate matches file schema

        // get included columns
        int includedColumnsNum = 0;
        String[] cols = option.getIncludedCols();
        List<String> colsList = ImmutableList.copyOf(cols);
        if (cols.length == 0) {
            checkValid = false;
            return;
        }
        this.includedColumns = new boolean[colTypes.size()];
        for (int i = 0; i < colTypes.size(); i++) {
            if (colsList.contains(colTypes.get(i).getName())) {
                includedColumns[i] = true;
                includedColumnsNum++;
            }
        }

        // check included columns
        if (includedColumnsNum != cols.length && !option.isTolerantSchemaEvolution()) {
            checkValid = false;
            return;
        }

        // assign target columns
        targetColumns = new int[includedColumnsNum];
        int targetColIdx = 0;
        for (int i = 0; i < includedColumns.length; i++) {
            if (includedColumns[i]) {
                targetColumns[targetColIdx] = i;
                targetColIdx++;
            }
        }

        // get reader schema
        this.readerSchema = TypeDescription.createSchema(colTypes);

        // check reader schema
        if (readerSchema.getChildren() == null) {
            checkValid = false;
            return;
        }
        if (readerSchema.getChildren().size() != includedColumnsNum && !option.isTolerantSchemaEvolution()) {
            checkValid = false;
            return;
        }

        // get column readers
        readers = new ColumnReader[includedColumnsNum];
        List<TypeDescription> columnSchemas = readerSchema.getChildren();
        for (int i = 0; i < includedColumnsNum; i++) {
            readers[i] = ColumnReader.newColumnReader(columnSchemas.get(i));
        }

        checkValid = true;
    }

    private boolean read()
    {
        if (!checkValid) {
            return false;
        }

        everRead = true;

        List<PixelsProto.RowGroupStatistic> rowGroupStatistics
                = footer.getRowGroupStatsList();
        boolean[] includedRGs = new boolean[rowGroupStatistics.size()];
        if (includedRGs.length == 0) {
            return false;
        }

        // read row group statistics and find target row groups
        for (int i = 0; i < rowGroupStatistics.size(); i++) {
            includedRGs[i] = true;
        }
        targetRGs = new int[includedRGs.length];
        int targetRGIdx = 0;
        for (int i = 0; i < includedRGs.length; i++) {
            if (includedRGs[i]) {
                targetRGs[targetRGIdx] = i;
            }
        }

        // read row group footers
        rowGroupFooters =
                new PixelsProto.RowGroupFooter[includedRGs.length];
        for (int i = 0; i < includedRGs.length; i++) {
            if (includedRGs[i]) {
                PixelsProto.RowGroupInformation rowGroupInformation =
                        footer.getRowGroupInfos(i);
                long footerOffset = rowGroupInformation.getFooterOffset();
                long footerLength = rowGroupInformation.getFooterLength();
                byte[] footerBuffer = new byte[(int) footerLength];
                try {
                    physicalFSReader.seek(footerOffset);
                    physicalFSReader.readFully(footerBuffer);
                    rowGroupFooters[i] =
                            PixelsProto.RowGroupFooter.parseFrom(footerBuffer);
                }
                catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }
            }
        }

        // read chunk offset and length of each target column chunks
        List<ChunkId> chunks = new ArrayList<>();
        for (int rgIdx = 0; rgIdx < rowGroupFooters.length; rgIdx++) {
            PixelsProto.RowGroupIndex rowGroupIndex =
                    rowGroupFooters[rgIdx].getRowGroupIndexEntry();
            for (int targetColIdx = 0; targetColIdx < targetColumns.length; targetColIdx++) {
                int colIdx = targetColumns[targetColIdx];
                PixelsProto.ColumnChunkIndex chunkIndex =
                        rowGroupIndex.getColumnChunkIndexEntries(colIdx);
                ChunkId chunk = new ChunkId(rgIdx, colIdx,
                        chunkIndex.getChunkOffset(),
                        chunkIndex.getChunkLength());
                chunks.add(chunk);
            }
        }

        // sort chunks by starting offset
        chunks.sort(Comparator.comparingLong(ChunkId::getOffset));

        // get chunk blocks
        List<ChunkSeq> chunkSeqs = new ArrayList<>();
        ChunkSeq chunkSeq = new ChunkSeq();
        for (ChunkId chunk : chunks) {
            if (!chunkSeq.addChunk(chunk)) {
                chunkSeqs.add(chunkSeq);
                chunkSeq = new ChunkSeq();
            }
        }
        chunkSeqs.add(chunkSeq);

        // read chunk blocks into buffers
        this.chunkBuffers = new ByteBuf[includedRGs.length][includedColumns.length];
        try {
            for (ChunkSeq block : chunkSeqs) {
                if (block.getLength() == 0) {
                    continue;
                }
                int offset = (int) block.getOffset();
                int length = (int) block.getLength();
                byte[] chunkBlockBuffer = new byte[length];
                physicalFSReader.seek(offset);
                physicalFSReader.readFully(chunkBlockBuffer);
                ByteBuf chunkBlockBuf = Unpooled.wrappedBuffer(chunkBlockBuffer);
                List<ChunkId> chunkIds = block.getSortedChunks();
                int chunkSliceOffset = 0;
                for (ChunkId chunkId : chunkIds) {
                    int chunkLength = (int) chunkId.getLength();
                    int rgId = chunkId.getRowGroupId();
                    int colId = chunkId.getColumnId();
                    ByteBuf chunkBuf = chunkBlockBuf.slice(chunkSliceOffset, chunkLength);
                    chunkBuffers[rgId][colId] = chunkBuf;
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    /**
     * Read the next row batch.
     *
     * @param batch the row batch to read into
     * @return more rows available
     * @throws java.io.IOException
     */
    @Override
    public boolean nextBatch(VectorizedRowBatch batch) throws IOException
    {
        if (!checkValid) {
            return false;
        }

        if (!everRead) {
            if (!read()) {
                return false;
            }
        }

        // column vector projection
        ColumnVector[] columnVectors = batch.cols;
        if (columnVectors.length != includedColumns.length) {
            return false;
        }
        batch.projectionSize = targetColumns.length;
        System.arraycopy(targetColumns, 0, batch.projectedColumns, 0, targetColumns.length);

        if (readerCurRGIdx >= targetRGs.length) {
            return false;
        }

        PixelsProto.RowGroupInformation rowGroupInformation =
                footer.getRowGroupInfos(targetRGs[readerCurRGIdx]);
        int curBatchSize = (int) rowGroupInformation.getNumberOfRows() - readerCurRGOffset;
        if (batch.size + curBatchSize > batch.getMaxSize()) {
            curBatchSize = batch.getMaxSize() - batch.size;
        }

        // add record
        for (int i = 0; i < targetColumns.length; i++) {
            PixelsProto.ColumnEncoding encoding =
                    rowGroupFooters[targetRGs[readerCurRGIdx]].getRowGroupEncoding()
                            .getColumnChunkEncodings(targetColumns[i]);
            byte[] input = chunkBuffers[targetRGs[readerCurRGIdx]][targetColumns[i]].array();
            readers[i].read(input, encoding, readerCurRGOffset, curBatchSize, columnVectors[i]);
        }

        readerCurRGOffset += curBatchSize;
        rowIndex += curBatchSize;
        batch.size += curBatchSize;
        if (readerCurRGOffset >= rowGroupInformation.getNumberOfRows()) {
            readerCurRGIdx++;
            readerCurRGOffset = 0;
        }

        return true;
    }

    @Override
    public VectorizedRowBatch nextBatch() throws IOException
    {
        VectorizedRowBatch rowBatch = readerSchema.createRowBatch();
        nextBatch(rowBatch);
        return rowBatch;
    }

    @Override
    public VectorizedRowBatch nextBatch(int max) throws IOException
    {
        VectorizedRowBatch rowBatch = readerSchema.createRowBatch(max);
        nextBatch(rowBatch);
        return rowBatch;
    }

    /**
     * Get current row number
     *
     * @return number of the row currently being read
     */
    @Override
    public long getRowNumber()
    {
        if (!checkValid) {
            return -1L;
        }
        return rowIndex;
    }

    /**
     * Seek to specified row
     * Currently not supported
     *
     * @param rowIndex row number
     * @return seek success
     */
    @Deprecated
    @Override
    public boolean seekToRow(long rowIndex)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean skip(long rowNum)
    {
        return false;
    }

    /**
     * Cleanup and release resources
     */
    @Override
    public void close()
    {
        // release chunk buffer
        for (int i = 0; i < targetRGs.length; i++) {
            for (int j = 0; j < targetColumns.length; j++) {
                ByteBuf chunkBuf = chunkBuffers[targetRGs[i]][targetColumns[j]];
                if (chunkBuf != null) {
                    chunkBuf.release();
                }
            }
        }
    }
}
