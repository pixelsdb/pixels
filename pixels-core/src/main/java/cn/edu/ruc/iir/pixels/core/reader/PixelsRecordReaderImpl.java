package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.ChunkBlock;
import cn.edu.ruc.iir.pixels.core.ChunkId;
import cn.edu.ruc.iir.pixels.core.PhysicalFSReader;
import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;

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
    private final PixelsProto.PostScript postScript;
    private final PixelsProto.Footer footer;
    private final PixelsReaderOption option;

    private TypeDescription fileSchema;
    private TypeDescription readerSchema;
    private boolean checkValid = false;
    private boolean readValid = false;
    private long rowIndex = 0L;
    private boolean[] includedColumns;   // columns included by reader option; if included, set true
    private boolean[] includedRGs;       // row groups included by reader option; if included, set true
    private int[] targetRGs;
    private int[] targetColumns;
    private int includedColumnsNum;

    private ByteBuf[][] chunkBuffers;

    public PixelsRecordReaderImpl(PhysicalFSReader physicalFSReader,
                                  PixelsProto.PostScript postScript,
                                  PixelsProto.Footer footer,
                                  PixelsReaderOption option)
    {
        this.physicalFSReader = physicalFSReader;
        this.postScript = postScript;
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
        this.fileSchema = TypeDescription.createSchema(colTypes);
        if (fileSchema.getChildren() == null || fileSchema.getChildren().isEmpty()) {
            checkValid = false;
            return;
        }

        // check if predicate matches file schema

        // get included columns
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
            }
        }

        // check included columns
        for (boolean col : includedColumns) {
            includedColumnsNum += col ? 1 : 0;
        }
        if (includedColumnsNum != cols.length && !option.isTolerantSchemaEvolution()) {
            checkValid = false;
            return;
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

        checkValid = true;
    }

    private void read()
    {
        if (!checkValid) {
            readValid = false;
            return;
        }

        List<PixelsProto.RowGroupStatistic> rowGroupStatistics
                = footer.getRowGroupStatsList();
        this.includedRGs = new boolean[rowGroupStatistics.size()];
        if (includedRGs.length == 0) {
            readValid = false;
            return;
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
        PixelsProto.RowGroupFooter[] rowGroupFooters =
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
                    readValid = false;
                    return;
                }
            }
        }

        // read chunk offset and length of each target column chunks
        List<ChunkId> chunks = new ArrayList<>();
        targetColumns = new int[includedColumnsNum];      // index of included columns
        int targetColumnsIdx = 0;
        for (int i = 0; i < includedColumns.length; i++) {
            if (includedColumns[i]) {
                targetColumns[targetColumnsIdx] = i;
            }
        }
        for (int rgIdx = 0; rgIdx < rowGroupFooters.length; rgIdx++) {
            PixelsProto.RowGroupIndex rowGroupIndex =
                    rowGroupFooters[rgIdx].getRowGroupIndexEntry();
            for (int targetColIdx = 0; targetColIdx < targetColumns.length; targetColIdx++) {
                int colIdx = targetColumns[targetColumnsIdx];
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
        List<ChunkBlock> chunkBlocks = new ArrayList<>();
        ChunkBlock chunkBlock = new ChunkBlock();
        for (ChunkId chunk : chunks) {
            if (!chunkBlock.addChunk(chunk)) {
                chunkBlocks.add(chunkBlock);
                chunkBlock = new ChunkBlock();
            }
        }

        // read chunk blocks into buffers
        this.chunkBuffers = new ByteBuf[includedRGs.length][includedColumns.length];
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
        if (!checkValid || !readValid) {
            return false;
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
     *
     */
    @Override
    public void close()
    {
        // release chunk buffer
        for (int i = 0; i < targetRGs.length; i++) {
            for (int j = 0; j < targetColumns.length; j++) {
                chunkBuffers[i][j].release();
            }
        }
    }
}
