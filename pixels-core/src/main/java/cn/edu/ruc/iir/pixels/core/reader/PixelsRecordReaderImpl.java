package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.common.metrics.BytesMsCost;
import cn.edu.ruc.iir.pixels.common.metrics.ReadPerfMetrics;
import cn.edu.ruc.iir.pixels.common.physical.PhysicalFSReader;
import cn.edu.ruc.iir.pixels.core.ChunkId;
import cn.edu.ruc.iir.pixels.core.ChunkSeq;
import cn.edu.ruc.iir.pixels.core.PixelsPredicate;
import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.stats.ColumnStats;
import cn.edu.ruc.iir.pixels.core.stats.StatsRecorder;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import com.alibaba.fastjson.JSON;
import io.airlift.log.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsRecordReaderImpl
        implements PixelsRecordReader
{
    private static final Logger logger = Logger.get(PixelsRecordReaderImpl.class.getName());

    private final PhysicalFSReader physicalFSReader;
    private final PixelsProto.PostScript postScript;
    private final PixelsProto.Footer footer;
    private final PixelsReaderOption option;
    private final boolean enableMetrics;
    private final boolean enableCache;
    private final String metricsDir;
    private final ReadPerfMetrics readPerfMetrics;

    private TypeDescription fileSchema;
    private boolean checkValid = false;
    private boolean everRead = false;
    private long rowIndex = 0L;
    private boolean[] includedColumns;   // columns included by reader option; if included, set true
    private int[] targetRGs;             // target row groups to read after matching reader option, each element represents row group id
    private int[] targetColumns;         // target columns to read after matching reader option, each element represents column id
    private int[] resultColumns;         // columns specified in option by user to read
    private VectorizedRowBatch resultRowBatch;

    private int targetRGNum = 0;         // number of target row groups
    private int curRGIdx = 0;            // index of current reading row group in targetRGs
    private int curRowInRG = 0;          // starting index of values to read by reader in current row group
    private int curChunkIdx = -1;        // current chunk id being read

    private PixelsProto.RowGroupFooter[] rowGroupFooters;
    private byte[][] chunkBuffers;       // buffers of each chunk in this file, arranged by chunk's row group id and column id
    private ColumnReader[] readers;      // column readers for each target columns

    private long completedBytes = 0L;

    public PixelsRecordReaderImpl(PhysicalFSReader physicalFSReader,
                                  PixelsProto.PostScript postScript,
                                  PixelsProto.Footer footer,
                                  PixelsReaderOption option,
                                  boolean enableMetrics,
                                  boolean enableCache,
                                  String metricsDir)
    {
        this.physicalFSReader = physicalFSReader;
        this.postScript = postScript;
        this.footer = footer;
        this.option = option;
        this.enableMetrics = enableMetrics;
        this.enableCache = enableCache;
        this.metricsDir = metricsDir;
        this.readPerfMetrics = new ReadPerfMetrics();
        checkBeforeRead();
    }

    private void checkBeforeRead()
    {
        // get file schema
        List<PixelsProto.Type> fileColTypes = footer.getTypesList();
        if (fileColTypes == null || fileColTypes.isEmpty()) {
            checkValid = false;
            return;
        }
        fileSchema = TypeDescription.createSchema(fileColTypes);
        if (fileSchema.getChildren() == null || fileSchema.getChildren().isEmpty()) {
            checkValid = false;
            return;
        }

        // check predicate schemas
        if (option.getPredicate().isPresent()) {
            // todo check if predicate matches file schema
        }

        // filter included columns
        int includedColumnsNum = 0;
        String[] cols = option.getIncludedCols();
        // if size of cols is 0, create an empty row batch
        if (cols.length == 0) {
            TypeDescription resultSchema = TypeDescription.createSchema(new ArrayList<>());
            this.resultRowBatch = resultSchema.createRowBatch(0);
            resultRowBatch.selectedInUse = false;
            resultRowBatch.selected = null;
            resultRowBatch.projectionSize = 0;
            return;
        }
        List<Integer> userColsList = new ArrayList<>();
        this.includedColumns = new boolean[fileColTypes.size()];
        for (String col : cols) {
            for (int j = 0; j < fileColTypes.size(); j++) {
                if (col.equalsIgnoreCase(fileColTypes.get(j).getName())) {
                    userColsList.add(j);
                    includedColumns[j] = true;
                    includedColumnsNum++;
                    break;
                }
            }
        }

        // check included columns
        if (includedColumnsNum != cols.length && !option.isTolerantSchemaEvolution()) {
            checkValid = false;
            return;
        }

        // create result columns storing result column ids by user specified order
        this.resultColumns = new int[includedColumnsNum];
        for (int i = 0; i < userColsList.size(); i++) {
            this.resultColumns[i] = userColsList.get(i);
        }

        // assign target columns
        int targetColumnsNum = new HashSet<>(userColsList).size();
        targetColumns = new int[targetColumnsNum];
        int targetColIdx = 0;
        for (int i = 0; i < includedColumns.length; i++) {
            if (includedColumns[i]) {
                targetColumns[targetColIdx] = i;
                targetColIdx++;
            }
        }

        // create column readers
        List<TypeDescription> columnSchemas = fileSchema.getChildren();
        readers = new ColumnReader[resultColumns.length];
        for (int i = 0; i < resultColumns.length; i++) {
            int index = resultColumns[i];
            readers[i] = ColumnReader.newColumnReader(columnSchemas.get(index));
        }

        // create result vectorized row batch
        List<PixelsProto.Type> resultTypes = new ArrayList<>();
        for (int resultColumn : resultColumns) {
            resultTypes.add(fileColTypes.get(resultColumn));
        }
        TypeDescription resultSchema = TypeDescription.createSchema(resultTypes);
        this.resultRowBatch = resultSchema.createRowBatch();
        // forbid selected array
        resultRowBatch.selectedInUse = false;
        resultRowBatch.selected = null;
        resultRowBatch.projectionSize = includedColumnsNum;

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

        Map<Integer, ColumnStats> columnStatsMap = new HashMap<>();
        // read row group statistics and find target row groups
        if (option.getPredicate().isPresent()) {
            List<TypeDescription> columnSchemas = fileSchema.getChildren();
            PixelsPredicate predicate = option.getPredicate().get();

            // first, get file level column statistic, if not matches, skip this file
            List<PixelsProto.ColumnStatistic> fileColumnStatistics = footer.getColumnStatsList();
            for (int idx : targetColumns) {
                columnStatsMap.put(idx,
                        StatsRecorder.create(columnSchemas.get(idx), fileColumnStatistics.get(idx)));
            }
            if (!predicate.matches(postScript.getNumberOfRows(), columnStatsMap)) {
                return false;
            }
            columnStatsMap.clear();

            // second, get row group statistics, if not matches, skip the row group
            for (int i = 0; i < includedRGs.length; i++) {
                PixelsProto.RowGroupStatistic rowGroupStatistic = rowGroupStatistics.get(i);
                List<PixelsProto.ColumnStatistic> rgColumnStatistics =
                        rowGroupStatistic.getColumnChunkStatsList();
                for (int idx : targetColumns) {
                    columnStatsMap.put(idx,
                            StatsRecorder.create(columnSchemas.get(idx), rgColumnStatistics.get(idx)));
                }
                includedRGs[i] = predicate.matches(footer.getRowGroupInfos(i).getNumberOfRows(), columnStatsMap);
            }
        }
        else {
            for (int i = 0; i < rowGroupStatistics.size(); i++) {
                includedRGs[i] = true;
            }
        }
        targetRGs = new int[includedRGs.length];
        int targetRGIdx = 0;
        for (int i = 0; i < includedRGs.length; i++) {
            if (includedRGs[i]) {
                targetRGs[targetRGIdx] = i;
                targetRGIdx++;
            }
        }
        targetRGNum = targetRGIdx;

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
        // cache
        if (enableCache) {
        }
        List<ChunkId> chunks = new ArrayList<>();
        for (int rgIdx = 0; rgIdx < rowGroupFooters.length; rgIdx++) {
            if (includedRGs[rgIdx]) {
                PixelsProto.RowGroupIndex rowGroupIndex =
                        rowGroupFooters[rgIdx].getRowGroupIndexEntry();
                for (int colIdx : targetColumns) {
                    PixelsProto.ColumnChunkIndex chunkIndex =
                            rowGroupIndex.getColumnChunkIndexEntries(colIdx);
                    ChunkId chunk = new ChunkId(rgIdx, colIdx,
                            chunkIndex.getChunkOffset(),
                            chunkIndex.getChunkLength());
                    chunks.add(chunk);
                }
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
                chunkSeq.addChunk(chunk);
            }
        }
        chunkSeqs.add(chunkSeq);

        // read chunk blocks into buffers
        this.chunkBuffers = new byte[includedRGs.length * includedColumns.length][];
        try {
            long offsetBeforeSeek = 0;
            long readDiskBegin = System.currentTimeMillis();
            for (ChunkSeq seq : chunkSeqs) {
                if (seq.getLength() == 0) {
                    continue;
                }
                int offset = (int) seq.getOffset();
                int length = (int) seq.getLength();
                completedBytes += length;
                byte[] chunkBlockBuffer = new byte[length];
                if (enableMetrics)
                {
                    long seekStart = System.currentTimeMillis();
                    physicalFSReader.seek(offset);
                    long seekEnd = System.currentTimeMillis();
                    BytesMsCost seekCost = new BytesMsCost();
                    seekCost.setBytes(Math.abs(offsetBeforeSeek - offset));
                    seekCost.setMs(seekEnd - seekStart);
                    readPerfMetrics.addSeek(seekCost);
                    offsetBeforeSeek = offset;

                    long readStart = System.currentTimeMillis();
                    physicalFSReader.readFully(chunkBlockBuffer);
                    long readEnd = System.currentTimeMillis();
                    BytesMsCost readCost = new BytesMsCost();
                    readCost.setBytes(length);
                    readCost.setMs(readEnd - readStart);
                    readPerfMetrics.addSeqRead(readCost);
                }
                else {
                    physicalFSReader.seek(offset);
                    physicalFSReader.readFully(chunkBlockBuffer);
                }
                List<ChunkId> chunkIds = seq.getSortedChunks();
                int chunkSliceOffset = 0;
                for (ChunkId chunkId : chunkIds) {
                    int chunkLength = (int) chunkId.getLength();
                    int rgId = chunkId.getRowGroupId();
                    int colId = chunkId.getColumnId();
                    byte[] chunkBytes = Arrays.copyOfRange(chunkBlockBuffer,
                            chunkSliceOffset, chunkSliceOffset + chunkLength);
                    chunkBuffers[rgId * includedColumns.length + colId] = chunkBytes;
                    chunkSliceOffset += chunkLength;
                }
            }
            long readDiskEnd = System.currentTimeMillis();
            logger.debug("[" + physicalFSReader.getPath().getName() + "] " + readDiskBegin + " " + readDiskEnd +
                    ", disk cost: " + (readDiskEnd - readDiskBegin));
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
     * @param batchSize the row batch to read into
     * @return more rows available
     * @throws java.io.IOException
     */
    @Override
    public VectorizedRowBatch readBatch(int batchSize) throws IOException
    {
        // project nothing, must be count(*)
        if (resultRowBatch.projectionSize == 0) {
            resultRowBatch.size = postScript.getNumberOfRows();
            resultRowBatch.endOfFile = true;
            return resultRowBatch;
        }

        resultRowBatch.reset();

        if (!checkValid) {
            resultRowBatch.endOfFile = true;
            return resultRowBatch;
        }

        if (!everRead) {
            if (!read()) {
                resultRowBatch.endOfFile = true;
                return resultRowBatch;
            }
        }

        // ensure size for result row batch
        resultRowBatch.ensureSize(batchSize);

        int rgRowCount = 0;
        int curBatchSize = 0;
        if (curRGIdx < targetRGNum) {
            rgRowCount = (int) footer.getRowGroupInfos(targetRGs[curRGIdx]).getNumberOfRows();
        }

        ColumnVector[] columnVectors = resultRowBatch.cols;
        while (resultRowBatch.size < batchSize && curRowInRG < rgRowCount) {
            // update current batch size
            curBatchSize = rgRowCount - curRowInRG;
            if (curBatchSize + resultRowBatch.size >= batchSize) {
                curBatchSize = batchSize - resultRowBatch.size;
            }

            // read vectors
            for (int i = 0; i < resultColumns.length; i++) {
                if (!columnVectors[i].duplicated) {
                    PixelsProto.RowGroupFooter rowGroupFooter =
                            rowGroupFooters[targetRGs[curRGIdx]];
                    PixelsProto.ColumnEncoding encoding = rowGroupFooter.getRowGroupEncoding()
                            .getColumnChunkEncodings(resultColumns[i]);
                    int index = targetRGs[curRGIdx] * includedColumns.length + resultColumns[i];
                    byte[] input = chunkBuffers[index];
                    if (curChunkIdx != -1 && curChunkIdx != index) {
                        chunkBuffers[curChunkIdx] = null;
                    }
                    curChunkIdx = index;
                    PixelsProto.ColumnChunkIndex chunkIndex = rowGroupFooter.getRowGroupIndexEntry()
                            .getColumnChunkIndexEntries(resultColumns[i]);
                    readers[i].read(input, encoding, curRowInRG, curBatchSize,
                            postScript.getPixelStride(), resultRowBatch.size, columnVectors[i], chunkIndex);
                }
            }

            // update current row index in the row group
            curRowInRG += curBatchSize;
            rowIndex += curBatchSize;
            resultRowBatch.size += curBatchSize;
            // update row group index if current row index exceeds max row count in the row group
            if (curRowInRG >= rgRowCount) {
                curRGIdx++;
                // if not end of file, update row count
                if (curRGIdx < targetRGNum) {
                    rgRowCount = (int) footer.getRowGroupInfos(targetRGs[curRGIdx]).getNumberOfRows();
                }
                // if end of file, set result vectorized row batch endOfFile
                else {
                    resultRowBatch.endOfFile = true;
                    break;
                }
                curRowInRG = 0;
            }
        }

        for (ColumnVector cv : columnVectors) {
            if (cv.duplicated) {
                cv.copyFrom(columnVectors[cv.originVecId]);
            }
        }

        return resultRowBatch;
    }

    @Override
    public VectorizedRowBatch readBatch() throws IOException
    {
        return readBatch(VectorizedRowBatch.DEFAULT_SIZE);
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

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    /**
     * Cleanup and release resources
     */
    @Override
    public void close()
    {
        completedBytes = 0;
        // release chunk buffer
        if (targetRGs != null) {
            for (int targetRG : targetRGs) {
                for (int targetColumn : targetColumns) {
                    chunkBuffers[targetRG * includedColumns.length + targetColumn] = null;
                }
            }
        }
//         write out read performance metrics
        if (enableMetrics) {
            String metrics = JSON.toJSONString(readPerfMetrics);
            Path metricsFilePath = Paths.get(metricsDir,
                    String.valueOf(System.nanoTime()) +
                    physicalFSReader.getPath().getName() +
                    ".json");
//            try {
//                RandomAccessFile raf = new RandomAccessFile(metricsFilePath.toFile(), "rw");
//                raf.seek(0L);
//                raf.writeChars(metrics);
//                raf.close();
//            }
//            catch (IOException e) {
//                e.printStackTrace();
//            }
        }
        // reset read performance metrics
        readPerfMetrics.clear();
    }
}
