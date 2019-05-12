package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.cache.PixelsCacheReader;
import cn.edu.ruc.iir.pixels.common.exception.FSException;
import cn.edu.ruc.iir.pixels.common.metrics.ReadPerfMetrics;
import cn.edu.ruc.iir.pixels.common.physical.PhysicalFSReader;
import cn.edu.ruc.iir.pixels.core.ChunkId;
import cn.edu.ruc.iir.pixels.core.ChunkSeq;
import cn.edu.ruc.iir.pixels.core.PixelsFooterCache;
import cn.edu.ruc.iir.pixels.core.PixelsPredicate;
import cn.edu.ruc.iir.pixels.core.PixelsProto;
import cn.edu.ruc.iir.pixels.core.TypeDescription;
import cn.edu.ruc.iir.pixels.core.stats.ColumnStats;
import cn.edu.ruc.iir.pixels.core.stats.StatsRecorder;
import cn.edu.ruc.iir.pixels.core.vector.ColumnVector;
import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
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
    private static final Logger logger = LogManager.getLogger(PixelsRecordReaderImpl.class);

    private final PhysicalFSReader physicalFSReader;
    private final PixelsProto.PostScript postScript;
    private final PixelsProto.Footer footer;
    private final PixelsReaderOption option;
    private final int RGStart;
    private int RGLen;
    private final boolean enableMetrics;
    private final String metricsDir;
    private final ReadPerfMetrics readPerfMetrics;
    private final boolean enableCache;
    private final List<String> cacheOrder;
    private final PixelsCacheReader cacheReader;
    private final PixelsFooterCache pixelsFooterCache;
    private final String fileName;

    private TypeDescription fileSchema;
    private boolean checkValid = false;
    private boolean everRead = false;
    private long rowIndex = 0L;
    private boolean[] includedColumns;   // columns included by reader option; if included, set true
    private int[] targetRGs;             // target row groups to read after matching reader option, each element represents a row group id
    private int[] targetColumns;         // target columns to read after matching reader option, each element represents a column id
    private int[] resultColumns;         // columns specified in option by user to read
    private VectorizedRowBatch resultRowBatch;

    private int targetRGNum = 0;         // number of target row groups
    private int curRGIdx = 0;            // index of current reading row group in targetRGs
    private int curRowInRG = 0;          // starting index of values to read by reader in current row group

    private PixelsProto.RowGroupFooter[] rowGroupFooters;
    private byte[][] chunkBuffers;       // buffers of each chunk in this file, arranged by chunk's row group id and column id
    private ColumnReader[] readers;      // column readers for each target columns

    private long completedBytes = 0L;

    public PixelsRecordReaderImpl(PhysicalFSReader physicalFSReader,
                                  PixelsProto.PostScript postScript,
                                  PixelsProto.Footer footer,
                                  PixelsReaderOption option,
                                  boolean enableMetrics,
                                  String metricsDir,
                                  boolean enableCache,
                                  List<String> cacheOrder,
                                  PixelsCacheReader cacheReader,
                                  PixelsFooterCache pixelsFooterCache)
    {
        this.physicalFSReader = physicalFSReader;
        this.postScript = postScript;
        this.footer = footer;
        this.option = option;
        this.RGStart = option.getRGStart();
        this.RGLen = option.getRGLen();
        this.enableMetrics = enableMetrics;
        this.metricsDir = metricsDir;
        this.readPerfMetrics = new ReadPerfMetrics();
        this.enableCache = enableCache;
        this.cacheOrder = cacheOrder;
        this.cacheReader = cacheReader;
        this.pixelsFooterCache = pixelsFooterCache;
        this.fileName = physicalFSReader.getPath().getName();
        long checkStartNano = System.nanoTime();
        checkBeforeRead();
        long checkCost = System.nanoTime() - checkStartNano;
        logger.debug("[check before read]" + checkCost);
    }

    private void checkBeforeRead()
    {
        // get file schema
        List<PixelsProto.Type> fileColTypes = footer.getTypesList();
        if (fileColTypes == null || fileColTypes.isEmpty())
        {
            checkValid = false;
            return;
        }
        fileSchema = TypeDescription.createSchema(fileColTypes);
        if (fileSchema.getChildren() == null || fileSchema.getChildren().isEmpty())
        {
            checkValid = false;
            return;
        }

        // check RGStart and RGLen are within the range of actual number of row groups
        int rgNum = footer.getRowGroupInfosCount();
        if (RGStart >= rgNum)
        {
            checkValid = false;
            return;
        }
        if (RGStart + RGLen > rgNum)
        {
            RGLen = rgNum - RGStart;
        }

        // filter included columns
        int includedColumnsNum = 0;
        String[] optionIncludedCols = option.getIncludedCols();
        // if size of cols is 0, create an empty row batch
        if (optionIncludedCols.length == 0)
        {
            TypeDescription resultSchema = TypeDescription.createSchema(new ArrayList<>());
            this.resultRowBatch = resultSchema.createRowBatch(0);
            resultRowBatch.selectedInUse = false;
            resultRowBatch.selected = null;
            resultRowBatch.projectionSize = 0;
            checkValid = true;
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
                    includedColumnsNum++;
                    break;
                }
            }
        }

        // check included columns
        if (includedColumnsNum != optionIncludedCols.length && !option.isTolerantSchemaEvolution())
        {
            checkValid = false;
            return;
        }

        // create result columns storing result column ids by user specified order
        this.resultColumns = new int[includedColumnsNum];
        for (int i = 0; i < optionColsIndices.size(); i++)
        {
            this.resultColumns[i] = optionColsIndices.get(i);
        }

        // assign target columns, ordered by original column order in schema
        int targetColumnsNum = new HashSet<>(optionColsIndices).size();
        targetColumns = new int[targetColumnsNum];
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
        List<PixelsProto.Type> resultTypes = new ArrayList<>();
        for (int resultColumn : resultColumns)
        {
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
        if (!checkValid)
        {
            return false;
        }

        long readStartNano = System.nanoTime();
        everRead = true;

        List<PixelsProto.RowGroupStatistic> rowGroupStatistics
                = footer.getRowGroupStatsList();
        if (RGLen == -1)
        {
            RGLen = rowGroupStatistics.size() - RGStart;
        }
        boolean[] includedRGs = new boolean[RGLen];
        if (includedRGs.length == 0)
        {
            return false;
        }

        Map<Integer, ColumnStats> columnStatsMap = new HashMap<>();
        // read row group statistics and find target row groups
        if (option.getPredicate().isPresent())
        {
            List<TypeDescription> columnSchemas = fileSchema.getChildren();
            PixelsPredicate predicate = option.getPredicate().get();

            // first, get file level column statistic, if not matches, skip this file
            List<PixelsProto.ColumnStatistic> fileColumnStatistics = footer.getColumnStatsList();
            for (int id : targetColumns)
            {
                columnStatsMap.put(id,
                        StatsRecorder.create(columnSchemas.get(id), fileColumnStatistics.get(id)));
            }
            if (!predicate.matches(postScript.getNumberOfRows(), columnStatsMap))
            {
                return false;
            }
            columnStatsMap.clear();

            // second, get row group statistics, if not matches, skip the row group
            for (int i = 0; i < RGLen; i++)
            {
                PixelsProto.RowGroupStatistic rowGroupStatistic = rowGroupStatistics.get(i + RGStart);
                List<PixelsProto.ColumnStatistic> rgColumnStatistics =
                        rowGroupStatistic.getColumnChunkStatsList();
                for (int id : targetColumns)
                {
                    columnStatsMap.put(id,
                            StatsRecorder.create(columnSchemas.get(id), rgColumnStatistics.get(id)));
                }
                includedRGs[i] = predicate.matches(footer.getRowGroupInfos(i).getNumberOfRows(), columnStatsMap);
            }
        }
        else
        {
            for (int i = 0; i < RGLen; i++)
            {
                includedRGs[i] = true;
            }
        }
        targetRGs = new int[includedRGs.length];
        int targetRGIdx = 0;
        for (int i = 0; i < RGLen; i++)
        {
            if (includedRGs[i])
            {
                targetRGs[targetRGIdx] = i + RGStart;
                targetRGIdx++;
            }
        }
        targetRGNum = targetRGIdx;

        // read row group footers
        rowGroupFooters =
                new PixelsProto.RowGroupFooter[targetRGNum];
        long readRGFootersStartNano = System.nanoTime();
        for (int i = 0; i < targetRGNum; i++)
        {
            int rgId = targetRGs[i];
            String rgCacheId = fileName + "-" + rgId;
            // TODO: is it meaningful to cache footers? record reader is created to read a split and then released.
            PixelsProto.RowGroupFooter rowGroupFooter = pixelsFooterCache.getRGFooter(rgCacheId);
            // cache miss, read from disk and put it into cache
            if (rowGroupFooter == null)
            {
                logger.debug("[rg cache miss]" + rgCacheId);
                PixelsProto.RowGroupInformation rowGroupInformation =
                        footer.getRowGroupInfos(rgId);
                long footerOffset = rowGroupInformation.getFooterOffset();
                long footerLength = rowGroupInformation.getFooterLength();
                byte[] footerBuffer = new byte[(int) footerLength];
                try {
                    physicalFSReader.seek(footerOffset);
                    physicalFSReader.readFully(footerBuffer);
                    rowGroupFooters[i] =
                            PixelsProto.RowGroupFooter.parseFrom(footerBuffer);
                    pixelsFooterCache.putRGFooter(rgCacheId, rowGroupFooters[i]);
                }
                catch (IOException e) {
                    e.printStackTrace();
                    return false;
                }
            }
            // cache hit
            else
            {
                logger.debug("[rg cache hit]" + rgCacheId);
                rowGroupFooters[i] = rowGroupFooter;
            }
        }
        long readRGFooterEndNano = System.nanoTime();
        logger.debug("[read rg footers]" + fileName + "," + (readRGFooterEndNano - readRGFootersStartNano));

        // read chunk offset and length of each target column chunks
        this.chunkBuffers = new byte[targetRGNum * includedColumns.length][];
        List<ChunkId> chunks = new ArrayList<>();
        // read cached data which are in need
        if (enableCache) {
            long cacheReadStartNano = System.nanoTime();
            int cacheReadCount = 0;
            long cacheReadSize = 0L;
            for (int rgIdx = 0; rgIdx < targetRGNum; rgIdx++) {
                int rgId = rgIdx + RGStart;
                PixelsProto.RowGroupIndex rowGroupIndex =
                        rowGroupFooters[rgIdx].getRowGroupIndexEntry();
                for (int colId : targetColumns) {
                    String cacheIdentifier = "" + rgId + ":" + colId;
                    // if cached, read from cache files
                    if (cacheOrder.contains(cacheIdentifier)) {
                        long blockId = -1;
                        try
                        {
                            blockId = physicalFSReader.getCurrentBlockId();
                        } catch (FSException e)
                        {
                            logger.error(e);
                            return false;
                        }
                        byte[] columnlet = cacheReader.get(blockId, (short) rgId, (short) colId);
                        // if cache hit, read columnlet into buffer
                        if (columnlet != null && columnlet.length > 0) {
                            cacheReadCount++;
                            cacheReadSize += columnlet.length;
                            int bufferIdx = rgIdx * includedColumns.length + colId;
                            chunkBuffers[bufferIdx] = columnlet;
                        }
                        // if cache miss, add chunkId to be read from disks
                        else {
                            PixelsProto.ColumnChunkIndex chunkIndex =
                                    rowGroupIndex.getColumnChunkIndexEntries(colId);
                            ChunkId chunk = new ChunkId(rgIdx, colId,
                                                        chunkIndex.getChunkOffset(),
                                                        chunkIndex.getChunkLength());
                            chunks.add(chunk);
                        }
                    }
                    // else, read from disks
                    else {
                        PixelsProto.ColumnChunkIndex chunkIndex =
                                rowGroupIndex.getColumnChunkIndexEntries(colId);
                        ChunkId chunk = new ChunkId(rgIdx, colId,
                                                    chunkIndex.getChunkOffset(),
                                                    chunkIndex.getChunkLength());
                        chunks.add(chunk);
                    }
                }
            }
            long cacheReadEndNano = System.nanoTime();
            long cacheReadCost = cacheReadEndNano - cacheReadStartNano;
            logger.debug("[cache stat]" + fileName + "," + cacheReadCount + "," + cacheReadSize + "," + cacheReadCost);
        }
        else {
            for (int rgIdx = 0; rgIdx < targetRGNum; rgIdx++) {
                PixelsProto.RowGroupIndex rowGroupIndex =
                        rowGroupFooters[rgIdx].getRowGroupIndexEntry();
                for (int colId : targetColumns) {
                    PixelsProto.ColumnChunkIndex chunkIndex =
                            rowGroupIndex.getColumnChunkIndexEntries(colId);
                    ChunkId chunk = new ChunkId(rgIdx, colId,
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
        for (ChunkId chunk : chunks)
        {
            if (!chunkSeq.addChunk(chunk))
            {
                chunkSeqs.add(chunkSeq);
                chunkSeq = new ChunkSeq();
                chunkSeq.addChunk(chunk);
            }
        }
        chunkSeqs.add(chunkSeq);

        // read chunk blocks into buffers
        try
        {
            long diskReadStartNano = System.nanoTime();
            int diskReadCount = 0;
            long diskReadSize = 0L;
            long offsetBeforeSeek = 0;
            for (ChunkSeq seq : chunkSeqs)
            {
                if (seq.getLength() == 0)
                {
                    continue;
                }
                diskReadCount++;
                int offset = (int) seq.getOffset();
                int length = (int) seq.getLength();
                diskReadSize += length;
                completedBytes += length;
                byte[] chunkBlockBuffer = new byte[length];
//                if (enableMetrics)
//                {
//                    long seekStart = System.currentTimeMillis();
//                    physicalFSReader.seek(offset);
//                    long seekEnd = System.currentTimeMillis();
//                    BytesMsCost seekCost = new BytesMsCost();
//                    seekCost.setBytes(Math.abs(offsetBeforeSeek - offset));
//                    seekCost.setMs(seekEnd - seekStart);
//                    readPerfMetrics.addSeek(seekCost);
//                    offsetBeforeSeek = offset;
//
//                    long readStart = System.currentTimeMillis();
//                    physicalFSReader.readFully(chunkBlockBuffer);
//                    long readEnd = System.currentTimeMillis();
//                    BytesMsCost readCost = new BytesMsCost();
//                    readCost.setBytes(length);
//                    readCost.setMs(readEnd - readStart);
//                    readPerfMetrics.addSeqRead(readCost);
//                }
//                else
//                {
                    physicalFSReader.seek(offset);
                    physicalFSReader.readFully(chunkBlockBuffer);
//                }
                List<ChunkId> chunkIds = seq.getSortedChunks();
                int chunkSliceOffset = 0;
                for (ChunkId chunkId : chunkIds)
                {
                    int chunkLength = (int) chunkId.getLength();
                    int rgIdx = chunkId.getRowGroupId();
                    int colId = chunkId.getColumnId();
                    // TODO: do not copy, we should use a buffer slice wrapper instead of byte array.
                    byte[] chunkBytes = Arrays.copyOfRange(chunkBlockBuffer,
                            chunkSliceOffset, chunkSliceOffset + chunkLength);
                    chunkBuffers[rgIdx * includedColumns.length + colId] = chunkBytes;
                    chunkSliceOffset += chunkLength;
                }
            }
            long diskReadEndNano = System.nanoTime();
            long diskReadCost = diskReadEndNano - diskReadStartNano;
            logger.debug("[disk stat]" + fileName + "," + diskReadCount + "," + diskReadSize + "," + diskReadCost);
        }
        catch (IOException e)
        {
            logger.error(e);
            return false;
        }
        long readEndNano = System.nanoTime();
        logger.debug("[pixels read]" + fileName + "," + (readEndNano - readStartNano));

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
        if (!checkValid)
        {
            TypeDescription resultSchema = TypeDescription.createSchema(new ArrayList<>());
            this.resultRowBatch = resultSchema.createRowBatch(0);
            resultRowBatch.selectedInUse = false;
            resultRowBatch.selected = null;
            resultRowBatch.projectionSize = 0;
            resultRowBatch.endOfFile = true;
            return resultRowBatch;
        }

        // project nothing, must be count(*)
        if (resultRowBatch.projectionSize == 0)
        {
            resultRowBatch.size = postScript.getNumberOfRows();
            resultRowBatch.endOfFile = true;
            return resultRowBatch;
        }

        resultRowBatch.reset();

        if (!everRead)
        {
            if (!read())
            {
                resultRowBatch.endOfFile = true;
                return resultRowBatch;
            }
        }

        // ensure size for result row batch
        resultRowBatch.ensureSize(batchSize);

        /**
         * TODO:
         * Here, we immediately decode and copy data items from chunk buffers into resultRowBatch.cols.
         * If the resultRowBatch is not immediately consumed by upper layers (such as a presto worker),
         * the decoded column vectors will hold the memory, increasing risk of OOM and GC problems.
         * We should put such decoding in a separate function, so that clients can make their choice of
         * decoding lazily or not.
         * And to support presto lazy block, we have to support decoding one column (in a batch) at a time.
         */

        int rgRowCount = 0;
        int curBatchSize = 0;
        if (curRGIdx < targetRGNum)
        {
            rgRowCount = (int) footer.getRowGroupInfos(targetRGs[curRGIdx]).getNumberOfRows();
        }

        ColumnVector[] columnVectors = resultRowBatch.cols;
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
                    PixelsProto.RowGroupFooter rowGroupFooter =
                            rowGroupFooters[curRGIdx];
                    PixelsProto.ColumnEncoding encoding = rowGroupFooter.getRowGroupEncoding()
                            .getColumnChunkEncodings(resultColumns[i]);
                    int index = curRGIdx * includedColumns.length + resultColumns[i];
                    PixelsProto.ColumnChunkIndex chunkIndex = rowGroupFooter.getRowGroupIndexEntry()
                            .getColumnChunkIndexEntries(resultColumns[i]);
                    /**
                     * Chunk buffer is decoded in read, this produces more garbage in heap. We can consider using late decoding,
                     * which only decodes a value from buffer when the value is needed, and the decoded value is stored in stack
                     * instead of in heap, without increasing the burden of garbage collection.
                     * But reading all items from chunk buffer to column vector in a single function may benefit from
                     * vectorized execution. This is a trade-off.
                     */
                    readers[i].read(chunkBuffers[index], encoding, curRowInRG, curBatchSize,
                            postScript.getPixelStride(), resultRowBatch.size, columnVectors[i], chunkIndex);
                }
            }

            // update current row index in the row group
            curRowInRG += curBatchSize;
            rowIndex += curBatchSize;
            resultRowBatch.size += curBatchSize;
            // update row group index if current row index exceeds max row count in the row group
            if (curRowInRG >= rgRowCount)
            {
                curRGIdx++;
                // if not end of file, update row count
                if (curRGIdx < targetRGNum)
                {
                    rgRowCount = (int) footer.getRowGroupInfos(targetRGs[curRGIdx]).getNumberOfRows();
                }
                // if end of file, set result vectorized row batch endOfFile
                else
                {
                    resultRowBatch.endOfFile = true;
                    break;
                }
                curRowInRG = 0;
            }
        }

        for (ColumnVector cv : columnVectors)
        {
            if (cv.duplicated)
            {
                // TODO: why copy duplicated cvs?
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
        if (!checkValid)
        {
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
        if (chunkBuffers != null)
        {
            for (int i = 0; i < chunkBuffers.length; i++)
            {
                chunkBuffers[i] = null;
            }
        }
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
