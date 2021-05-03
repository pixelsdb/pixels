/*
 * Copyright 2017-2019 PixelsDB.
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

import io.pixelsdb.pixels.cache.ColumnletId;
import io.pixelsdb.pixels.cache.PixelsCacheReader;
import io.pixelsdb.pixels.common.exception.FSException;
import io.pixelsdb.pixels.common.metrics.ReadPerfMetrics;
import io.pixelsdb.pixels.common.physical.PhysicalFSReader;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.exception.PixelsReaderException;
import io.pixelsdb.pixels.core.stats.ColumnStats;
import io.pixelsdb.pixels.core.stats.StatsRecorder;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author guodong
 * @author hank
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
    private boolean everPrepared = false;
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
    private ByteBuffer[] chunkBuffers;       // buffers of each chunk in this file, arranged by chunk's row group id and column id
    private ColumnReader[] readers;      // column readers for each target columns

    private long diskReadBytes = 0L;
    private long cacheReadBytes = 0L;
    private long readTimeNanos = 0L;

    public PixelsRecordReaderImpl(PhysicalFSReader physicalFSReader,
                                  PixelsProto.PostScript postScript,
                                  PixelsProto.Footer footer,
                                  PixelsReaderOption option,
                                  boolean enableMetrics,
                                  String metricsDir,
                                  boolean enableCache,
                                  List<String> cacheOrder,
                                  PixelsCacheReader cacheReader,
                                  PixelsFooterCache pixelsFooterCache) throws IOException
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
        checkBeforeRead();
    }

    private void checkBeforeRead() throws IOException
    {
        // get file schema
        List<PixelsProto.Type> fileColTypes = footer.getTypesList();
        if (fileColTypes == null || fileColTypes.isEmpty())
        {
            checkValid = false;
            //throw new IOException("ISSUE-103: type list is empty.");
            return;
        }
        fileSchema = TypeDescription.createSchema(fileColTypes);
        if (fileSchema.getChildren() == null || fileSchema.getChildren().isEmpty())
        {
            checkValid = false;
            //throw new IOException("ISSUE-103: file schema is empty.");
            return;
        }

        // check RGStart and RGLen are within the range of actual number of row groups
        int rgNum = footer.getRowGroupInfosCount();
        if (RGStart >= rgNum)
        {
            checkValid = false;
            //throw new IOException("ISSUE-103: row group start is out of bound.");
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
            // Issue #103: set the following members to null.
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
                    includedColumnsNum++;
                    break;
                }
            }
        }

        // check included columns
        if (includedColumnsNum != optionIncludedCols.length && !option.isTolerantSchemaEvolution())
        {
            checkValid = false;
            //throw new IOException("ISSUE-103: includedColumnsNum is " + includedColumnsNum +
            //        " while optionIncludedCols.length is " + optionIncludedCols.length);
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
        this.resultRowBatch = resultSchema.createRowBatch(0);
        // forbid selected array
        resultRowBatch.selectedInUse = false;
        resultRowBatch.selected = null;
        resultRowBatch.projectionSize = includedColumnsNum;

        checkValid = true;
    }

    /**
     * This method is to prepare the internal status for read operations.
     * It should only return false when there is an error. Special cases
     * must be processed correctly and return true.
     * @return
     * @throws IOException
     */
    private boolean prepareRead() throws IOException
    {
        if (!checkValid)
        {
            return false;
        }

        everPrepared = true;

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

        // read row group statistics and find target row groups
        if (option.getPredicate().isPresent())
        {
            PixelsPredicate predicate = option.getPredicate().get();
            if (option.getIncludedCols().length == 0)
            {
                /**
                 * Issue #103:
                 * If there is no included columns while predicate is present,
                 * The predicate(s) should be constant expressions.
                 */
                if (predicate.matchesAll())
                {
                    for (int i = 0; i < RGLen; i++)
                    {
                        includedRGs[i] = true;
                    }
                }
                else if (predicate.matchesNone())
                {
                    for (int i = 0; i < RGLen; i++)
                    {
                        includedRGs[i] = false;
                    }
                }
                else
                {
                    throw new PixelsReaderException(
                            "predicate does not match none or all while included columns is empty.");
                }
            }
            else
            {
                Map<Integer, ColumnStats> columnStatsMap = new HashMap<>();
                List<TypeDescription> columnSchemas = fileSchema.getChildren();

                // first, get file level column statistic, if not matches, skip this file
                List<PixelsProto.ColumnStatistic> fileColumnStatistics = footer.getColumnStatsList();
                for (int id : targetColumns)
                {
                    columnStatsMap.put(id,
                            StatsRecorder.create(columnSchemas.get(id), fileColumnStatistics.get(id)));
                }
                if (!predicate.matches(postScript.getNumberOfRows(), columnStatsMap))
                {
                    /**
                     * Issue #103:
                     * 1. matches() is fixed in this issue, but it is not sure if there is
                     * any further problems with it, as the related domain APIs in presto spi is mysterious.
                     *
                     * 2. Whenever predicate does not match any column statistics, we should be return
                     * false. Instead, we must make sure that includedRGs will be filled with false values.
                     * By this way, the subsequent methods such as read() an readBatch() can skip all row
                     * groups in this file without additional overheads, as targetRGs would be empty and
                     * targetRGNum would be 0.
                     */
                    //return false;
                    for (int i = 0; i < RGLen; i++)
                    {
                        includedRGs[i] = false;
                    }
                }
                else
                {
                    // columnStatsMap.clear();
                    // second, get row group statistics, if not matches, skip the row group
                    for (int i = 0; i < RGLen; i++)
                    {
                        // Issue #103: columnStatsMap should be cleared for each row group.
                        columnStatsMap.clear();
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
        for (int i = 0; i < targetRGNum; i++)
        {
            int rgId = targetRGs[i];
            String rgCacheId = fileName + "-" + rgId;
            PixelsProto.RowGroupFooter rowGroupFooter = pixelsFooterCache.getRGFooter(rgCacheId);
            // cache miss, read from disk and put it into cache
            if (rowGroupFooter == null)
            {
                PixelsProto.RowGroupInformation rowGroupInformation =
                        footer.getRowGroupInfos(rgId);
                long footerOffset = rowGroupInformation.getFooterOffset();
                long footerLength = rowGroupInformation.getFooterLength();
                byte[] footerBuffer = new byte[(int) footerLength];
                try
                {
                    physicalFSReader.seek(footerOffset);
                    physicalFSReader.readFully(footerBuffer);
                    rowGroupFooters[i] =
                            PixelsProto.RowGroupFooter.parseFrom(footerBuffer);
                    pixelsFooterCache.putRGFooter(rgCacheId, rowGroupFooters[i]);
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                    return false;
                }
            }
            // cache hit
            else
            {
                rowGroupFooters[i] = rowGroupFooter;
            }
        }
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
    private boolean read() throws IOException
    {
        if (!checkValid)
        {
            return false;
        }

        if (!everPrepared)
        {
            if (prepareRead() == false)
            {
                return false;
            }
        }

        everRead = true;

        if (targetRGNum == 0)
        {
            /**
             * Issue #103:
             * No row groups to read, return false, and then resultRowBatch.endOfFile
             * will be set in readBatch().
             */
            return false;
        }

        // read chunk offset and length of each target column chunks
        this.chunkBuffers = new ByteBuffer[targetRGNum * includedColumns.length];
        List<ChunkId> diskChunks = new ArrayList<>(targetRGNum * targetColumns.length);
        // read cached data which are in need
        if (enableCache)
        {
            long blockId;
            try
            {
                blockId = physicalFSReader.getCurrentBlockId();
            }
            catch (IOException | FSException e)
            {
                logger.error(e);
                throw new IOException("failed to get block id.", e);
                //return false;
            }
            List<ColumnletId> cacheChunks = new ArrayList<>(targetRGNum * targetColumns.length);
            // find cached chunks
            for (int colId : targetColumns)
            {
                // direct cache read is just for debug, so we just get this parameter here for simplicity.
                // TODO: remove this line when debug is finished.
                boolean direct = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("cache.read.direct"));
                for (int rgIdx = 0; rgIdx < targetRGNum; rgIdx++)
                {
                    /**
                     * Issue #103:
                     * rgId should be targetRGs[rgIdx] instead of (rgIdx + RGStart).
                     */
                    // int rgId = rgIdx + RGStart;
                    int rgId = targetRGs[rgIdx];
                    // TODO: not only columnlets in cacheOrder are cached.
                    String cacheIdentifier = rgId + ":" + colId;
                    // if cached, read from cache files
                    if (cacheOrder.contains(cacheIdentifier))
                    {
                        ColumnletId chunkId = new ColumnletId((short) rgId, (short) colId, direct);
                        cacheChunks.add(chunkId);
                    }
                    // if cache miss, add chunkId to be read from disks
//                    /*
                    else
                    {
                        PixelsProto.RowGroupIndex rowGroupIndex =
                                rowGroupFooters[rgIdx].getRowGroupIndexEntry();
                        PixelsProto.ColumnChunkIndex chunkIndex =
                                rowGroupIndex.getColumnChunkIndexEntries(colId);
                        /**
                         * Comments added in Issue #103:
                         * It is not a bug to use rgIdx as the rowGroupId of ChunkId.
                         * ChunkId is to index the column chunk content in chunkBuffers
                         * but not the column chunk in Pixels file.
                         */
                        ChunkId chunk = new ChunkId(rgIdx, colId,
                                chunkIndex.getChunkOffset(),
                                chunkIndex.getChunkLength());
                        diskChunks.add(chunk);
                    }
//                    */
                }
            }
            // read cached chunks
            long cacheReadStartNano = System.nanoTime();
            for (ColumnletId columnletId : cacheChunks)
            {
                short rgId = columnletId.rowGroupId;
                short colId = columnletId.columnId;
//                long getBegin = System.nanoTime();
                ByteBuffer columnlet = cacheReader.get(blockId, rgId, colId, columnletId.direct);
//                long getEnd = System.nanoTime();
//                logger.debug("[cache get]: " + columnlet.length + "," + (getEnd - getBegin));
                chunkBuffers[(rgId - RGStart) * includedColumns.length + colId] = columnlet;
                if (columnlet == null || columnlet.capacity() == 0)
                {
                    /**
                     * Issue #67 (patch):
                     * Deal with null or empty cache chunk.
                     * If cache read failed (e.g. cache read timeout), columnlet will be null.
                     * In this condition, we have to read the columnlet from disk.
                     */
                    int rgIdx = rgId - RGStart;
                    PixelsProto.RowGroupIndex rowGroupIndex =
                            rowGroupFooters[rgIdx].getRowGroupIndexEntry();
                    PixelsProto.ColumnChunkIndex chunkIndex =
                            rowGroupIndex.getColumnChunkIndexEntries(colId);
                    ChunkId diskChunk = new ChunkId(rgIdx, colId, chunkIndex.getChunkOffset(),
                            chunkIndex.getChunkLength());
                    diskChunks.add(diskChunk);
                }
                else
                {
                    this.cacheReadBytes += columnlet.capacity();
                }
            }
            long cacheReadEndNano = System.nanoTime();
            long cacheReadCost = cacheReadEndNano - cacheReadStartNano;
            /*
            // We used deal with null or empty cache chunk here to get more accurate cacheReadCost.
            // In Issue #67 (patch), we move the logic into the above loop for better performance.
            for (ColumnletId chunkId : cacheChunks)
            {
                short rgId = chunkId.rowGroupId;
                short colId = chunkId.columnId;
                int rgIdx = rgId - RGStart;
                int bufferIdx = rgIdx * includedColumns.length + colId;
                if (chunkBuffers[bufferIdx] == null || chunkBuffers[bufferIdx].capacity() == 0)
                {
                    PixelsProto.RowGroupIndex rowGroupIndex =
                            rowGroupFooters[rgIdx].getRowGroupIndexEntry();
                    PixelsProto.ColumnChunkIndex chunkIndex =
                            rowGroupIndex.getColumnChunkIndexEntries(colId);
                    ChunkId diskChunk = new ChunkId(rgIdx, colId, chunkIndex.getChunkOffset(),
                            chunkIndex.getChunkLength());
                    diskChunks.add(diskChunk);
                }
                else
                {
                    this.cacheReadBytes += chunkBuffers[bufferIdx].capacity();
                }
            }
            */
//            logger.debug("[cache stat]: " + cacheChunks.size() + "," + cacheReadBytes + "," + cacheReadCost + "," + cacheReadBytes * 1.0 / cacheReadCost);
        }
        else
        {
            for (int rgIdx = 0; rgIdx < targetRGNum; rgIdx++)
            {
                PixelsProto.RowGroupIndex rowGroupIndex =
                        rowGroupFooters[rgIdx].getRowGroupIndexEntry();
                for (int colId : targetColumns)
                {
                    PixelsProto.ColumnChunkIndex chunkIndex =
                            rowGroupIndex.getColumnChunkIndexEntries(colId);
                    ChunkId chunk = new ChunkId(rgIdx, colId,
                            chunkIndex.getChunkOffset(),
                            chunkIndex.getChunkLength());
                    diskChunks.add(chunk);
                }
            }
        }

        if (diskChunks.isEmpty() == false)
        {
            /**
             * Comments added in Issue #67 (path):
             * By ordering disk chunks (ascending) according to the start offset and
             * group continuous disk chunks into one group, we can read the continuous
             * disk chunks in only one disk read, this would significantly improve I/O
             * performance. In presto.orc (io.prestosql.orc.AbstractOrcDataSource),
             * similar strategy is called disk range merging.
             *
             * diskChunks.sort and ChunkSeq are also optimized in Issue #67 (path).
             */

            // sort chunks by starting offset
            diskChunks.sort((o1, o2) -> (
                    o1.offset < o2.offset ? -1 :
                            (o1.offset > o2.offset ? 1 : 0)));

            // get chunk blocks. In best case, diskChunkSeqs.size() == targetRGNum.
            List<ChunkSeq> diskChunkSeqs = new ArrayList<>(this.targetRGNum);
            ChunkSeq diskChunkSeq = new ChunkSeq();
            for (ChunkId chunk : diskChunks)
            {
                if (!diskChunkSeq.addChunk(chunk))
                {
                    diskChunkSeqs.add(diskChunkSeq);
                    diskChunkSeq = new ChunkSeq();
                    diskChunkSeq.addChunk(chunk);
                }
            }
            diskChunkSeqs.add(diskChunkSeq);

            // read chunk blocks into buffers
            try
            {
                for (ChunkSeq seq : diskChunkSeqs)
                {
                    if (seq.getLength() == 0)
                    {
                        continue;
                    }
                    int offset = (int) seq.getOffset();
                    int length = (int) seq.getLength();
                    diskReadBytes += length;
                    ByteBuffer chunkBlockBuffer = ByteBuffer.allocate(length);
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
                    physicalFSReader.readFully(chunkBlockBuffer.array());
//                }
                    List<ChunkId> chunkIds = seq.getChunks();
                    int chunkSliceOffset = 0;
                    for (ChunkId chunkId : chunkIds)
                    {
                        int chunkLength = (int) chunkId.length;
                        /**
                         * Comments added in Issue #103:
                         * chunkId.rowGroupId does not mean the row group id in the Pixels file,
                         * it is the index of group that is to be read (some row groups in the file
                         * may be filtered out by the predicate and will not be read) and it is
                         * used to calculate the index of chunkBuffers.
                         */
                        int rgIdx = chunkId.rowGroupId;
                        int colId = chunkId.columnId;
                        chunkBlockBuffer.position(chunkSliceOffset);
                        chunkBlockBuffer.limit(chunkSliceOffset + chunkLength);
                        ByteBuffer chunkBuffer = chunkBlockBuffer.slice();
                        chunkBuffers[rgIdx * includedColumns.length + colId] = chunkBuffer;
                        chunkSliceOffset += chunkLength;
                    }
                }
            } catch (IOException e)
            {
                logger.error(e);
                throw new IOException("failed to read chunks block into buffers.", e);
                // return false;
            }
        }

        return true;
    }

    /**
     * Prepare for the next row batch.
     *
     * @param batchSize the willing batch size
     * @return the real batch size
     */
    @Override
    public int prepareBatch(int batchSize) throws IOException
    {
        if (!everPrepared)
        {
            if (prepareRead() == false)
            {
                throw new IOException("Failed to prepare for read.");
            }
        }
        // curBatchSize is the available size of the next batch.
        int curBatchSize = -curRowInRG;
        for (int rgIdx = curRGIdx; rgIdx < targetRGNum; ++rgIdx)
        {
            int rgRowCount = (int) footer.getRowGroupInfos(targetRGs[rgIdx]).getNumberOfRows();
            curBatchSize += rgRowCount;
            if (curBatchSize >= batchSize)
            {
                curBatchSize = batchSize;
                break;
            }
        }
        return curBatchSize;
    }

    /**
     * Read the next row batch.
     *
     * @param batchSize the row batch to read into
     * @return more rows available
     * @throws java.io.IOException
     */
    @Override
    public VectorizedRowBatch readBatch(int batchSize)
            throws IOException
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
            long start = System.nanoTime();
            if (!read())
            {
                resultRowBatch.size = 0; // added in Issue #103.
                resultRowBatch.endOfFile = true;
                return resultRowBatch;
            }
            readTimeNanos += System.nanoTime() - start;
        }

        // ensure size for result row batch
        resultRowBatch.ensureSize(batchSize);

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
                            .getColumnChunkIndexEntries(
                                    resultColumns[i]);
                    // TODO: read chunk buffer lazily when a column block is read by PixelsPageSource.
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
                // copyFrom() is actually a shallow copy
                // rename copyFrom() to duplicate(), so it is more readable
                cv.duplicate(columnVectors[cv.originVecId]);
            }
        }

        return resultRowBatch;
    }

    @Override
    public VectorizedRowBatch readBatch()
            throws IOException
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
        return diskReadBytes + cacheReadBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    /**
     * Cleanup and release resources
     */
    @Override
    public void close() throws IOException
    {
        diskReadBytes = 0L;
        cacheReadBytes = 0L;
        // release chunk buffer
        if (chunkBuffers != null)
        {
            for (int i = 0; i < chunkBuffers.length; i++)
            {
                chunkBuffers[i] = null;
            }
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
                } catch (IOException e)
                {
                    logger.error("Failed to close column reader.", e);
                    throw e;
                } finally
                {
                    readers[i] = null;
                }
            }
        }
        if (resultRowBatch != null)
        {
            resultRowBatch.close();
            resultRowBatch = null;
        }
        if (rowGroupFooters != null)
        {
            for (int i = 0; i < rowGroupFooters.length; ++i)
            {
                rowGroupFooters[i] = null;
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
