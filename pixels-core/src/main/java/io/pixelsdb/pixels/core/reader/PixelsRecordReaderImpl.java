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

import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.cache.ColumnletId;
import io.pixelsdb.pixels.cache.PixelsCacheReader;
import io.pixelsdb.pixels.common.metrics.ReadPerfMetrics;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.Scheduler;
import io.pixelsdb.pixels.common.physical.SchedulerFactory;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.predicate.PixelsPredicate;
import io.pixelsdb.pixels.core.stats.ColumnStats;
import io.pixelsdb.pixels.core.stats.StatsRecorder;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * @author guodong
 * @author hank
 */
public class PixelsRecordReaderImpl
        implements PixelsRecordReader
{
    private static final Logger logger = LogManager.getLogger(PixelsRecordReaderImpl.class);

    private final PhysicalReader physicalReader;
    private final PixelsProto.PostScript postScript;
    private final PixelsProto.Footer footer;
    private final PixelsReaderOption option;
    private final long queryId;
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
    private final List<PixelsProto.Type> includedColumnTypes;

    private TypeDescription fileSchema;
    private TypeDescription resultSchema;
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
     * Target row groups to read after matching reader option,
     * each element represents a row group id.
     */
    private int[] targetRGs;
    /**
     * Target columns to read after matching reader option,
     * each element represents a column id (column's index in the schema).
     * Different from resultColumns, the ith column id in targetColumn
     * corresponds to the ith true value in includedColumns.
     */
    private int[] targetColumns;
    /**
     * The ith element in resultColumns is the column id (column's index in the schema)
     * of ith included column in the read option. The order of columns in the read option's
     * includedCols may be arbitrary, not related to the column order in schema.
     */
    private int[] resultColumns;
    private int includedColumnNum = 0; // the number of columns to read.
    private int qualifiedRowNum = 0; // the number of qualified rows in this split.
    private boolean endOfFile = false;

    private int targetRGNum = 0;         // number of target row groups
    private int curRGIdx = 0;            // index of current reading row group in targetRGs
    private int curRowInRG = 0;          // starting index of values to read by reader in current row group

    private PixelsProto.RowGroupFooter[] rowGroupFooters;
    // buffers of each chunk in this file, arranged by chunk's row group id and column id
    private ByteBuffer[] chunkBuffers;
    private ColumnReader[] readers;      // column readers for each target columns

    private long diskReadBytes = 0L;
    private long cacheReadBytes = 0L;
    private long readTimeNanos = 0L;
    private long memoryUsage = 0L;

    public PixelsRecordReaderImpl(PhysicalReader physicalReader,
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
        this.physicalReader = physicalReader;
        this.postScript = postScript;
        this.footer = footer;
        this.option = option;
        this.queryId = option.getQueryId();
        this.RGStart = option.getRGStart();
        this.RGLen = option.getRGLen();
        this.enableMetrics = enableMetrics;
        this.metricsDir = metricsDir;
        this.readPerfMetrics = new ReadPerfMetrics();
        this.enableCache = enableCache;
        this.cacheOrder = cacheOrder;
        this.cacheReader = cacheReader;
        this.pixelsFooterCache = pixelsFooterCache;
        this.fileName = this.physicalReader.getName();
        this.includedColumnTypes = new ArrayList<>();
        // Issue #175: this check is currently not necessary.
        // requireNonNull(TransContext.Instance().getQueryTransInfo(this.queryId),
        //         "The transaction context does not contain query (trans) id '" + this.queryId + "'");
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
        if (RGLen == -1)
        {
            RGLen = footer.getRowGroupStatsList().size() - RGStart;
        }
        if (RGStart + RGLen > rgNum)
        {
            RGLen = rgNum - RGStart;
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
            //throw new IOException("ISSUE-103: includedColumnsNum is " + includedColumnsNum +
            //        " while optionIncludedCols.length is " + optionIncludedCols.length);
            return;
        }

        // create result columns storing result column ids in user specified order
        this.resultColumns = new int[includedColumnNum];
        for (int i = 0; i < optionColsIndices.size(); i++)
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
    private boolean prepareRead() throws IOException
    {
        if (!checkValid)
        {
            everPrepared = false;
            return false;
        }

        List<PixelsProto.RowGroupStatistic> rowGroupStatistics
                = footer.getRowGroupStatsList();
        boolean[] includedRGs = new boolean[RGLen];
        if (includedRGs.length == 0)
        {
            everPrepared = false;
            return false;
        }

        /**
         * Issue #105:
         * As we use int32 for NumberOfRows in postScript, we also use int here.
         */
        int includedRowNum = 0;
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
                    includedRowNum = postScript.getNumberOfRows();
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
                    throw new IOException(
                            "predicate does not match none or all while included columns is empty. predicate=" +
                            predicate.toString());
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
                     * 2. Whenever predicate does not match any column statistics, we should not return
                     * false. Instead, we must make sure that includedRGs are filled in by false values.
                     * By this way, the subsequent methods such as read() an readBatch() can skip all row
                     * groups of this record reader without additional overheads, as targetRGs would be
                     * empty (has no valid element) and targetRGNum would be 0.
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
                        if (includedRGs[i] == true)
                        {
                            includedRowNum += footer.getRowGroupInfos(i).getNumberOfRows();
                        }
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
            includedRowNum = postScript.getNumberOfRows();
        }

        /**
         * Issue #105:
         * project nothing, must be count(*).
         * includedColumnNum should only be set in checkBeforeRead().
         */
        if (includedColumnNum == 0)
        {
            qualifiedRowNum = includedRowNum;
            endOfFile = true;
            // init the following members as null or 0.
            targetRGs = null;
            targetRGNum = 0;
            rowGroupFooters = null;

            everPrepared = true;
            return true;
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
        rowGroupFooters = new PixelsProto.RowGroupFooter[targetRGNum];
        /**
         * Issue #114:
         * Use request batch and read scheduler to execute the read requests.
         *
         * Here, we create an empty batch as footer cache is very likely to be hit in
         * the subsequent queries on the same table.
         */
        Scheduler.RequestBatch requestBatch = new Scheduler.RequestBatch();
        List<CompletableFuture> actionFutures = new ArrayList<>();
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
                int fi = i;
                actionFutures.add(requestBatch.add(queryId, footerOffset, (int) footerLength).thenAccept(resp ->
                {
                    if (resp != null)
                    {
                        try
                        {
                            PixelsProto.RowGroupFooter parsed = PixelsProto.RowGroupFooter.parseFrom(resp);
                            rowGroupFooters[fi] = parsed;
                            pixelsFooterCache.putRGFooter(rgCacheId, parsed);
                        } catch (InvalidProtocolBufferException e)
                        {
                            throw new RuntimeException("Failed to parse row group footer from byte buffer.", e);
                        }
                    }
                }));
            }
            // cache hit
            else
            {
                rowGroupFooters[i] = rowGroupFooter;
            }
        }
        Scheduler scheduler = SchedulerFactory.Instance().getScheduler();
        try
        {
            scheduler.executeBatch(physicalReader, requestBatch, queryId);
            requestBatch.completeAll(actionFutures).join();
            requestBatch.clear();
            actionFutures.clear();
        } catch (Exception e)
        {
            throw new IOException("Failed to read row group footers, " +
                    "only the last error is thrown, check the logs for more information.", e);
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

        // read chunk offset and length of each target column chunks
        this.chunkBuffers = new ByteBuffer[targetRGNum * includedColumns.length];
        List<ChunkId> diskChunks = new ArrayList<>(targetRGNum * targetColumns.length);
        // read cached data which are in need
        if (enableCache)
        {
            long blockId;
            try
            {
                blockId = physicalReader.getBlockId();
            }
            catch (IOException e)
            {
                logger.error("failed to get block id.", e);
                throw new IOException("failed to get block id.", e);
                //return false;
            }
            List<ColumnletId> cacheChunks = new ArrayList<>(targetRGNum * targetColumns.length);
            // find cached chunks
            for (int colId : targetColumns)
            {
                // direct cache read is just for debug, so we just get this parameter here for simplicity.
                // boolean direct = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("cache.read.direct"));
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
                        ColumnletId chunkId = new ColumnletId((short) rgId, (short) colId, true/*direct*/);
                        cacheChunks.add(chunkId);
                    }
                    // if cache miss, add chunkId to be read from disks
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
                memoryUsage += columnletId.direct ? 0 : columnlet.capacity();
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
            // In Issue #67 (patch), we moved this logic into the above loop for better performance.
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

        if (!diskChunks.isEmpty())
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
             *
             * Issue #114:
             * Disk chunks ordering and request merging (was implemented in the removed
             * ChunkSeq) are moved into the sortmerge scheduler, which can be enabled
             * by setting read.request.scheduler=sortmerge.
             */
            Scheduler.RequestBatch requestBatch = new Scheduler.RequestBatch(diskChunks.size());
            List<CompletableFuture> actionFutures = new ArrayList<>(diskChunks.size());
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
                actionFutures.add(requestBatch.add(queryId, chunk.offset, (int)chunk.length)
                        .thenAccept(resp ->
                {
                    if (resp != null)
                    {
                        chunkBuffers[rgIdx * numCols + colId] = resp;
                    }
                }));
                // don't update statistics in whenComplete as it may be executed in other threads.
                diskReadBytes += chunk.length;
                memoryUsage += chunk.length;
            }

            Scheduler scheduler = SchedulerFactory.Instance().getScheduler();
            try
            {
                scheduler.executeBatch(physicalReader, requestBatch, queryId);
                requestBatch.completeAll(actionFutures).join();
                requestBatch.clear();
                actionFutures.clear();
            } catch (Exception e)
            {
                throw new IOException("Failed to read chunks block into buffers, " +
                        "only the last error is thrown, check the logs for more information.", e);
            }
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
     *
     * Unfortunately, we have no way to distinguish such type from the normal
     * queries by the predicates and projection columns from Presto.
     *
     * preRowInRG will keep in sync with curRowInRG if readBatch() is actually
     * called from LazyBlock.load().
     */
    private volatile int preRowInRG = 0;
    /**
     * Issue #105:
     * Similar to preRowInRG.
     */
    private volatile int preRGIdx = 0;

    /**
     * Prepare for the next row batch. This method is independent from readBatch().
     *
     * @param batchSize the willing batch size
     * @return the real batch size
     */
    @Override
    public int prepareBatch(int batchSize) throws IOException
    {
        if (endOfFile)
        {
            return 0;
        }

        if (!everPrepared)
        {
            if (prepareRead() == false)
            {
                throw new IOException("Failed to prepare for read.");
            }
        }

        /**
         * Issue #105:
         * project nothing, must be count(*).
         * qualifiedRowNum and endOfFile have been set in prepareRead();
         */
        if (includedColumnNum == 0)
        {
            if (endOfFile == false)
            {
                throw new IOException("EOF should be set in case of none projection columns");
            }
            return qualifiedRowNum;
        }

        // curBatchSize is the available size of the next batch.
        int curBatchSize = -preRowInRG;
        for (int rgIdx = preRGIdx; rgIdx < targetRGNum; ++rgIdx)
        {
            int rgRowCount = (int) footer.getRowGroupInfos(targetRGs[rgIdx]).getNumberOfRows();
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

    @Override
    public VectorizedRowBatch readBatch(int batchSize, boolean reuse)
            throws IOException
    {
        if (!checkValid || endOfFile)
        {
            TypeDescription resultSchema = TypeDescription.createSchema(new ArrayList<>());
            VectorizedRowBatch resultRowBatch = resultSchema.createRowBatch(0);
            resultRowBatch.projectionSize = 0;
            resultRowBatch.endOfFile = true;
            this.endOfFile = true;
            resultRowBatch.size = 0;
            return resultRowBatch;
        }

        if (!everRead)
        {
            long start = System.nanoTime();
            if (!read())
            {
                throw new IOException("failed to read file.");
            }
            readTimeNanos += System.nanoTime() - start;
        }

        // project nothing, must be count(*)
        if (includedColumnNum == 0)
        {
            /**
             * Issue #105:
             * EOF and batch size have been set in prepareRead() and checked in read().
             */
            if (endOfFile == false)
            {
                throw new IOException("EOF should be set in case of none projection columns");
            }
            checkValid = false; // Issue #105: to reject continuous read.
            TypeDescription resultSchema = TypeDescription.createSchema(new ArrayList<>());
            VectorizedRowBatch resultRowBatch = resultSchema.createRowBatch(0);
            resultRowBatch.projectionSize = 0;
            resultRowBatch.size = qualifiedRowNum;
            resultRowBatch.endOfFile = true;
            this.endOfFile = true;
            return resultRowBatch;
        }

        VectorizedRowBatch resultRowBatch;
        if (reuse)
        {
            if (this.resultRowBatch == null || this.resultRowBatch.projectionSize != includedColumnNum)
            {
                this.resultRowBatch = resultSchema.createRowBatch(batchSize);
                this.resultRowBatch.projectionSize = includedColumnNum;
            }
            this.resultRowBatch.reset();
            this.resultRowBatch.ensureSize(batchSize);
            resultRowBatch = this.resultRowBatch;
        } else
        {
            resultRowBatch = resultSchema.createRowBatch(batchSize);
            resultRowBatch.projectionSize = includedColumnNum;
        }

        int rgRowCount = 0;
        int curBatchSize = 0;
        ColumnVector[] columnVectors = resultRowBatch.cols;

        if (curRGIdx < targetRGNum)
        {
            rgRowCount = (int) footer.getRowGroupInfos(targetRGs[curRGIdx]).getNumberOfRows();
        }

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
                    PixelsProto.RowGroupFooter rowGroupFooter = rowGroupFooters[curRGIdx];
                    PixelsProto.ColumnEncoding encoding = rowGroupFooter.getRowGroupEncoding()
                            .getColumnChunkEncodings(resultColumns[i]);
                    int index = curRGIdx * includedColumns.length + resultColumns[i];
                    PixelsProto.ColumnChunkIndex chunkIndex = rowGroupFooter.getRowGroupIndexEntry()
                            .getColumnChunkIndexEntries(resultColumns[i]);
                    readers[i].read(chunkBuffers[index], encoding, curRowInRG, curBatchSize,
                            postScript.getPixelStride(), resultRowBatch.size, columnVectors[i], chunkIndex);
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
                //preRGIdx = curRGIdx; // keep in sync with curRGIdx
                // if not end of file, update row count
                if (curRGIdx < targetRGNum)
                {
                    rgRowCount = (int) footer.getRowGroupInfos(targetRGs[curRGIdx]).getNumberOfRows();
                }
                // if end of file, set result vectorized row batch endOfFile
                else
                {
                    checkValid = false; // Issue #105: to reject continuous read.
                    resultRowBatch.endOfFile = true;
                    this.endOfFile = true;
                    break;
                }
                //preRowInRG = curRowInRG = 0; // keep in sync with curRowInRG.
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

    /**
     * This method is valid after calling prepareBatch or readBatch.
     * Before that, it will always return false.
     * @return true if reach EOF.
     */
    @Override
    public boolean isEndOfFile ()
    {
        return endOfFile;
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
        return diskReadBytes + cacheReadBytes;
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
                    throw new IOException("Failed to close column reader.", e);
                } finally
                {
                    readers[i] = null;
                }
            }
        }
        if (rowGroupFooters != null)
        {
            for (int i = 0; i < rowGroupFooters.length; ++i)
            {
                rowGroupFooters[i] = null;
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
        public final long length;

        public ChunkId(int rowGroupId, int columnId, long offset, long length)
        {
            this.rowGroupId = rowGroupId;
            this.columnId = columnId;
            this.offset = offset;
            this.length = length;
        }
    }
}
