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
import io.pixelsdb.pixels.cache.ColumnChunkId;
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
import io.pixelsdb.pixels.core.stats.IntegerStatsRecorder;
import io.pixelsdb.pixels.core.stats.StatsRecorder;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
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
public class PixelsRecordReaderImpl implements PixelsRecordReader
{
    private static final Logger logger = LogManager.getLogger(PixelsRecordReaderImpl.class);

    private final PhysicalReader physicalReader;
    private final PixelsProto.PostScript postScript;
    private final PixelsProto.Footer footer;
    private final PixelsReaderOption option;
    private final long transId;
    private final long timestamp;
    private final int RGStart;
    private int RGLen;
    private final boolean enableMetrics;
    private final String metricsDir;
    private final ReadPerfMetrics readPerfMetrics;
    private final boolean enableCache;
    private final List<String> cacheOrder;
    private final PixelsCacheReader cacheReader;
    private final PixelsFooterCache pixelsFooterCache;
    private final String filePath;
    private final List<PixelsProto.Type> includedColumnTypes;

    private TypeDescription fileSchema = null;
    private TypeDescription resultSchema = null;
    private boolean checkValid = false;
    private boolean everPrepared = false;
    private boolean everRead = false;
    private long rowIndex = 0L;
    private VectorizedRowBatch resultRowBatch;
    /**
     * Columns included by reader option; if included, set true
     * If the timestamp is set, the hidden timestamp column should be included.
     */
    private boolean[] includedColumns;
    /**
     * Target row groups to read after matching reader option,
     * each element represents a row group id.
     */
    private int[] targetRGs;
    /**
     * The target columns to read after matching reader option.
     * If the timestamp is set, the hidden timestamp column should be included.
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
     * Note that the hidden timestamp column is not included in resultColumns.
     */
    private int[] resultColumns;
    /**
     * The ith element is true if the ith column in the resultSchema should use encoded column vectors.
     */
    private boolean[] resultColumnsEncoded;
    private int includedColumnNum = 0; // the number of columns to read, include the hidden timestamp column if set.
    /**
     * the number of qualified rows in this split.
     * If the timestamp is set, the final value is set after filtering by timestamp in {@link #readBatch(int, boolean)}.
     */
    private int qualifiedRowNum = 0;
    private boolean endOfFile = false;

    private int targetRGNum = 0;         // number of target row groups
    private int curRGIdx = 0;            // index of current reading row group in targetRGs
    private int curRowInRG = 0;          // starting index of values to read by reader in current row group

    private PixelsProto.RowGroupFooter[] rowGroupFooters;
    // buffers of each chunk in this file, arranged by chunk's row group id and column id
    private ByteBuffer[] chunkBuffers;
    private ColumnReader[] readers;      // column readers for each target columns
    private final boolean enableEncodedVector;

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
        this.transId = option.getTransId();
        this.timestamp = option.getTimestamp();
        this.RGStart = option.getRGStart();
        this.RGLen = option.getRGLen();
        this.enableEncodedVector = option.isEnableEncodedColumnVector();
        this.enableMetrics = enableMetrics;
        this.metricsDir = metricsDir;
        this.readPerfMetrics = new ReadPerfMetrics();
        this.enableCache = enableCache;
        this.cacheOrder = cacheOrder;
        this.cacheReader = cacheReader;
        this.pixelsFooterCache = pixelsFooterCache;
        this.filePath = this.physicalReader.getPath();
        this.includedColumnTypes = new ArrayList<>();
        // Issue #175: this check is currently not necessary.
        // requireNonNull(TransContextCache.Instance().getQueryTransInfo(this.transId),
        //         "The transaction context does not contain query (trans) id '" + this.transId + "'");
        checkBeforeRead();
    }

    private void checkBeforeRead() throws IOException
    {
        // get file schema
        List<PixelsProto.Type> fileColTypes = footer.getTypesList();
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

        // check RGStart and RGLen are within the range of actual number of row groups
        int rgNum = footer.getRowGroupInfosCount();

        if (rgNum == 0)
        {
            checkValid = true;
            endOfFile = true;
            return;
        }

        if (RGStart >= rgNum)
        {
            checkValid = false;
            throw new IOException("row group start (" + RGStart + ") is out of bound (" + rgNum + ").");
        }
        if (RGLen == -1 || RGStart + RGLen > rgNum)
        {
            RGLen = rgNum - RGStart;
        }

        // filter included columns
        includedColumnNum = 0;
        String[] optionIncludedCols = option.getIncludedCols();
        // if size of cols is 0, create an empty row batch
        if (this.timestamp == -1L && optionIncludedCols.length == 0)
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

        // if the timestamp is set, the hidden timestamp column should be included
        if (this.timestamp != -1L)
        {
            // timestamp is not visible to the user, so it must not be included in the option.includeCols
            optionColsIndices.add(fileColTypes.size() - 1);
            this.includedColumns[fileColTypes.size() - 1] = true;
            includedColumnNum++;
        }

        // check included columns
        if (includedColumnNum != optionIncludedCols.length + (this.timestamp != -1L ? 1 : 0)
                && !option.isTolerantSchemaEvolution())
        {
            checkValid = false;
            throw new IOException("includedColumnsNum is " + includedColumnNum +
                    " whereas optionIncludedCols.length is " + optionIncludedCols.length);
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

        // create column readers
        List<TypeDescription> columnSchemas = fileSchema.getChildren();
        readers = new ColumnReader[targetColumnNum];
        for (int i = 0; i < resultColumns.length; i++)
        {
            int index = resultColumns[i];
            readers[i] = ColumnReader.newColumnReader(columnSchemas.get(index));
        }
        if (this.timestamp != -1L)
        {
            // create reader for the hidden timestamp column
            readers[readers.length - 1] = ColumnReader.newColumnReader(columnSchemas.get(columnSchemas.size() - 1));
        }

        // create result vectorized row batch
        for (int resultColumn : resultColumns)
        {
            includedColumnTypes.add(fileColTypes.get(resultColumn));
        }

        resultSchema = TypeDescription.createSchema(includedColumnTypes);

        // add hidden timestamp column to the included column types
        if (this.timestamp != -1L)
        {
            includedColumnTypes.add(fileColTypes.get(fileColTypes.size() - 1));
        }

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
                    List<TypeDescription> columnSchemas = fileSchema.getChildren();
                    for (int i = 0; i < RGLen; i++)
                    {
                        if (this.timestamp != -1L)
                        {
                            // get the hidden timestamp column statistics
                            PixelsProto.RowGroupStatistic rowGroupStatistic = rowGroupStatistics.get(RGStart + i);
                            List<PixelsProto.ColumnStatistic> rgColumnStatistics =
                                    rowGroupStatistic.getColumnChunkStatsList();
                            IntegerStatsRecorder timestampStats = (IntegerStatsRecorder) StatsRecorder.create(columnSchemas.get(columnSchemas.size() - 1),
                                    rgColumnStatistics.get(rgColumnStatistics.size() - 1));
                            // check if the row group is qualified
                            if (timestampStats.getMinimum() > this.timestamp)
                            {
                                includedRGs[i] = false;
                                continue;
                            }
                        }
                        includedRGs[i] = true;
                        includedRowNum += footer.getRowGroupInfos(RGStart + i).getNumberOfRows();
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
                    throw new IOException(
                            "predicate does not match none or all while included columns is empty, predicate=" +
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
                     * 1. PixelsTupleDomainPredicate.matches() is fixed in this issue, but there could be
                     * other problems in it, as the related TupleDomain APIs in presto spi is mysterious.
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
                    // second, get row group statistics, if not matches, skip the row group
                    for (int i = 0; i < RGLen; i++)
                    {
                        PixelsProto.RowGroupStatistic rowGroupStatistic = rowGroupStatistics.get(RGStart + i);
                        List<PixelsProto.ColumnStatistic> rgColumnStatistics =
                                rowGroupStatistic.getColumnChunkStatsList();
                        if (this.timestamp != -1L)
                        {
                            // get the hidden timestamp column statistics
                            IntegerStatsRecorder timestampStats = (IntegerStatsRecorder) StatsRecorder.create(columnSchemas.get(columnSchemas.size() - 1),
                                    rgColumnStatistics.get(rgColumnStatistics.size() - 1));
                            // check if the row group is qualified
                            if (timestampStats.getMinimum() > this.timestamp)
                            {
                                includedRGs[i] = false;
                                continue;
                            }
                        }
                        // Issue #103: columnStatsMap should be cleared for each row group.
                        columnStatsMap.clear();
                        for (int id : targetColumns)
                        {
                            columnStatsMap.put(id,
                                    StatsRecorder.create(columnSchemas.get(id), rgColumnStatistics.get(id)));
                        }
                        includedRGs[i] = predicate.matches(footer.getRowGroupInfos(i).getNumberOfRows(), columnStatsMap);
                        if (includedRGs[i])
                        {
                            includedRowNum += footer.getRowGroupInfos(RGStart + i).getNumberOfRows();
                        }
                    }
                }
            }
        }
        else
        {
            List<TypeDescription> columnSchemas = fileSchema.getChildren();
            for (int i = 0; i < RGLen; i++)
            {
                if (this.timestamp != -1L)
                {
                    PixelsProto.RowGroupStatistic rowGroupStatistic = rowGroupStatistics.get(RGStart + i);
                    List<PixelsProto.ColumnStatistic> rgColumnStatistics =
                            rowGroupStatistic.getColumnChunkStatsList();
                    // get the hidden timestamp column statistics
                    IntegerStatsRecorder timestampStats = (IntegerStatsRecorder) StatsRecorder.create(columnSchemas.get(columnSchemas.size() - 1),
                            rgColumnStatistics.get(rgColumnStatistics.size() - 1));
                    // check if the row group is qualified
                    if (timestampStats.getMinimum() > this.timestamp)
                    {
                        includedRGs[i] = false;
                        continue;
                    }
                }
                includedRGs[i] = true;
                includedRowNum += footer.getRowGroupInfos(RGStart + i).getNumberOfRows();
            }
        }

        if (this.timestamp == -1L && includedColumnNum == 0)
        {
            /**
             * Issue #105:
             * project nothing, must be count(*).
             * includedColumnNum should only be set in checkBeforeRead().
             */
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
            String rgCacheId = filePath + "-" + rgId;
            PixelsProto.RowGroupFooter rowGroupFooter = pixelsFooterCache.getRGFooter(rgCacheId);
            // cache miss, read from disk and put it into cache
            if (rowGroupFooter == null)
            {
                PixelsProto.RowGroupInformation rowGroupInformation =
                        footer.getRowGroupInfos(rgId);
                long footerOffset = rowGroupInformation.getFooterOffset();
                long footerLength = rowGroupInformation.getFooterLength();
                int fi = i;
                actionFutures.add(requestBatch.add(transId, footerOffset, (int) footerLength).thenAccept(resp ->
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
            scheduler.executeBatch(physicalReader, requestBatch, transId);
            requestBatch.completeAll(actionFutures).join();
            requestBatch.clear();
            actionFutures.clear();
        } catch (Exception e)
        {
            throw new IOException("Failed to read row group footers, " +
                    "only the last error is thrown, check the logs for more information.", e);
        }

        this.resultColumnsEncoded = new boolean[resultColumns.length];
        PixelsProto.RowGroupEncoding firstRgEncoding = rowGroupFooters[0].getRowGroupEncoding();
        for (int i = 0; i < resultColumns.length; i++)
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
    private boolean read() throws IOException
    {
        if (!checkValid)
        {
            return false;
        }

        if (!everPrepared)
        {
            if (!prepareRead())
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
        if (this.timestamp == -1L && includedColumnNum == 0)
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
            List<ColumnChunkId> cacheChunks = new ArrayList<>(targetRGNum * targetColumns.length);
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
                    // TODO: not only the column chunks in cacheOrder are cached.
                    String cacheIdentifier = rgId + ":" + colId;
                    // if cached, read from cache files
                    if (cacheOrder.contains(cacheIdentifier))
                    {
                        ColumnChunkId chunkId = new ColumnChunkId((short) rgId, (short) colId, true/*direct*/);
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
//            long cacheReadStartNano = System.nanoTime();
            for (ColumnChunkId columnChunkId : cacheChunks)
            {
                short rgId = columnChunkId.rowGroupId;
                short colId = columnChunkId.columnId;
//                long getBegin = System.nanoTime();
                ByteBuffer columnChunk = cacheReader.get(blockId, rgId, colId, columnChunkId.direct);
                memoryUsage += columnChunkId.direct ? 0 : columnChunk.capacity();
//                long getEnd = System.nanoTime();
//                logger.debug("[cache get]: " + columnChunk.length + "," + (getEnd - getBegin));
                chunkBuffers[(rgId - RGStart) * includedColumns.length + colId] = columnChunk;
                if (columnChunk == null || columnChunk.capacity() == 0)
                {
                    /**
                     * Issue #67 (patch):
                     * Deal with null or empty cache chunk.
                     * If cache read failed (e.g. cache read timeout), column chunk will be null.
                     * In this condition, we have to read the column chunk from disk.
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
                    this.cacheReadBytes += columnChunk.capacity();
                }
            }
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
             * Comments added in Issue #67 (patch):
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
                actionFutures.add(requestBatch.add(transId, chunk.offset, chunk.length)
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
                scheduler.executeBatch(physicalReader, requestBatch, transId);
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
    @Override
    public int prepareBatch(int batchSize) throws IOException
    {
        if (endOfFile)
        {
            return 0;
        }

        if (!everPrepared)
        {
            if (!prepareRead())
            {
                throw new IOException("Failed to prepare for read.");
            }
        }

        if (this.timestamp == -1L && includedColumnNum == 0)
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

    @Override
    public VectorizedRowBatch readBatch(int batchSize, boolean reuse)
            throws IOException
    {
        if (!checkValid || endOfFile)
        {
            this.endOfFile = true;
            return createEmptyEOFRowBatch(0);
        }

        if (!everRead)
        {
            long start = System.nanoTime();
            if (!read())
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
        if (this.timestamp == -1L && includedColumnNum == 0)
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
            if (this.resultRowBatch == null || this.resultRowBatch.projectionSize != resultColumns.length)
            {
                this.resultRowBatch = resultSchema.createRowBatch(batchSize, resultColumnsEncoded);
                this.resultRowBatch.projectionSize = resultColumns.length;
            }
            this.resultRowBatch.reset();
            this.resultRowBatch.ensureSize(batchSize, false);
            resultRowBatch = this.resultRowBatch;
        } else
        {
            resultRowBatch = resultSchema.createRowBatch(batchSize, resultColumnsEncoded);
            resultRowBatch.projectionSize = resultColumns.length;
        }

        int rgRowCount = 0;
        int curBatchSize = 0;
        ColumnVector[] columnVectors = resultRowBatch.cols;

        if (curRGIdx < targetRGNum)
        {
            rgRowCount = (int) footer.getRowGroupInfos(targetRGs[curRGIdx]).getNumberOfRows();
        }

        if (this.timestamp != -1L)
        {
            while (resultRowBatch.size < batchSize && curRowInRG < rgRowCount)
            {
                // update current batch size
                curBatchSize = rgRowCount - curRowInRG;
                if (curBatchSize + resultRowBatch.size >= batchSize)
                {
                    curBatchSize = batchSize - resultRowBatch.size;
                }

                // read timestamp into timestamp vector
                LongColumnVector timestampVector = new LongColumnVector(curBatchSize);
                PixelsProto.RowGroupFooter rowGroupFooter = rowGroupFooters[curRGIdx];
                int timestampColId = includedColumns.length - 1;
                PixelsProto.ColumnEncoding timestampEncoding = rowGroupFooter.getRowGroupEncoding()
                        .getColumnChunkEncodings(timestampColId);
                int timestampIndex = curRGIdx * includedColumns.length + timestampColId;
                PixelsProto.ColumnChunkIndex timestampChunkIndex = rowGroupFooter.getRowGroupIndexEntry()
                        .getColumnChunkIndexEntries(timestampColId);
                readers[readers.length - 1].read(chunkBuffers[timestampIndex], timestampEncoding, curRowInRG, curBatchSize,
                        postScript.getPixelStride(), 0, timestampVector, timestampChunkIndex);

                /**
                 * construct the selected rows bitmap, size is curBatchSize
                 * the i-th bit presents the curRowInRG + i row in chunkBuffers is selected or not.
                 */
                BitSet selectedRows = new BitSet(curBatchSize);
                int addedRows = 0;
                for (int i = 0; i < curBatchSize; i++)
                {
                    if (timestampVector.vector[i] <= this.timestamp)
                    {
                        selectedRows.set(i);
                        addedRows++;
                    }
                }

                // read vectors with selected rows
                for (int i = 0; i < resultColumns.length; i++)
                {
                    if (!columnVectors[i].duplicated)
                    {
                        PixelsProto.ColumnEncoding encoding = rowGroupFooter.getRowGroupEncoding()
                                .getColumnChunkEncodings(resultColumns[i]);
                        int index = curRGIdx * includedColumns.length + resultColumns[i];
                        PixelsProto.ColumnChunkIndex chunkIndex = rowGroupFooter.getRowGroupIndexEntry()
                                .getColumnChunkIndexEntries(resultColumns[i]);
                        readers[i].readSelected(chunkBuffers[index], encoding, curRowInRG, curBatchSize,
                                postScript.getPixelStride(), resultRowBatch.size, columnVectors[i], chunkIndex, selectedRows);
                    }
                }

                // update current row index in the row group
                curRowInRG += curBatchSize;
                rowIndex += curBatchSize;
                resultRowBatch.size += addedRows;
                // update qualified row number
                this.qualifiedRowNum += addedRows;

                // update row group index if current row index exceeds max row count in the row group
                if (curRowInRG >= rgRowCount) {
                    curRGIdx++;
                    //preRGIdx = curRGIdx; // keep in sync with curRGIdx
                    // if not end of file, update row count
                    if (curRGIdx < targetRGNum) {
                        rgRowCount = (int) footer.getRowGroupInfos(targetRGs[curRGIdx]).getNumberOfRows();
                        // refresh resultColumnsEncoded for reading the column vectors in the next row group.
                        PixelsProto.RowGroupEncoding rgEncoding = rowGroupFooters[curRGIdx].getRowGroupEncoding();
                        for (int i = 0; i < includedColumnNum; i++) {
                            this.resultColumnsEncoded[i] = rgEncoding.getColumnChunkEncodings(targetColumns[i]).getKind() !=
                                    PixelsProto.ColumnEncoding.Kind.NONE && enableEncodedVector;
                        }
                    }
                    // if end of file, set result vectorized row batch endOfFile
                    else {
                        checkValid = false; // Issue #105: to reject continuous read.
                        resultRowBatch.endOfFile = true;
                        this.endOfFile = true;
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

            for (ColumnVector cv : columnVectors) {
                if (cv.duplicated) {
                    // copyFrom() is actually a shallow copy
                    // rename copyFrom() to duplicate(), so it is more readable
                    cv.duplicate(columnVectors[cv.originVecId]);
                }
            }
        }
        else
        {
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
                if (curRowInRG >= rgRowCount) {
                    curRGIdx++;
                    //preRGIdx = curRGIdx; // keep in sync with curRGIdx
                    // if not end of file, update row count
                    if (curRGIdx < targetRGNum) {
                        rgRowCount = (int) footer.getRowGroupInfos(targetRGs[curRGIdx]).getNumberOfRows();
                        // refresh resultColumnsEncoded for reading the column vectors in the next row group.
                        PixelsProto.RowGroupEncoding rgEncoding = rowGroupFooters[curRGIdx].getRowGroupEncoding();
                        for (int i = 0; i < includedColumnNum; i++) {
                            this.resultColumnsEncoded[i] = rgEncoding.getColumnChunkEncodings(targetColumns[i]).getKind() !=
                                    PixelsProto.ColumnEncoding.Kind.NONE && enableEncodedVector;
                        }
                    }
                    // if end of file, set result vectorized row batch endOfFile
                    else {
                        checkValid = false; // Issue #105: to reject continuous read.
                        resultRowBatch.endOfFile = true;
                        this.endOfFile = true;
                        break;
                    }
                    //preRowInRG = curRowInRG = 0; // keep in sync with curRowInRG.
                    curRowInRG = 0;
                }
                if (this.enableEncodedVector) {
                    /**
                     * Issue #374:
                     * Dictionary column vector can not contain data from multiple column chunks,
                     * hence we do not pad the row batch with rows from the next row group.
                     */
                    break;
                }
            }

            for (ColumnVector cv : columnVectors) {
                if (cv.duplicated) {
                    // copyFrom() is actually a shallow copy
                    // rename copyFrom() to duplicate(), so it is more readable
                    cv.duplicate(columnVectors[cv.originVecId]);
                }
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
        return diskReadBytes + cacheReadBytes;
    }

    @Override
    public int getNumReadRequests()
    {
        if (physicalReader == null)
        {
            return 0;
        }
        return physicalReader.getNumReadRequests();
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