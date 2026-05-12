/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.retina;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.index.IndexOption;
import io.pixelsdb.pixels.common.index.MainIndex;
import io.pixelsdb.pixels.common.index.MainIndexFactory;
import io.pixelsdb.pixels.common.index.RowIdRange;
import io.pixelsdb.pixels.common.index.SinglePointIndexFactory;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.KeyColumns;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.metadata.domain.Schema;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;

import io.pixelsdb.pixels.common.utils.NetUtils;
import io.pixelsdb.pixels.common.utils.PixelsFileNameUtils;
import io.pixelsdb.pixels.common.utils.RetinaUtils;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Storage GC: identifies high-deletion-ratio files and rewrites them
 * to reclaim physical storage while keeping active queries unaffected.
 *
 * <p>Checkpoint ownership: the GC checkpoint is <em>always</em> written unconditionally
 * by {@link RetinaResourceManager#runGC()} after Memory GC and <em>before</em> this
 * class is invoked. {@code StorageGarbageCollector} never writes checkpoints.
 */
public class StorageGarbageCollector
{
    private static final Logger logger = LogManager.getLogger(StorageGarbageCollector.class);

    private final RetinaResourceManager resourceManager;
    private final MetadataService metadataService;
    private final double gcThreshold;
    private final long targetFileSize;
    private final int maxFilesPerGroup;
    private final int maxFileGroupsPerRun;
    private final int rowGroupSize;
    private final EncodingLevel encodingLevel;
    private final long retireDelayMs;

    // -------------------------------------------------------------------------
    // Value types
    // -------------------------------------------------------------------------

    /**
     * Metadata about a single candidate file: its invalid-row ratio exceeds
     * {@link #gcThreshold} and it is eligible for GC rewrite.
     */
    static final class FileCandidate
    {
        final File file;
        final String filePath;
        final long fileId;
        final int rgCount;
        final long tableId;
        final int virtualNodeId;
        final double invalidRatio;
        /** Physical file size in bytes, used for greedy group splitting. 0 if unknown. */
        final long fileSizeBytes;

        FileCandidate(File file, String filePath, long fileId, int rgCount,
                      long tableId, int virtualNodeId, double invalidRatio,
                      long fileSizeBytes)
        {
            this.file = file;
            this.filePath = filePath;
            this.fileId = fileId;
            this.rgCount = rgCount;
            this.tableId = tableId;
            this.virtualNodeId = virtualNodeId;
            this.invalidRatio = invalidRatio;
            this.fileSizeBytes = fileSizeBytes;
        }
    }

    /**
     * A group of candidate files sharing the same {@code (tableId, virtualNodeId)}.
     * Files within the same group may be rewritten together while preserving row-ordering invariants.
     */
    static final class FileGroup
    {
        final long tableId;
        final int virtualNodeId;
        final List<FileCandidate> files;

        FileGroup(long tableId, int virtualNodeId, List<FileCandidate> files)
        {
            assert files.stream().allMatch(f -> f.virtualNodeId == virtualNodeId)
                    : "All files in a FileGroup must share the same virtualNodeId";
            this.tableId = tableId;
            this.virtualNodeId = virtualNodeId;
            this.files = files;
        }
    }

    /**
     * Reverse mapping for one old file: maps new-file coordinates back to old-file
     * global row offsets.  One {@code BackwardInfo} per old file in the group.
     */
    static final class BackwardInfo
    {
        final long oldFileId;
        /** newRgId → bwdMapping[newRgRowOffset] = oldGlobalRowOffset, or -1 if no mapping */
        final Map<Integer, int[]> backwardRgMappings;
        /** oldFileRgRowStart[rgId] = global row offset of first row in that RG; length = rgCount + 1 */
        final int[] oldFileRgRowStart;

        BackwardInfo(long oldFileId, Map<Integer, int[]> backwardRgMappings, int[] oldFileRgRowStart)
        {
            this.oldFileId = oldFileId;
            this.backwardRgMappings = backwardRgMappings;
            this.oldFileRgRowStart = oldFileRgRowStart;
        }
    }

    /**
     * Captures a kept row's primary key bytes and create_ts during rewrite,
     * for reconstructing the {@link IndexProto.IndexKey} during index synchronisation.
     */
    static final class PendingIndexEntry
    {
        final int newGlobalRowOffset;
        final ByteString pkBytes;
        final long createTs;

        PendingIndexEntry(int newGlobalRowOffset, ByteString pkBytes, long createTs)
        {
            this.newGlobalRowOffset = newGlobalRowOffset;
            this.pkBytes = pkBytes;
            this.createTs = createTs;
        }
    }

    /**
     * Carries everything produced by {@link #rewriteFileGroup}: file metadata,
     * per-RG row counts, forward row mappings, backward row mappings, and
     * pending index entries captured during rewrite.
     */
    static final class RewriteResult
    {
        final FileGroup group;
        final String newFilePath;
        final long newFileId;
        final int newFileRgCount;
        final int[] newFileRgActualRecordNums;
        /** Sentinel array: newFileRgRowStart[i] = global row offset of first row in RG i. */
        final int[] newFileRgRowStart;
        /** oldFileId → (oldRgId → fwdMapping[oldRgRowOffset] = newGlobalRowOffset, or -1 if deleted) */
        final Map<Long, Map<Integer, int[]>> forwardRgMappings;
        /** One {@link BackwardInfo} per old file; empty list when all rows were deleted. */
        final List<BackwardInfo> backwardInfos;
        /** PK + create_ts captured for each kept row; empty when primary index is absent. */
        final List<PendingIndexEntry> pendingIndexEntries;

        /** Set by {@link #syncIndex} after allocating new rowIds. */
        long newRowIdStart = -1;
        /**
         * Set by {@link #syncIndex} after updating SinglePointIndex; old rowIds that were replaced.
         * <br/>
         * <b>Alignment invariant:</b> {@code oldRowIds.size() == pendingIndexEntries.size()}; each
         * slot corresponds 1:1 to the same-position entry in {@link #pendingIndexEntries}. Slots
         * where {@link io.pixelsdb.pixels.common.index.SinglePointIndex#updatePrimaryEntry} returned
         * a negative value (i.e. no prior entry to replace) are stored as {@code -1L} placeholders,
         * so that rollback can pair each {@code PendingIndexEntry} with its own old rowId.
         */
        List<Long> oldRowIds;

        RewriteResult(FileGroup group, String newFilePath, long newFileId,
                      int newFileRgCount, int[] newFileRgActualRecordNums, int[] newFileRgRowStart,
                      Map<Long, Map<Integer, int[]>> forwardRgMappings,
                      List<BackwardInfo> backwardInfos,
                      List<PendingIndexEntry> pendingIndexEntries)
        {
            this.group = group;
            this.newFilePath = newFilePath;
            this.newFileId = newFileId;
            this.newFileRgCount = newFileRgCount;
            this.newFileRgActualRecordNums = newFileRgActualRecordNums;
            this.newFileRgRowStart = newFileRgRowStart;
            this.forwardRgMappings = forwardRgMappings;
            this.backwardInfos = backwardInfos;
            this.pendingIndexEntries = pendingIndexEntries;
        }
    }

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    StorageGarbageCollector(RetinaResourceManager resourceManager,
                            MetadataService metadataService,
                            double gcThreshold,
                            long targetFileSize,
                            int maxFilesPerGroup,
                            int maxFileGroupsPerRun,
                            int rowGroupSize,
                            EncodingLevel encodingLevel,
                            long retireDelayMs)
    {
        this.resourceManager = resourceManager;
        this.metadataService = metadataService;
        this.gcThreshold = gcThreshold;
        this.targetFileSize = targetFileSize;
        this.maxFilesPerGroup = maxFilesPerGroup;
        this.maxFileGroupsPerRun = maxFileGroupsPerRun;
        this.rowGroupSize = rowGroupSize;
        this.encodingLevel = encodingLevel;
        this.retireDelayMs = retireDelayMs;
    }

    // -------------------------------------------------------------------------
    // Public entry point
    // -------------------------------------------------------------------------

    /**
     * Runs one Storage GC cycle: identify candidates, trim non-candidate bitmaps,
     * then scan metadata and process candidate file groups.
     *
     * <p>The GC checkpoint has already been written unconditionally by
     * {@link RetinaResourceManager#runGC()} before this method is called.
     *
     * @param safeGcTs          safe GC timestamp produced by Memory GC
     * @param fileStats         file-level visibility statistics pre-computed during Memory GC;
     *                          key = fileId, value = {@code long[]{totalRows, totalInvalidCount}}.
     *                          Replaces the old per-RG {@code rgStats} map, eliminating the
     *                          per-RG aggregation loop in candidate selection.
     * @param gcSnapshotBitmaps per-RG snapshot bitmaps (mutated in-place: non-candidate
     *                          entries removed to reduce memory pressure)
     */
    void runStorageGC(long safeGcTs, Map<Long, long[]> fileStats,
                      Map<String, long[]> gcSnapshotBitmaps)
    {
        // Pre-compute candidate file IDs from file-level stats (O(1) per file).
        Set<Long> candidateFileIds = new HashSet<>();
        for (Map.Entry<Long, long[]> entry : fileStats.entrySet())
        {
            long[] stats = entry.getValue();
            if (stats[0] > 0 && (double) stats[1] / stats[0] > gcThreshold)
            {
                candidateFileIds.add(entry.getKey());
            }
        }
        if (candidateFileIds.isEmpty())
        {
            return;
        }

        // Trim non-candidate bitmap entries immediately.  The checkpoint has already been
        // written with the full snapshot, so only candidate bitmaps are needed for rewriting.
        gcSnapshotBitmaps.entrySet().removeIf(e ->
                !candidateFileIds.contains(RetinaUtils.parseFileIdFromRgKey(e.getKey())));

        List<FileGroup> fileGroups = scanAndGroupFiles(candidateFileIds, fileStats);
        if (!fileGroups.isEmpty())
        {
            processFileGroups(fileGroups, safeGcTs, gcSnapshotBitmaps);
        }
    }

    /**
     * Scans all schemas/tables and returns at most {@link #maxFileGroupsPerRun} groups of
     * candidate files, sorted by average {@code invalidRatio} descending.
     *
     * <p>Only files whose ID appears in {@code candidateFileIds} are considered; all others
     * are skipped immediately.  File-level stats (totalRows, invalidCount) are read from
     * {@code fileStats} in O(1) — the old per-RG aggregation loop is eliminated.
     *
     * @param candidateFileIds file IDs that exceed the {@link #gcThreshold}, pre-computed
     *                         in {@link #runStorageGC}
     * @param fileStats        file-level visibility statistics; key = fileId,
     *                         value = {@code long[]{totalRows, totalInvalidCount}}
     */
    List<FileGroup> scanAndGroupFiles(Set<Long> candidateFileIds,
                                      Map<Long, long[]> fileStats)
    {
        List<FileCandidate> candidates = new ArrayList<>();

        List<Schema> schemas;
        try
        {
            schemas = metadataService.getSchemas();
        }
        catch (MetadataException e)
        {
            logger.error("Storage GC: failed to retrieve schemas", e);
            return Collections.emptyList();
        }

        for (Schema schema : schemas)
        {
            List<Table> tables;
            try
            {
                tables = metadataService.getTables(schema.getName());
            }
            catch (MetadataException e)
            {
                logger.warn("Storage GC: failed to get tables for schema '{}', skipping",
                        schema.getName(), e);
                continue;
            }

            for (Table table : tables)
            {
                Layout layout;
                try
                {
                    layout = metadataService.getLatestLayout(schema.getName(), table.getName());
                }
                catch (MetadataException e)
                {
                    logger.warn("Storage GC: failed to get layout for {}.{}, skipping",
                            schema.getName(), table.getName(), e);
                    continue;
                }
                if (layout == null)
                {
                    continue;
                }

                List<Path> paths = new ArrayList<>();
                paths.addAll(layout.getOrderedPaths());
                paths.addAll(layout.getCompactPaths());

                for (Path path : paths)
                {
                    List<File> files;
                    try
                    {
                        files = metadataService.getFiles(path.getId());
                    }
                    catch (MetadataException e)
                    {
                        logger.warn("Storage GC: failed to get files for pathId={}, skipping",
                                path.getId(), e);
                        continue;
                    }

                    Storage pathStorage = null;
                    for (File file : files)
                    {
                        if (!candidateFileIds.contains(file.getId()))
                        {
                            continue;
                        }

                        String filePath = File.getFilePath(path, file);

                        if (!PixelsFileNameUtils.isGcEligible(filePath))
                        {
                            continue;
                        }

                        long[] stats = fileStats.get(file.getId());
                        if (stats == null || stats[0] == 0)
                        {
                            continue;
                        }
                        double invalidRatio = (double) stats[1] / stats[0];

                        long sizeBytes;
                        try
                        {
                            if (pathStorage == null)
                            {
                                pathStorage = StorageFactory.Instance().getStorage(filePath);
                            }
                            sizeBytes = pathStorage.getStatus(filePath).getLength();
                        }
                        catch (IOException ex)
                        {
                            logger.error("Storage GC: cannot stat file {}, skipping candidate",
                                    filePath, ex);
                            continue;
                        }

                        int vNodeId = PixelsFileNameUtils.extractVirtualNodeId(filePath);
                        candidates.add(new FileCandidate(
                                file, filePath, file.getId(), file.getNumRowGroup(),
                                table.getId(), vNodeId, invalidRatio, sizeBytes));
                    }
                }
            }
        }

        return groupAndMerge(candidates);
    }

    /**
     * Groups candidates by {@code (tableId, virtualNodeId)}, sorts each group by
     * {@code invalidRatio} descending, then greedily splits each group into sub-groups
     * whose estimated effective data size does not exceed {@link #targetFileSize}.
     *
     * <p>Effective data size per file is estimated as
     * {@code fileSizeBytes * (1 - invalidRatio)}.  When {@code fileSizeBytes} is
     * unknown (0), the file is treated as fitting within any remaining budget —
     * i.e. splitting degrades to the old "all-in-one-group" behaviour.
     *
     * <p>If a single file's effective data already exceeds {@code targetFileSize},
     * it forms its own {@link FileGroup}.
     *
     * <p>The returned list is sorted by average {@code invalidRatio} descending and
     * capped at {@link #maxFileGroupsPerRun}.
     */
    List<FileGroup> groupAndMerge(List<FileCandidate> candidates)
    {
        Map<Long, Map<Integer, List<FileCandidate>>> grouped = new LinkedHashMap<>();
        for (FileCandidate c : candidates)
        {
            grouped.computeIfAbsent(c.tableId, k -> new LinkedHashMap<>())
                   .computeIfAbsent(c.virtualNodeId, k -> new ArrayList<>())
                   .add(c);
        }

        List<FileGroup> groups = new ArrayList<>();
        for (Map.Entry<Long, Map<Integer, List<FileCandidate>>> tableEntry : grouped.entrySet())
        {
            long tableId = tableEntry.getKey();
            for (Map.Entry<Integer, List<FileCandidate>> vnodeEntry : tableEntry.getValue().entrySet())
            {
                int vNodeId = vnodeEntry.getKey();
                List<FileCandidate> files = vnodeEntry.getValue();
                files.sort(Comparator.comparingDouble((FileCandidate c) -> c.invalidRatio).reversed());
                splitIntoGroups(groups, tableId, vNodeId, files);
            }
        }

        groups.sort(Comparator.comparingDouble(
                (FileGroup g) -> g.files.stream().mapToDouble(c -> c.invalidRatio).average().orElse(0.0))
                .reversed());

        if (groups.size() > maxFileGroupsPerRun)
        {
            return groups.subList(0, maxFileGroupsPerRun);
        }
        return groups;
    }

    /**
     * Greedily packs {@code files} (already sorted by invalidRatio desc) into
     * sub-groups bounded by both {@link #targetFileSize} (effective output bytes)
     * and {@link #maxFilesPerGroup} (old file count).  Whichever limit is reached
     * first triggers a group flush.
     */
    private void splitIntoGroups(List<FileGroup> out, long tableId, int vNodeId,
                                 List<FileCandidate> files)
    {
        if (targetFileSize <= 0 && maxFilesPerGroup <= 0)
        {
            out.add(new FileGroup(tableId, vNodeId, files));
            return;
        }

        List<FileCandidate> current = new ArrayList<>();
        long currentEffectiveBytes = 0;

        for (FileCandidate fc : files)
        {
            long effectiveBytes = fc.fileSizeBytes > 0
                    ? (long) (fc.fileSizeBytes * (1.0 - fc.invalidRatio)) : 0L;

            boolean singleFileOversized = targetFileSize > 0 && effectiveBytes > targetFileSize;
            if (singleFileOversized)
            {
                if (!current.isEmpty())
                {
                    out.add(new FileGroup(tableId, vNodeId, current));
                    current = new ArrayList<>();
                    currentEffectiveBytes = 0;
                }
                out.add(new FileGroup(tableId, vNodeId, Collections.singletonList(fc)));
                continue;
            }

            boolean sizeWouldExceed = targetFileSize > 0
                    && currentEffectiveBytes + effectiveBytes > targetFileSize;
            boolean fileCountFull = maxFilesPerGroup > 0
                    && current.size() >= maxFilesPerGroup;

            if ((sizeWouldExceed || fileCountFull) && !current.isEmpty())
            {
                out.add(new FileGroup(tableId, vNodeId, current));
                current = new ArrayList<>();
                currentEffectiveBytes = 0;
            }
            current.add(fc);
            currentEffectiveBytes += effectiveBytes;
        }
        if (!current.isEmpty())
        {
            out.add(new FileGroup(tableId, vNodeId, current));
        }
    }



    /**
     * Computes the cumulative row-start offsets for an old file's RGs.
     * {@code starts[rgId]} = global row offset of the first row in RG {@code rgId};
     * {@code starts[rgCount]} = total row count (sentinel).
     */
    private static int[] computeOldFileRgRowStart(Map<Integer, int[]> rgMappings, int rgCount)
    {
        int[] starts = new int[rgCount + 1];
        int accum = 0;
        for (int rgId = 0; rgId < rgCount; rgId++)
        {
            starts[rgId] = accum;
            int[] mapping = rgMappings.get(rgId);
            accum += (mapping != null) ? mapping.length : 0;
        }
        starts[rgCount] = accum;
        return starts;
    }

    /**
     * Registers dual-write for the given rewrite result so that subsequent
     * {@link RetinaResourceManager#deleteRecord} calls propagate deletes
     * between old and new files.
     */
    void registerDualWrite(RewriteResult result)
    {
        resourceManager.registerDualWrite(result);
    }

    /**
     * Removes dual-write for the given rewrite result.
     */
    void unregisterDualWrite(RewriteResult result)
    {
        resourceManager.unregisterDualWrite(result);
    }

    /**
     * Processes the candidate file groups produced by {@link #scanAndGroupFiles}.
     * Non-candidate bitmap entries have already been trimmed in {@link #runStorageGC}.
     *
     * @param fileGroups        non-empty list of candidate groups
     * @param safeGcTs          safe GC timestamp produced by Memory GC
     * @param gcSnapshotBitmaps per-RG snapshot bitmaps (already trimmed to candidates)
     */
    void processFileGroups(List<FileGroup> fileGroups, long safeGcTs,
                           Map<String, long[]> gcSnapshotBitmaps)
    {
        for (FileGroup group : fileGroups)
        {
            processFileGroup(group, safeGcTs, gcSnapshotBitmaps);
        }
    }

    
    /**
     * Rewrites all files in one {@link FileGroup} into a single new file, filtering out
     * rows marked as deleted in {@code gcSnapshotBitmaps}.
     *
     * <p>The new file is registered as {@code TEMPORARY} in the catalog and its
     * {@link RGVisibility} objects are initialised with {@code baseTimestamp = safeGcTs}.
     *
     * <p>After rewriting completes the {@code gcSnapshotBitmaps} entries for this group
     * are removed (they are no longer needed by subsequent steps).
     *
     * @param group             candidate file group produced by {@link #scanAndGroupFiles}
     * @param safeGcTs          safe GC timestamp; used as the base timestamp for new-file Visibility
     * @param gcSnapshotBitmaps per-RG deletion bitmaps; entries for this group are removed on exit
     * @return rewrite result carrying file metadata and row mappings
     */
    RewriteResult rewriteFileGroup(FileGroup group, long safeGcTs,
                                   Map<String, long[]> gcSnapshotBitmaps) throws Exception
    {
        String firstFilePath = group.files.get(0).filePath;
        Storage storage = StorageFactory.Instance().getStorage(firstFilePath);
        String dirUri = firstFilePath.substring(0, firstFilePath.lastIndexOf("/"));
        String newFileName = PixelsFileNameUtils.buildOrderedFileName(
                NetUtils.getLocalHostName(), group.virtualNodeId);
        String newFilePath = dirUri + "/" + newFileName;

        // Open the first old file once to read schema + writer parameters.
        // hasHiddenColumn is read here and propagated to the new-file writer so that
        // the hidden create_ts column is preserved in the rewritten file.  Without it,
        // queries reading the new file would lose the ability to filter by create_ts,
        // making rows with create_ts > safeGcTs incorrectly visible to snapshots between
        // safeGcTs and their actual create_ts.
        // One footer cache per rewrite call; shared across all readers for this file group.
        PixelsFooterCache footerCache = new PixelsFooterCache();

        TypeDescription schema;
        int pixelStride;
        int compressionBlockSize;
        boolean hasHiddenColumn;
        try (PixelsReader firstReader = PixelsReaderImpl.newBuilder()
                .setStorage(storage).setPath(firstFilePath)
                .setPixelsFooterCache(footerCache).build())
        {
            schema = firstReader.getFileSchema();
            pixelStride = (int) firstReader.getPixelStride();
            compressionBlockSize = (int) firstReader.getCompressionBlockSize();
            hasHiddenColumn = firstReader.getPostScript().getHasHiddenColumn();
        }

        int globalNewRowOffset = 0;
        Map<Long, Map<Integer, int[]>> forwardRgMappings = new HashMap<>();
        int nUserCols = schema.getChildren().size();
        String[] includeColNames = schema.getFieldNames().toArray(new String[0]);
        List<PendingIndexEntry> pendingIndexEntries = new ArrayList<>();

        // Resolve PK columns for index key capture; null if no primary index exists.
        int[] pkColIndices = null;
        List<TypeDescription> pkColTypes = null;
        try
        {
            io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex primaryIndex =
                    metadataService.getPrimaryIndex(group.tableId);
            if (primaryIndex != null)
            {
                KeyColumns keyColumns = primaryIndex.getKeyColumns();
                List<Integer> colIds = keyColumns.getKeyColumnIds();
                pkColIndices = new int[colIds.size()];
                pkColTypes = new ArrayList<>(colIds.size());
                List<TypeDescription> children = schema.getChildren();
                for (int i = 0; i < colIds.size(); i++)
                {
                    int colId = colIds.get(i);
                    pkColIndices[i] = colId;
                    pkColTypes.add(children.get(colId));
                }
            }
        }
        catch (MetadataException e)
        {
            logger.warn("StorageGC: failed to resolve primary index for tableId={}, index sync will be skipped",
                    group.tableId, e);
        }

        try (PixelsWriter writer = PixelsWriterImpl.newBuilder()
                .setSchema(schema).setPixelStride(pixelStride)
                .setRowGroupSize(rowGroupSize).setStorage(storage)
                .setPath(newFilePath).setOverwrite(false)
                .setEncodingLevel(encodingLevel)
                .setCompressionBlockSize(compressionBlockSize)
                .setHasHiddenColumn(hasHiddenColumn)
                .build())
        {
            int batchCapacity = VectorizedRowBatch.DEFAULT_SIZE;
            int[] selected = new int[batchCapacity];
            // filteredBatch extends cols[] with one extra LongColumnVector for create_ts
            // when hasHiddenColumn=true.  Per-column addSelected is used because the
            // source batch's cols[] does not include the hidden column slot.
            VectorizedRowBatch filteredBatch = schema.createRowBatch(batchCapacity);
            if (hasHiddenColumn)
            {
                ColumnVector[] ext = Arrays.copyOf(filteredBatch.cols, nUserCols + 1);
                ext[nUserCols] = new LongColumnVector(batchCapacity);
                filteredBatch.cols = ext;
            }
            PixelsReaderOption opt = new PixelsReaderOption();
            opt.includeCols(includeColNames);
            opt.exposeHiddenColumn(hasHiddenColumn);

            for (FileCandidate fc : group.files)
            {
                try (PixelsReader reader = PixelsReaderImpl.newBuilder()
                        .setStorage(storage).setPath(fc.filePath)
                        .setPixelsFooterCache(footerCache).build())
                {
                    for (int oldRgId = 0; oldRgId < reader.getRowGroupNum(); oldRgId++)
                    {
                        long[] gcBitmap = gcSnapshotBitmaps.get(RetinaUtils.buildRgKey(fc.fileId, oldRgId));
                        int rgRecordNum = reader.getRowGroupInfo(oldRgId).getNumberOfRows();

                        // transTimestamp is not set: GC filtering uses gcSnapshotBitmap
                        // exclusively.  Setting it would activate the hidden-timestamp
                        // filter and wrongly exclude alive rows with create_ts > safeGcTs.
                        opt.rgRange(oldRgId, 1);

                        int oldRgRowOffset = 0;
                        int[] fwdMapping = new int[rgRecordNum];

                        try (PixelsRecordReader recordReader = reader.read(opt))
                        {
                            VectorizedRowBatch batch;
                            while ((batch = recordReader.readBatch()) != null && batch.size > 0)
                            {
                                // GC row filter: a row is excluded iff its bit is set in
                                // gcSnapshotBitmap, meaning delete_ts <= safeGcTs.  Rows with
                                // create_ts > safeGcTs are kept as long as their bit is 0
                                // (not yet deleted or deleted after safeGcTs).
                                int kept = 0;
                                for (int r = 0; r < batch.size; r++, oldRgRowOffset++)
                                {
                                    // Check if the row's bit is set in the deletion bitmap (each long holds 64 bits)
                                    if (gcBitmap != null && (gcBitmap[oldRgRowOffset >>> 6] & (1L << (oldRgRowOffset & 63))) != 0)
                                    {
                                        fwdMapping[oldRgRowOffset] = -1;
                                    }
                                    else
                                    {
                                        selected[kept++] = r;
                                        fwdMapping[oldRgRowOffset] = globalNewRowOffset;
                                        if (pkColIndices != null)
                                        {
                                            ByteString pkBytes = extractPkBytes(batch, r, pkColIndices, pkColTypes);
                                            long createTs = hasHiddenColumn
                                                    ? ((LongColumnVector) batch.getHiddenColumnVector()).vector[r]
                                                    : 0L;
                                            pendingIndexEntries.add(
                                                    new PendingIndexEntry(globalNewRowOffset, pkBytes, createTs));
                                        }
                                        globalNewRowOffset++;
                                    }
                                }
                                if (kept > 0)
                                {
                                    for (int i = 0; i < nUserCols; i++)
                                    {
                                        filteredBatch.cols[i].addSelected(selected, 0, kept, batch.cols[i]);
                                    }
                                    if (hasHiddenColumn)
                                    {
                                        ((LongColumnVector) filteredBatch.cols[nUserCols])
                                                .addSelected(selected, 0, kept, batch.getHiddenColumnVector());
                                    }
                                    filteredBatch.size = kept;
                                    writer.addRowBatch(filteredBatch);
                                    filteredBatch.reset();
                                }
                            }
                        }
                        forwardRgMappings.computeIfAbsent(fc.fileId, k -> new HashMap<>()).put(oldRgId, fwdMapping);
                    }
                }
            }
        } // writer.close()

        // Release the gcSnapshotBitmaps for this group; rewriting is done.
        for (FileCandidate fc : group.files)
        {
            for (int rgId = 0; rgId < fc.rgCount; rgId++)
            {
                gcSnapshotBitmaps.remove(RetinaUtils.buildRgKey(fc.fileId, rgId));
            }
        }

        // Edge case: all rows in the group were deleted — skip catalog registration,
        // delete the empty file, and return early.  The old files will be cleaned up
        // by the delayed-cleanup phase once it is implemented.
        if (globalNewRowOffset == 0)
        {
            logger.info("StorageGC: all rows deleted for table={}, vNodeId={}, skipping empty file",
                    group.tableId, group.virtualNodeId);
            try
            {
                storage.delete(newFilePath, false);
            }
            catch (IOException e)
            {
                logger.warn("StorageGC: failed to delete empty rewrite file {}", newFilePath, e);
            }
            return new RewriteResult(group, newFilePath, -1,
                    0, new int[0], new int[]{0}, forwardRgMappings, Collections.emptyList(),
                    Collections.emptyList());
        }

        // Read the new file's Footer to get per-RG row counts.
        int newFileRgCount;
        int[] newFileRgActualRecordNums;
        int[] newFileRgRowStart;
        try (PixelsReader newReader = PixelsReaderImpl.newBuilder()
                .setStorage(storage).setPath(newFilePath)
                .setPixelsFooterCache(footerCache).build())
        {
            newFileRgCount = newReader.getRowGroupNum();
            newFileRgActualRecordNums = new int[newFileRgCount];
            newFileRgRowStart = new int[newFileRgCount + 1];
            int accum = 0;
            for (int rgId = 0; rgId < newFileRgCount; rgId++)
            {
                newFileRgActualRecordNums[rgId] = newReader.getRowGroupInfo(rgId).getNumberOfRows();
                newFileRgRowStart[rgId] = accum;
                accum += newFileRgActualRecordNums[rgId];
            }
            newFileRgRowStart[newFileRgCount] = accum;
        }

        // Build backward mappings by inverting the forward mappings.
        // newGlobal values are globally monotonic (globalNewRowOffset is a single counter),
        // so we advance a linear cursor instead of binary-searching per row.
        List<BackwardInfo> backwardInfos = new ArrayList<>();
        int curNewRgId = 0;
        for (FileCandidate fc : group.files)
        {
            Map<Integer, int[]> rgMappings = forwardRgMappings.get(fc.fileId);
            int[] oldFileRgRowStart = computeOldFileRgRowStart(rgMappings, fc.rgCount);
            Map<Integer, int[]> bwdMappings = new HashMap<>();
            for (int oldRgId = 0; oldRgId < fc.rgCount; oldRgId++)
            {
                int[] fwdMapping = rgMappings.get(oldRgId);
                if (fwdMapping == null)
                {
                    continue;
                }
                for (int oldOff = 0; oldOff < fwdMapping.length; oldOff++)
                {
                    int newGlobal = fwdMapping[oldOff];
                    if (newGlobal < 0)
                    {
                        continue;
                    }
                    while (curNewRgId + 1 < newFileRgCount
                            && newGlobal >= newFileRgRowStart[curNewRgId + 1])
                    {
                        curNewRgId++;
                    }
                    int newRgOff = newGlobal - newFileRgRowStart[curNewRgId];
                    int oldGlobal = oldFileRgRowStart[oldRgId] + oldOff;
                    bwdMappings.computeIfAbsent(curNewRgId, k ->
                    {
                        int[] arr = new int[newFileRgActualRecordNums[k]];
                        Arrays.fill(arr, -1);
                        return arr;
                    })[newRgOff] = oldGlobal;
                }
            }
            backwardInfos.add(new BackwardInfo(fc.fileId, bwdMappings, oldFileRgRowStart));
        }

        // Register the new file as TEMPORARY in the catalog and initialise Visibility.
        // Track registration progress so that partial state can be cleaned up on failure.
        long newFileId = -1;
        int registeredRgCount = 0;
        try
        {
            long minRowId = Long.MAX_VALUE, maxRowId = Long.MIN_VALUE;
            for (FileCandidate fc : group.files)
            {
                minRowId = Math.min(minRowId, fc.file.getMinRowId());
                maxRowId = Math.max(maxRowId, fc.file.getMaxRowId());
            }
            File newFile = new File();
            newFile.setName(newFileName);
            newFile.setType(File.Type.TEMPORARY);
            newFile.setNumRowGroup(newFileRgCount);
            newFile.setMinRowId(minRowId);
            newFile.setMaxRowId(maxRowId);
            newFile.setPathId(group.files.get(0).file.getPathId());
            if (!metadataService.addFiles(Collections.singletonList(newFile)))
            {
                throw new MetadataException("failed to add metadata for GC rewrite file " + newFilePath);
            }
            newFileId = metadataService.getFileId(newFilePath);

            for (int rgId = 0; rgId < newFileRgCount; rgId++)
            {
                resourceManager.addVisibility(newFileId, rgId, newFileRgActualRecordNums[rgId], safeGcTs, null, false);
                registeredRgCount = rgId + 1;
            }
        }
        catch (Exception e)
        {
            cleanupTemporaryFile(storage, newFilePath, newFileId, registeredRgCount);
            throw e;
        }

        return new RewriteResult(group, newFilePath, newFileId,
                newFileRgCount, newFileRgActualRecordNums, newFileRgRowStart,
                forwardRgMappings, backwardInfos, pendingIndexEntries);
    }

    /**
     * Best-effort cleanup of a partially-created TEMPORARY file.  Removes the
     * catalog record, the physical file, and any RGVisibility keys that were
     * registered before the failure.
     */
    private void cleanupTemporaryFile(Storage storage, String newFilePath,
                                      long newFileId, int registeredRgCount)
    {
        if (newFileId > 0)
        {
            for (int rgId = 0; rgId < registeredRgCount; rgId++)
            {
                try
                {
                    resourceManager.reclaimVisibility(newFileId, rgId, 0);
                }
                catch (Exception ex)
                {
                    logger.warn("StorageGC cleanup: failed to remove Visibility for fileId={}, rgId={}", newFileId, rgId, ex);
                }
            }
            try
            {
                if (!metadataService.deleteFiles(Collections.singletonList(newFileId)))
                {
                    throw new MetadataException("failed to delete temporary GC catalog entry for fileId=" + newFileId);
                }
            }
            catch (Exception ex)
            {
                logger.warn("StorageGC cleanup: failed to delete catalog entry for fileId={}", newFileId, ex);
            }
        }
        try
        {
            if (storage.exists(newFilePath))
            {
                storage.delete(newFilePath, false);
            }
        }
        catch (IOException ex)
        {
            logger.warn("StorageGC cleanup: failed to delete physical file {}", newFilePath, ex);
        }
    }

    // -------------------------------------------------------------------------
    // Visibility Synchronization
    // -------------------------------------------------------------------------

    /**
     * Exports deletion chain items from old files, performs coordinate transformation,
     * and imports them into the corresponding new file RGs.
     *
     * @param result   the rewrite result containing forward mappings and new file metadata
     * @param safeGcTs the safe GC timestamp; only chain items with ts > safeGcTs are exported
     */
    void syncVisibility(RewriteResult result, long safeGcTs) throws RetinaException
    {
        // Buckets keyed by new RG id; values are interleaved (newRgRowOffset, timestamp) pairs
        // stored as growable primitive long[] to avoid Long boxing overhead.
        Map<Integer, long[]> bucketArrays = new HashMap<>();
        Map<Integer, Integer> bucketSizes = new HashMap<>();

        for (FileCandidate fc : result.group.files)
        {
            Map<Integer, int[]> fileMapping = result.forwardRgMappings.get(fc.fileId);
            if (fileMapping == null)
            {
                continue;
            }
            for (int rgId = 0; rgId < fc.rgCount; rgId++)
            {
                long[] items = resourceManager.exportChainItemsAfter(fc.fileId, rgId, safeGcTs);
                if (items == null || items.length == 0)
                {
                    continue;
                }
                int[] fwdMapping = fileMapping.get(rgId);
                if (fwdMapping == null)
                {
                    continue;
                }

                for (int i = 0; i < items.length; i += 2)
                {
                    int oldRgRowOffset = (int) items[i];
                    long timestamp = items[i + 1];
                    if (oldRgRowOffset < 0 || oldRgRowOffset >= fwdMapping.length)
                    {
                        continue;
                    }
                    int newGlobal = fwdMapping[oldRgRowOffset];
                    if (newGlobal < 0)
                    {
                        continue;
                    }
                    int newRgId = RetinaResourceManager.rgIdForGlobalRowOffset(newGlobal, result.newFileRgRowStart);
                    int newRgOff = newGlobal - result.newFileRgRowStart[newRgId];

                    int size = bucketSizes.getOrDefault(newRgId, 0);
                    long[] arr = bucketArrays.get(newRgId);
                    if (arr == null)
                    {
                        arr = new long[16];
                        bucketArrays.put(newRgId, arr);
                    }
                    else if (size + 2 > arr.length)
                    {
                        arr = Arrays.copyOf(arr, arr.length * 2);
                        bucketArrays.put(newRgId, arr);
                    }
                    arr[size] = newRgOff;
                    arr[size + 1] = timestamp;
                    bucketSizes.put(newRgId, size + 2);
                }
            }
        }

        for (Map.Entry<Integer, long[]> entry : bucketArrays.entrySet())
        {
            int newRgId = entry.getKey();
            int size = bucketSizes.get(newRgId);
            long[] interleaved = (size == entry.getValue().length)
                    ? entry.getValue()
                    : Arrays.copyOf(entry.getValue(), size);
            resourceManager.importDeletionChain(result.newFileId, newRgId, interleaved);
        }
    }

    // -------------------------------------------------------------------------
    // PK byte extraction
    // -------------------------------------------------------------------------

    /**
     * Extracts and concatenates the primary-key column bytes from a batch row,
     * using the same encoding as {@link TypeDescription#convertSqlStringToByte}.
     */
    private static ByteString extractPkBytes(VectorizedRowBatch batch, int row,
                                             int[] pkColIndices, List<TypeDescription> pkColTypes)
    {
        if (pkColIndices.length == 1)
        {
            byte[] bytes = pkColTypes.get(0).convertColumnVectorToByte(batch.cols[pkColIndices[0]], row);
            return ByteString.copyFrom(bytes);
        }
        int totalLen = 0;
        byte[][] parts = new byte[pkColIndices.length][];
        for (int i = 0; i < pkColIndices.length; i++)
        {
            parts[i] = pkColTypes.get(i).convertColumnVectorToByte(batch.cols[pkColIndices[i]], row);
            totalLen += parts[i].length;
        }
        ByteBuffer buf = ByteBuffer.allocate(totalLen);
        for (byte[] part : parts)
        {
            buf.put(part);
        }
        return ByteString.copyFrom((ByteBuffer) buf.rewind());
    }

    // -------------------------------------------------------------------------
    // Index Synchronization
    // -------------------------------------------------------------------------

    /**
     * Allocates new rowIds, inserts MainIndex entries, and updates the SinglePointIndex
     * for all kept rows in the rewrite result.
     *
     * @param result  the rewrite result (mutated: sets {@code newRowIdStart} and {@code oldRowIds})
     * @param tableId the table owning the rewritten files
     */
    void syncIndex(RewriteResult result, long tableId) throws Exception
    {
        int totalRows = result.newFileRgRowStart[result.newFileRgCount];
        if (totalRows == 0)
        {
            return;
        }

        MainIndex mainIndex = MainIndexFactory.Instance().getMainIndex(tableId);
        IndexProto.RowIdBatch rowIdBatch = mainIndex.allocateRowIdBatch(tableId, totalRows);
        long newRowIdStart = rowIdBatch.getRowIdStart();
        result.newRowIdStart = newRowIdStart;

        insertMainIndexEntries(result, mainIndex, newRowIdStart);

        if (!result.pendingIndexEntries.isEmpty())
        {
            result.oldRowIds = updateSinglePointIndex(result, tableId, newRowIdStart);
        }
    }

    private void insertMainIndexEntries(RewriteResult result, MainIndex mainIndex,
                                        long newRowIdStart) throws Exception
    {
        int totalRows = result.newFileRgRowStart[result.newFileRgCount];
        List<IndexProto.PrimaryIndexEntry> entries = new ArrayList<>(totalRows);
        int curRgId = 0;
        for (int i = 0; i < totalRows; i++)
        {
            while (curRgId + 1 < result.newFileRgCount
                    && i >= result.newFileRgRowStart[curRgId + 1])
            {
                curRgId++;
            }
            int rgOff = i - result.newFileRgRowStart[curRgId];
            entries.add(IndexProto.PrimaryIndexEntry.newBuilder()
                    .setRowId(newRowIdStart + i)
                    .setRowLocation(IndexProto.RowLocation.newBuilder()
                            .setFileId(result.newFileId).setRgId(curRgId).setRgRowOffset(rgOff))
                    .build());
        }
        mainIndex.putEntries(entries);
        mainIndex.flushCache(result.newFileId);
    }

    private List<Long> updateSinglePointIndex(RewriteResult result, long tableId,
                                              long newRowIdStart) throws Exception
    {
        io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex primaryIndex =
                metadataService.getPrimaryIndex(tableId);
        IndexOption indexOption = IndexOption.builder().vNodeId(result.group.virtualNodeId).build();
        io.pixelsdb.pixels.common.index.SinglePointIndex spIndex =
                SinglePointIndexFactory.Instance().getSinglePointIndex(
                        tableId, primaryIndex.getId(), indexOption);

        // Keep oldRowIds aligned 1:1 with pendingIndexEntries: slots where
        // updatePrimaryEntry returned a negative value are stored as -1L placeholders.
        // rollbackSinglePointIndex relies on this alignment to pair each PendingIndexEntry
        // with its own old rowId.
        List<Long> oldRowIds = new ArrayList<>(result.pendingIndexEntries.size());
        for (PendingIndexEntry pe : result.pendingIndexEntries)
        {
            long newRowId = newRowIdStart + pe.newGlobalRowOffset;
            IndexProto.IndexKey key = IndexProto.IndexKey.newBuilder()
                    .setTableId(tableId).setIndexId(primaryIndex.getId())
                    .setKey(pe.pkBytes).setTimestamp(pe.createTs).build();
            long oldRowId = spIndex.updatePrimaryEntry(key, newRowId);
            oldRowIds.add(oldRowId);
            if (oldRowId < 0)
            {
                logger.warn("StorageGC syncIndex: updatePrimaryEntry returned {} for tableId={}, " +
                        "newGlobalRowOffset={} — index may be inconsistent", oldRowId, tableId, pe.newGlobalRowOffset);
            }
        }
        return oldRowIds;
    }

    // -------------------------------------------------------------------------
    // Commit (atomic switch + delayed cleanup)
    // -------------------------------------------------------------------------

    /**
     * Atomically promotes the new TEMPORARY file to REGULAR, deletes old files from
     * the catalog, unregisters dual-write, and enqueues the old files for delayed cleanup.
     */
    void commitFileGroup(RewriteResult result) throws Exception
    {
        List<Long> oldFileIds = result.group.files.stream()
                .map(fc -> fc.fileId).collect(Collectors.toList());

        try
        {
            metadataService.atomicSwapFiles(result.newFileId, oldFileIds);
        }
        catch (Exception e)
        {
            File newFile = metadataService.getFileById(result.newFileId);
            if (newFile != null && newFile.getType() == File.Type.REGULAR)
            {
                logger.warn("atomicSwapFiles gRPC failed but server committed, continuing", e);
            }
            else
            {
                throw e;
            }
        }

        unregisterDualWrite(result);

        long retireDeadline = System.currentTimeMillis() + retireDelayMs;
        for (FileCandidate fc : result.group.files)
        {
            resourceManager.scheduleRetiredFile(
                    new RetinaResourceManager.RetiredFile(
                            fc.fileId, fc.rgCount, fc.filePath, retireDeadline, result.oldRowIds));
        }
    }

    // -------------------------------------------------------------------------
    // Rollback
    // -------------------------------------------------------------------------

    /**
     * Best-effort rollback of a partially-completed GC cycle for one file group.
     * Reverses index changes, dual-write, visibility, catalog, and physical file.
     */
    void rollback(RewriteResult result)
    {
        if (result == null || result.newFileId <= 0)
        {
            return;
        }
        try
        {
            File newFile = metadataService.getFileById(result.newFileId);
            if (newFile != null && newFile.getType() == File.Type.REGULAR)
            {
                logger.error("Cannot rollback: new file already REGULAR (id={})", result.newFileId);
                return;
            }

            if (result.oldRowIds != null && !result.oldRowIds.isEmpty())
            {
                rollbackSinglePointIndex(result);
            }

            if (result.newRowIdStart > 0)
            {
                try
                {
                    int totalRows = result.newFileRgRowStart[result.newFileRgCount];
                    MainIndex mainIndex = MainIndexFactory.Instance().getMainIndex(result.group.tableId);
                    mainIndex.deleteRowIdRange(new RowIdRange(result.newRowIdStart,
                            result.newRowIdStart + totalRows, result.newFileId, 0, 0, totalRows));
                }
                catch (Exception ex)
                {
                    logger.warn("Rollback: failed to clean MainIndex for fileId={}", result.newFileId, ex);
                }
            }

            unregisterDualWrite(result);

            for (int rgId = 0; rgId < result.newFileRgCount; rgId++)
            {
                try
                {
                    resourceManager.reclaimVisibility(result.newFileId, rgId, 0);
                }
                catch (Exception ex)
                {
                    logger.warn("Rollback: failed to remove Visibility for fileId={}, rgId={}",
                            result.newFileId, rgId, ex);
                }
            }

            try
            {
                if (!metadataService.deleteFiles(Collections.singletonList(result.newFileId)))
                {
                    throw new MetadataException("failed to rollback GC catalog entry for fileId=" + result.newFileId);
                }
            }
            catch (Exception ex)
            {
                logger.warn("Rollback: failed to delete catalog entry for fileId={}", result.newFileId, ex);
            }

            try
            {
                Storage storage = StorageFactory.Instance().getStorage(result.newFilePath);
                if (storage.exists(result.newFilePath))
                {
                    storage.delete(result.newFilePath, false);
                }
            }
            catch (IOException ex)
            {
                logger.warn("Rollback: failed to delete physical file {}", result.newFilePath, ex);
            }
        }
        catch (Exception e)
        {
            logger.error("Rollback failed for FileGroup tableId={}", result.group.tableId, e);
        }
    }

    private void rollbackSinglePointIndex(RewriteResult result)
    {
        try
        {
            io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex primaryIndex =
                    metadataService.getPrimaryIndex(result.group.tableId);
            if (primaryIndex == null)
            {
                return;
            }
            IndexOption indexOption = IndexOption.builder().vNodeId(result.group.virtualNodeId).build();
            io.pixelsdb.pixels.common.index.SinglePointIndex spIndex =
                    SinglePointIndexFactory.Instance().getSinglePointIndex(
                            result.group.tableId, primaryIndex.getId(), indexOption);

            // Alignment invariant: oldRowIds.size() == pendingIndexEntries.size()
            // (established in updateSinglePointIndex). Walk them in lockstep by
            // position and skip -1 placeholders — those slots had no prior entry
            // to redirect, so there is nothing to revert.
            int n = Math.min(result.pendingIndexEntries.size(), result.oldRowIds.size());
            if (n < result.pendingIndexEntries.size() || n < result.oldRowIds.size())
            {
                logger.warn("Rollback: pendingIndexEntries.size={} != oldRowIds.size={}; " +
                                "rolling back the common prefix only — index may remain inconsistent",
                        result.pendingIndexEntries.size(), result.oldRowIds.size());
            }
            for (int i = 0; i < n; i++)
            {
                long oldRowId = result.oldRowIds.get(i);
                if (oldRowId < 0)
                {
                    continue;
                }
                PendingIndexEntry pe = result.pendingIndexEntries.get(i);
                IndexProto.IndexKey key = IndexProto.IndexKey.newBuilder()
                        .setTableId(result.group.tableId).setIndexId(primaryIndex.getId())
                        .setKey(pe.pkBytes).setTimestamp(pe.createTs).build();
                spIndex.updatePrimaryEntry(key, oldRowId);
            }
        }
        catch (Exception e)
        {
            logger.warn("Rollback: failed to revert SinglePointIndex for tableId={}",
                    result.group.tableId, e);
        }
    }

    // -------------------------------------------------------------------------
    // Per-group orchestration
    // -------------------------------------------------------------------------

    /**
     * Processes a single file group through the complete GC pipeline:
     * rewrite → dual-write → visibility sync → index sync → commit.
     * On failure, releases group bitmaps and performs a best-effort rollback.
     */
    void processFileGroup(FileGroup group, long safeGcTs,
                          Map<String, long[]> gcSnapshotBitmaps)
    {
        RewriteResult result = null;
        try
        {
            result = rewriteFileGroup(group, safeGcTs, gcSnapshotBitmaps);

            if (result.newFileId <= 0)
            {
                return;
            }

            registerDualWrite(result);

            syncVisibility(result, safeGcTs);

            syncIndex(result, group.tableId);

            commitFileGroup(result);

            logger.info("StorageGC completed for FileGroup tableId={}, vNodeId={}, newFileId={}",
                    group.tableId, group.virtualNodeId, result.newFileId);
        }
        catch (Exception e)
        {
            logger.error("StorageGC failed for FileGroup tableId={}, vNodeId={}",
                    group.tableId, group.virtualNodeId, e);
            releaseGroupBitmaps(group, gcSnapshotBitmaps);
            rollback(result);
        }
    }

    private void releaseGroupBitmaps(FileGroup group, Map<String, long[]> gcSnapshotBitmaps)
    {
        for (FileCandidate fc : group.files)
        {
            for (int rgId = 0; rgId < fc.rgCount; rgId++)
            {
                gcSnapshotBitmaps.remove(RetinaUtils.buildRgKey(fc.fileId, rgId));
            }
        }
    }
}
