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

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Path;
import io.pixelsdb.pixels.common.metadata.domain.Schema;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.common.utils.PixelsFileNameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Storage GC: identifies high-deletion-ratio files and (in later tasks) rewrites them
 * to reclaim physical storage while keeping active queries unaffected.
 *
 * <p>In Task 2, only S1 (scan + grouping) and in-memory bitmap trimming are implemented;
 * S2-S6 rewrite steps are added in Tasks 3-6.
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
    final double gcThreshold;
    private final long targetFileSize;
    private final int maxFileGroupsPerRun;

    /**
     * Tracks file IDs currently being processed by an ongoing Storage GC cycle.
     * Prevents the same file from being picked up by a second concurrent scan.
     */
    private final ConcurrentHashMap<Long, Boolean> processingFiles = new ConcurrentHashMap<>();

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
        final long totalRows;
        final double invalidRatio;

        FileCandidate(File file, String filePath, long fileId, int rgCount,
                      long tableId, int virtualNodeId, long totalRows, double invalidRatio)
        {
            this.file = file;
            this.filePath = filePath;
            this.fileId = fileId;
            this.rgCount = rgCount;
            this.tableId = tableId;
            this.virtualNodeId = virtualNodeId;
            this.totalRows = totalRows;
            this.invalidRatio = invalidRatio;
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
            this.tableId = tableId;
            this.virtualNodeId = virtualNodeId;
            this.files = files;
        }
    }

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    StorageGarbageCollector(RetinaResourceManager resourceManager,
                            MetadataService metadataService,
                            double gcThreshold,
                            long targetFileSize,
                            int maxFileGroupsPerRun)
    {
        this.resourceManager = resourceManager;
        this.metadataService = metadataService;
        this.gcThreshold = gcThreshold;
        this.targetFileSize = targetFileSize;
        this.maxFileGroupsPerRun = maxFileGroupsPerRun;
    }

    // -------------------------------------------------------------------------
    // Public entry point
    // -------------------------------------------------------------------------

    /**
     * Runs one Storage GC cycle: S1 scan → group → process candidates.
     *
     * <p>The GC checkpoint has already been written unconditionally by
     * {@link RetinaResourceManager#runGC()} before this method is called.
     * This method is solely responsible for identifying candidate files and
     * processing them (S2-S6 added in Tasks 3-6).
     *
     * @param safeGcTs          safe GC timestamp produced by Memory GC
     * @param rgStats           per-RG visibility statistics pre-computed during Memory GC;
     *                          key = {@code "<fileId>_<rgId>"},
     *                          value = {@code long[]{recordNum, invalidCount}}
     * @param gcSnapshotBitmaps per-RG snapshot bitmaps (mutated in-place by
     *                          {@link #processFileGroups}: non-candidate entries removed)
     */
    void runStorageGC(long safeGcTs, Map<String, long[]> rgStats,
                      Map<String, long[]> gcSnapshotBitmaps)
    {
        List<FileGroup> fileGroups = scanAndGroupFiles(rgStats);
        if (!fileGroups.isEmpty())
        {
            processFileGroups(fileGroups, safeGcTs, gcSnapshotBitmaps);
        }
    }

    // -------------------------------------------------------------------------
    // S1 — scan and group
    // -------------------------------------------------------------------------

    /**
     * Scans all schemas/tables and returns at most {@link #maxFileGroupsPerRun} groups of
     * candidate files, sorted by average {@code invalidRatio} descending.
     *
     * @param rgStats per-RG visibility statistics pre-computed by
     *                {@link RetinaResourceManager#runGC()} during the Memory GC pass;
     *                key = {@code "<fileId>_<rgId>"}, value = {@code long[]{recordNum, invalidCount}}
     *                where {@code invalidCount} is the number of deleted rows (bitmap popcount).
     *                Using pre-computed stats avoids re-traversing the visibility map and
     *                re-computing bitcounts that were already produced during Memory GC.
     */
    List<FileGroup> scanAndGroupFiles(Map<String, long[]> rgStats)
    {
        List<FileCandidate> candidates = new ArrayList<>();

        List<Schema> schemas;
        try
        {
            schemas = metadataService.getSchemas();
        }
        catch (MetadataException e)
        {
            logger.error("Storage GC S1: failed to retrieve schemas", e);
            return new ArrayList<>();
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
                logger.warn("Storage GC S1: failed to get tables for schema '{}', skipping",
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
                    logger.warn("Storage GC S1: failed to get layout for {}.{}, skipping",
                            schema.getName(), table.getName(), e);
                    continue;
                }
                if (layout == null)
                {
                    continue;
                }

                // Scan both ordered and compact paths; single/copy files are filtered
                // by isGcEligible() inside the inner loop.
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
                        logger.warn("Storage GC S1: failed to get files for pathId={}, skipping",
                                path.getId(), e);
                        continue;
                    }

                    for (File file : files)
                    {
                        String filePath = File.getFilePath(path, file);

                        // Skip non-GC-eligible types (single, copy) and already-processing files.
                        if (!PixelsFileNameUtils.isGcEligible(filePath))
                        {
                            continue;
                        }
                        if (processingFiles.containsKey(file.getId()))
                        {
                            continue;
                        }

                        // Aggregate file-level stats from the pre-computed per-RG rgStats.
                        // rgStats[rgKey] = {recordNum, invalidCount}; entries absent from the
                        // map mean the RG has no visibility record and should be skipped.
                        long totalRows = 0;
                        long invalidCount = 0;
                        for (int rgId = 0; rgId < file.getNumRowGroup(); rgId++)
                        {
                            String rgKey = file.getId() + "_" + rgId;
                            long[] stats = rgStats.get(rgKey);
                            if (stats == null)
                            {
                                continue;
                            }
                            totalRows    += stats[0];
                            invalidCount += stats[1];
                        }
                        if (totalRows == 0)
                        {
                            continue;
                        }

                        double invalidRatio = (double) invalidCount / totalRows;
                        if (invalidRatio > gcThreshold)
                        {
                            int vNodeId = PixelsFileNameUtils.extractVirtualNodeId(filePath);
                            candidates.add(new FileCandidate(
                                    file, filePath, file.getId(), file.getNumRowGroup(),
                                    table.getId(), vNodeId, totalRows, invalidRatio));
                        }
                    }
                }
            }
        }

        return groupAndMerge(candidates);
    }

    /**
     * Groups candidates by {@code (tableId, virtualNodeId)}, sorts each group by
     * {@code invalidRatio} descending, then returns the top-N groups (by average
     * {@code invalidRatio}) capped at {@link #maxFileGroupsPerRun}.
     */
    List<FileGroup> groupAndMerge(List<FileCandidate> candidates)
    {
        // Two-level map: tableId → virtualNodeId → files
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
            for (Map.Entry<Integer, List<FileCandidate>> vnodeEntry : tableEntry.getValue().entrySet())
            {
                List<FileCandidate> files = vnodeEntry.getValue();
                files.sort(Comparator.comparingDouble((FileCandidate c) -> c.invalidRatio).reversed());
                groups.add(new FileGroup(tableEntry.getKey(), vnodeEntry.getKey(), files));
            }
        }

        // Sort groups by average invalidRatio descending so the worst groups are processed first.
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
     * Processes the candidate file groups produced by S1.
     *
     * <p>Because the GC checkpoint has already been written unconditionally in
     * {@link RetinaResourceManager#runGC()} with the <em>full</em> visibility snapshot
     * before this method is called, bitmaps for non-candidate files are no longer needed
     * for S2-S6. This method therefore immediately trims {@code gcSnapshotBitmaps} to
     * retain only the entries for candidate files, releasing memory for files that were
     * either below the threshold or excluded by the {@link #maxFileGroupsPerRun} cap.
     *
     * <p>Currently (Task 2) only logging is performed after the trim; S2-S6 rewrite steps
     * will be added in Tasks 3-6.
     *
     * @param fileGroups        non-empty list of candidate groups from {@link #scanAndGroupFiles}
     * @param safeGcTs          safe GC timestamp produced by Memory GC
     * @param gcSnapshotBitmaps per-RG snapshot bitmaps (mutated in-place: non-candidate
     *                          entries are removed to reduce memory pressure)
     */
    void processFileGroups(List<FileGroup> fileGroups, long safeGcTs,
                           Map<String, long[]> gcSnapshotBitmaps)
    {
        // Release bitmap entries for files that were not selected (below threshold or
        // over the maxFileGroupsPerRun cap). The checkpoint has already been written with
        // the full snapshot, so S2-S6 only needs bitmaps for candidate files.
        Set<String> candidateRgKeys = collectCandidateRgKeys(fileGroups);
        gcSnapshotBitmaps.entrySet().removeIf(e -> !candidateRgKeys.contains(e.getKey()));

        // Log candidate groups; S2-S6 will be added in later tasks.
        for (FileGroup group : fileGroups)
        {
            double avgRatio = group.files.stream()
                    .mapToDouble(c -> c.invalidRatio).average().orElse(0.0);
            logger.info("StorageGC candidate: table={}, vNodeId={}, files={}, avgInvalidRatio={:.4f}",
                    group.tableId, group.virtualNodeId, group.files.size(), avgRatio);
            // TODO Task 3+: processFileGroup(group, safeGcTs, gcSnapshotBitmaps)
            // Note: gcSnapshotBitmaps entries for this group must be released inside
            // processFileGroup() *after* S2 (row-filtering rewrite) completes,
            // because S2 reads the bitmaps to determine which rows are alive.
        }
    }

    /**
     * Collects all {@code "<fileId>_<rgId>"} keys for every RG in every candidate file
     * across all groups. Used by {@link #processFileGroups} to trim the
     * {@code gcSnapshotBitmaps} map after the checkpoint has been written.
     */
    Set<String> collectCandidateRgKeys(List<FileGroup> fileGroups)
    {
        Set<String> keys = new HashSet<>();
        for (FileGroup group : fileGroups)
        {
            for (FileCandidate c : group.files)
            {
                for (int rgId = 0; rgId < c.rgCount; rgId++)
                {
                    keys.add(c.fileId + "_" + rgId);
                }
            }
        }
        return keys;
    }
}
