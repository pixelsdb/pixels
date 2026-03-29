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

import io.pixelsdb.pixels.common.metadata.domain.File;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies Task 2: StorageGarbageCollector S1 scan + grouping + bitmap trim.
 *
 * <h3>Verification points</h3>
 * <ol>
 *   <li><b>groupAndMerge correctness</b> — candidates are grouped by {@code (tableId, virtualNodeId)},
 *       sorted by average {@code invalidRatio} descending, and capped at {@code maxFileGroupsPerRun}.</li>
 *   <li><b>S1 threshold filtering</b> — only files with {@code invalidRatio > gcThreshold} are
 *       included; e.g., threshold=0.5 → 60 % and 80 % selected, 40 % excluded.</li>
 *   <li><b>Bitmap trim after scan</b> — {@code processFileGroups} retains only candidate RG bitmaps
 *       from {@code gcSnapshotBitmaps}, releasing memory for files not selected for rewrite.</li>
 *   <li><b>System stability</b> — in Task 2, {@code processFileGroups} only trims and logs;
 *       S2-S6 are not triggered and the visibility map is not modified.</li>
 * </ol>
 */
public class TestStorageGarbageCollector
{
    // -----------------------------------------------------------------------
    // Test infrastructure
    // -----------------------------------------------------------------------

    private RetinaResourceManager retinaManager;

    @Before
    public void setUp()
    {
        retinaManager = RetinaResourceManager.Instance();
        resetManagerState();
    }

    @After
    public void tearDown()
    {
        resetManagerState();
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Clears RetinaResourceManager internal maps and resets latestGcTimestamp. */
    private void resetManagerState()
    {
        try
        {
            Field rgMapField = RetinaResourceManager.class.getDeclaredField("rgVisibilityMap");
            rgMapField.setAccessible(true);
            ((Map<?, ?>) rgMapField.get(retinaManager)).clear();

            Field gcTsField = RetinaResourceManager.class.getDeclaredField("latestGcTimestamp");
            gcTsField.setAccessible(true);
            gcTsField.setLong(retinaManager, -1L);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to reset RetinaResourceManager state", e);
        }
    }

    /** Creates a minimal {@link File} domain object with given id and rgCount. */
    private static File makeFile(long fileId, int rgCount)
    {
        File f = new File();
        f.setId(fileId);
        f.setNumRowGroup(rgCount);
        return f;
    }

    /**
     * Creates a {@code long[]} GC snapshot bitmap for one RG where exactly {@code deletedRows}
     * out of {@code totalRows} rows are marked as deleted (rows 0 .. deletedRows-1 are set).
     */
    private static long[] makeBitmap(int totalRows, int deletedRows)
    {
        int words = (totalRows + 63) / 64;
        long[] bitmap = new long[words];
        for (int r = 0; r < deletedRows; r++)
        {
            bitmap[r / 64] |= (1L << (r % 64));
        }
        return bitmap;
    }

    /**
     * Creates a per-RG stats entry {@code {recordNum, invalidCount}} for one RG
     * where exactly {@code deletedRows} out of {@code totalRows} rows are deleted.
     * This mirrors what {@code runGC()} pre-computes during the Memory GC pass.
     */
    private static long[] makeRgStats(int totalRows, int deletedRows)
    {
        return new long[]{totalRows, deletedRows};
    }

    // -----------------------------------------------------------------------
    // Stub: a StorageGarbageCollector subclass that bypasses MetadataService
    // and lets callers inject a controlled list of fake files to scan.
    // -----------------------------------------------------------------------

    /** Represents a fake catalog file entry used by {@link DirectScanStorageGC}. */
    static class FakeFileEntry
    {
        final long fileId;
        final int rgCount;
        final long tableId;
        final int virtualNodeId;

        FakeFileEntry(long fileId, int rgCount, long tableId, int virtualNodeId)
        {
            this.fileId = fileId;
            this.rgCount = rgCount;
            this.tableId = tableId;
            this.virtualNodeId = virtualNodeId;
        }
    }

    /**
     * StorageGarbageCollector subclass that replaces the metadata scan loop with
     * a caller-supplied list of {@link FakeFileEntry} objects, while reusing the
     * real invalidRatio computation and {@link StorageGarbageCollector#groupAndMerge} logic.
     */
    static class DirectScanStorageGC extends StorageGarbageCollector
    {
        private final List<FakeFileEntry> fakeEntries;

        DirectScanStorageGC(RetinaResourceManager rm, double threshold,
                            int maxGroups, List<FakeFileEntry> fakeEntries)
        {
            super(rm, null, threshold, 134_217_728L, maxGroups);
            this.fakeEntries = fakeEntries;
        }

        @Override
        List<FileGroup> scanAndGroupFiles(Map<String, long[]> rgStats)
        {
            List<FileCandidate> candidates = new ArrayList<>();
            for (FakeFileEntry entry : fakeEntries)
            {
                long totalRows = 0;
                long invalidCount = 0;
                for (int rgId = 0; rgId < entry.rgCount; rgId++)
                {
                    String rgKey = entry.fileId + "_" + rgId;
                    long[] stats = rgStats.get(rgKey);
                    if (stats == null) continue;
                    totalRows    += stats[0];
                    invalidCount += stats[1];
                }
                if (totalRows == 0) continue;
                double ratio = (double) invalidCount / totalRows;
                if (ratio > gcThreshold)
                {
                    candidates.add(new FileCandidate(
                            makeFile(entry.fileId, entry.rgCount),
                            "fake_" + entry.fileId + "_0_" + entry.virtualNodeId + "_ordered.pxl",
                            entry.fileId, entry.rgCount,
                            entry.tableId, entry.virtualNodeId,
                            totalRows, ratio));
                }
            }
            return groupAndMerge(candidates);
        }
    }

    // -----------------------------------------------------------------------
    // Tests: groupAndMerge logic
    // -----------------------------------------------------------------------

    /**
     * Three candidates from three distinct {@code (tableId, virtualNodeId)} pairs;
     * expect three separate groups sorted by invalidRatio descending.
     */
    @Test
    public void testGroupAndMerge_threeDistinctGroups()
    {
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, 0L, 10);

        List<StorageGarbageCollector.FileCandidate> candidates = Arrays.asList(
                new StorageGarbageCollector.FileCandidate(makeFile(1, 1), "f1", 1, 1, 1L, 0, 100, 0.60),
                new StorageGarbageCollector.FileCandidate(makeFile(2, 1), "f2", 2, 1, 1L, 1, 100, 0.70),
                new StorageGarbageCollector.FileCandidate(makeFile(3, 1), "f3", 3, 1, 2L, 0, 100, 0.80)
        );

        List<StorageGarbageCollector.FileGroup> groups = gc.groupAndMerge(candidates);

        assertEquals("expected 3 groups", 3, groups.size());
        // sorted by avg invalidRatio desc: 0.80, 0.70, 0.60
        assertEquals("group 0 should have highest ratio (0.80)", 0.80,
                groups.get(0).files.get(0).invalidRatio, 1e-9);
        assertEquals("group 1 should have ratio 0.70", 0.70,
                groups.get(1).files.get(0).invalidRatio, 1e-9);
        assertEquals("group 2 should have lowest ratio (0.60)", 0.60,
                groups.get(2).files.get(0).invalidRatio, 1e-9);
    }

    /**
     * Two candidates with the same {@code (tableId, virtualNodeId)} must be in one group,
     * with files sorted by invalidRatio descending inside the group.
     */
    @Test
    public void testGroupAndMerge_twoFilesInSameGroup()
    {
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, 0L, 10);

        List<StorageGarbageCollector.FileCandidate> candidates = Arrays.asList(
                new StorageGarbageCollector.FileCandidate(makeFile(1, 1), "f1", 1, 1, 1L, 5, 100, 0.60),
                new StorageGarbageCollector.FileCandidate(makeFile(2, 1), "f2", 2, 1, 1L, 5, 100, 0.80)
        );

        List<StorageGarbageCollector.FileGroup> groups = gc.groupAndMerge(candidates);

        assertEquals("both candidates should form one group", 1, groups.size());
        StorageGarbageCollector.FileGroup grp = groups.get(0);
        assertEquals(1L, grp.tableId);
        assertEquals(5, grp.virtualNodeId);
        assertEquals(2, grp.files.size());
        assertTrue("files within group sorted by invalidRatio desc",
                grp.files.get(0).invalidRatio >= grp.files.get(1).invalidRatio);
        assertEquals(0.80, grp.files.get(0).invalidRatio, 1e-9);
        assertEquals(0.60, grp.files.get(1).invalidRatio, 1e-9);
    }

    /**
     * When there are more candidate groups than {@code maxFileGroupsPerRun},
     * only the top-N groups (highest average invalidRatio) are returned.
     */
    @Test
    public void testGroupAndMerge_maxGroupsCap()
    {
        int max = 3;
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, 0L, max);

        // Build 5 groups with different tableIds and clear invalidRatios (0.55..0.99)
        List<StorageGarbageCollector.FileCandidate> candidates = new ArrayList<>();
        for (int i = 0; i < 5; i++)
        {
            candidates.add(new StorageGarbageCollector.FileCandidate(
                    makeFile(i, 1), "f" + i, i, 1, (long) (i + 10), 0, 100, 0.55 + i * 0.11));
        }

        List<StorageGarbageCollector.FileGroup> groups = gc.groupAndMerge(candidates);

        assertEquals("groups must be capped at maxFileGroupsPerRun", max, groups.size());
        // First group must have the highest average invalidRatio
        double firstAvg = groups.get(0).files.stream()
                .mapToDouble(c -> c.invalidRatio).average().orElse(0);
        double lastAvg = groups.get(groups.size() - 1).files.stream()
                .mapToDouble(c -> c.invalidRatio).average().orElse(0);
        assertTrue("groups must be sorted best-first", firstAvg >= lastAvg);
    }

    /**
     * An empty candidate list must produce an empty group list.
     */
    @Test
    public void testGroupAndMerge_emptyCandidates()
    {
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, 0L, 10);
        List<StorageGarbageCollector.FileGroup> groups =
                gc.groupAndMerge(Collections.emptyList());
        assertTrue("empty candidates → empty groups", groups.isEmpty());
    }

    // -----------------------------------------------------------------------
    // Tests: S1 threshold filtering (via DirectScanStorageGC)
    // -----------------------------------------------------------------------

    /**
     * Three files with deletion ratios 60 %, 40 %, 80 % against threshold=0.5;
     * only the 60 % and 80 % files should appear as candidates, and they must be
     * grouped by {@code (tableId, virtualNodeId)}.
     *
     * <p>This is the primary verification specified in the Task 2 doc:
     * <em>"threshold=0.5 → 60 % and 80 % selected, 40 % excluded"</em>.
     */
    @Test
    public void testScanAndGroupFiles_thresholdFiltering()
    {
        int totalRows = 100;
        long fileId60 = 60001L;
        long fileId40 = 40001L;
        long fileId80 = 80001L;

        // Build rgStats directly: {recordNum, invalidCount} per RG.
        // This simulates what runGC() pre-computes during the Memory GC pass.
        Map<String, long[]> rgStats = new HashMap<>();
        rgStats.put(fileId60 + "_0", makeRgStats(totalRows, 60));   // 60 % deleted
        rgStats.put(fileId40 + "_0", makeRgStats(totalRows, 40));   // 40 % deleted
        rgStats.put(fileId80 + "_0", makeRgStats(totalRows, 80));   // 80 % deleted

        List<FakeFileEntry> fakeFiles = Arrays.asList(
                new FakeFileEntry(fileId60, 1, 1L, 0),  // ratio=0.60, should be selected
                new FakeFileEntry(fileId40, 1, 1L, 0),  // ratio=0.40, should be excluded
                new FakeFileEntry(fileId80, 1, 2L, 0)   // ratio=0.80, should be selected (different table)
        );

        DirectScanStorageGC gc = new DirectScanStorageGC(
                retinaManager, 0.5, 10, fakeFiles);

        List<StorageGarbageCollector.FileGroup> groups = gc.scanAndGroupFiles(rgStats);

        // 60 % and 80 % → 2 separate groups (different tableId: 1 and 2)
        assertEquals("2 groups expected (60% and 80%)", 2, groups.size());

        // Collect all selected fileIds
        List<Long> selectedIds = new ArrayList<>();
        for (StorageGarbageCollector.FileGroup g : groups)
        {
            for (StorageGarbageCollector.FileCandidate c : g.files)
            {
                selectedIds.add(c.fileId);
            }
        }

        assertTrue("fileId60 should be selected", selectedIds.contains(fileId60));
        assertFalse("fileId40 should NOT be selected (ratio <= threshold)", selectedIds.contains(fileId40));
        assertTrue("fileId80 should be selected", selectedIds.contains(fileId80));
    }

    /**
     * Two files sharing the same {@code (tableId, virtualNodeId)} must be in the same group.
     */
    @Test
    public void testScanAndGroupFiles_sameTableVNodeGroupedTogether()
    {
        int totalRows = 100;
        long fileIdA = 70001L;
        long fileIdB = 70002L;

        Map<String, long[]> rgStats = new HashMap<>();
        rgStats.put(fileIdA + "_0", makeRgStats(totalRows, 60));  // 60 %
        rgStats.put(fileIdB + "_0", makeRgStats(totalRows, 75));  // 75 %

        // Both files belong to same (tableId=5, vNodeId=3)
        List<FakeFileEntry> fakeFiles = Arrays.asList(
                new FakeFileEntry(fileIdA, 1, 5L, 3),
                new FakeFileEntry(fileIdB, 1, 5L, 3)
        );

        DirectScanStorageGC gc = new DirectScanStorageGC(
                retinaManager, 0.5, 10, fakeFiles);

        List<StorageGarbageCollector.FileGroup> groups = gc.scanAndGroupFiles(rgStats);

        assertEquals("both files share (table=5, vNode=3) → 1 group", 1, groups.size());
        assertEquals("group must contain 2 files", 2, groups.get(0).files.size());
        assertEquals(5L, groups.get(0).tableId);
        assertEquals(3, groups.get(0).virtualNodeId);
    }

    /**
     * A file whose RG has no entry in {@code rgStats} must be skipped
     * (totalRows == 0 → excluded regardless of threshold).
     */
    @Test
    public void testScanAndGroupFiles_skipsFilesWithNoVisibility()
    {
        long orphanFileId = 99999L;  // no entry in rgStats → totalRows stays 0
        Map<String, long[]> rgStats = new HashMap<>();

        List<FakeFileEntry> fakeFiles = Collections.singletonList(
                new FakeFileEntry(orphanFileId, 1, 1L, 0));

        DirectScanStorageGC gc = new DirectScanStorageGC(
                retinaManager, 0.5, 10, fakeFiles);

        List<StorageGarbageCollector.FileGroup> groups = gc.scanAndGroupFiles(rgStats);
        assertTrue("file with no rgStats entry should be skipped", groups.isEmpty());
    }

    // -----------------------------------------------------------------------
    // Tests: processFileGroups bitmap trimming
    // -----------------------------------------------------------------------

    /**
     * After {@code processFileGroups}, the {@code gcSnapshotBitmaps} map must contain
     * only the RG keys belonging to candidate files.  Non-candidate entries must be removed
     * to release memory (the GC checkpoint has already been written with the full snapshot
     * by {@code runGC()} before this point).
     */
    @Test
    public void testProcessFileGroups_trimsBitmapMapToCandidate()
    {
        long candidateFileId = 66001L;
        long otherFileId     = 66002L;

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(candidateFileId + "_0", makeBitmap(100, 60));
        bitmaps.put(otherFileId + "_0", makeBitmap(100, 20));   // below threshold, not a candidate

        StorageGarbageCollector.FileCandidate candidate =
                new StorageGarbageCollector.FileCandidate(
                        makeFile(candidateFileId, 1), "f_" + candidateFileId,
                        candidateFileId, 1, 1L, 0, 100, 0.60);
        StorageGarbageCollector.FileGroup group =
                new StorageGarbageCollector.FileGroup(
                        1L, 0, Collections.singletonList(candidate));

        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, 134_217_728L, 10);

        gc.processFileGroups(Collections.singletonList(group), 300L, bitmaps);

        assertTrue("candidate RG key must be retained",
                bitmaps.containsKey(candidateFileId + "_0"));
        assertFalse("non-candidate RG key must be removed",
                bitmaps.containsKey(otherFileId + "_0"));
    }

    /**
     * Files excluded by the {@code maxFileGroupsPerRun} cap must also have their bitmap
     * entries removed: they were scanned but not selected for rewrite this cycle.
     */
    @Test
    public void testProcessFileGroups_trimsBitmapForCapExcludedFiles()
    {
        // Three files that would each form their own group, but maxGroups = 1
        long fileId1 = 67001L;  // highest ratio → selected
        long fileId2 = 67002L;  // medium ratio → excluded by cap
        long fileId3 = 67003L;  // lowest ratio  → excluded by cap

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId1 + "_0", makeBitmap(100, 80));
        bitmaps.put(fileId2 + "_0", makeBitmap(100, 70));
        bitmaps.put(fileId3 + "_0", makeBitmap(100, 60));

        StorageGarbageCollector.FileCandidate c1 = new StorageGarbageCollector.FileCandidate(
                makeFile(fileId1, 1), "f1", fileId1, 1, 10L, 0, 100, 0.80);

        // Only the top-1 group is passed to processFileGroups (cap=1 applied by groupAndMerge)
        StorageGarbageCollector.FileGroup selectedGroup =
                new StorageGarbageCollector.FileGroup(10L, 0, Collections.singletonList(c1));

        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, 134_217_728L, 1);

        gc.processFileGroups(Collections.singletonList(selectedGroup), 400L, bitmaps);

        assertTrue("selected file bitmap must be retained",   bitmaps.containsKey(fileId1 + "_0"));
        assertFalse("cap-excluded file2 bitmap must be removed", bitmaps.containsKey(fileId2 + "_0"));
        assertFalse("cap-excluded file3 bitmap must be removed", bitmaps.containsKey(fileId3 + "_0"));
    }

    // -----------------------------------------------------------------------
    // Tests: runStorageGC end-to-end (scan → process)
    // -----------------------------------------------------------------------

    /**
     * When {@code scanAndGroupFiles} finds no candidates, {@code runStorageGC} must be a
     * no-op: {@code gcSnapshotBitmaps} must remain unchanged.
     */
    @Test
    public void testRunStorageGC_noopWhenNoCandidates()
    {
        long fileId = 55001L;

        // Empty rgStats → totalRows = 0 for every fake file → no candidates
        Map<String, long[]> rgStats = new HashMap<>();

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmap(100, 30));

        DirectScanStorageGC gc = new DirectScanStorageGC(
                retinaManager, 0.5, 10,
                Collections.singletonList(new FakeFileEntry(fileId, 1, 1L, 0)));

        gc.runStorageGC(100L, rgStats, bitmaps);

        // No candidates → bitmaps must be untouched
        assertTrue("bitmap must be unchanged when no candidates",
                bitmaps.containsKey(fileId + "_0"));
        assertEquals("bitmap map size must stay 1", 1, bitmaps.size());
    }

    /**
     * When {@code runStorageGC} finds candidates, non-candidate bitmaps must be trimmed
     * from {@code gcSnapshotBitmaps} while candidate bitmaps are retained for S2-S6.
     */
    @Test
    public void testRunStorageGC_trimsBitmapsForCandidates()
    {
        long candidateFileId    = 56001L;  // 70 % deleted → above threshold
        long nonCandidateFileId = 56002L;  // 20 % deleted → below threshold

        Map<String, long[]> rgStats = new HashMap<>();
        rgStats.put(candidateFileId    + "_0", makeRgStats(100, 70));
        rgStats.put(nonCandidateFileId + "_0", makeRgStats(100, 20));

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(candidateFileId    + "_0", makeBitmap(100, 70));
        bitmaps.put(nonCandidateFileId + "_0", makeBitmap(100, 20));

        DirectScanStorageGC gc = new DirectScanStorageGC(
                retinaManager, 0.5, 10,
                Arrays.asList(
                        new FakeFileEntry(candidateFileId,    1, 1L, 0),
                        new FakeFileEntry(nonCandidateFileId, 1, 1L, 0)));

        gc.runStorageGC(200L, rgStats, bitmaps);

        assertTrue("candidate bitmap must be retained for S2",
                bitmaps.containsKey(candidateFileId + "_0"));
        assertFalse("non-candidate bitmap must be trimmed",
                bitmaps.containsKey(nonCandidateFileId + "_0"));
    }

}
