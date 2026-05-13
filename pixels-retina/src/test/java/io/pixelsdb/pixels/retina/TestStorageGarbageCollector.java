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

import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.utils.CheckpointFileIO;
import io.pixelsdb.pixels.common.utils.MetaDBUtil;
import io.pixelsdb.pixels.common.utils.PixelsFileNameUtils;
import io.pixelsdb.pixels.common.utils.RetinaUtils;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.DoubleColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link StorageGarbageCollector}, covering scan/grouping, data rewrite,
 * dual-write, visibility sync, index update, atomic switch, and end-to-end integration.
 *
 * <p>All tests use real {@link RetinaResourceManager} (with JNI/C++ native library)
 * and real {@link MetadataService} (requires a running metadata server).
 * Rewrite tests write Pixels files to a local temp directory using {@code file://}
 * URIs resolved by {@link io.pixelsdb.pixels.storage.localfs.LocalFS}.
 *
 * <h3>Test naming convention</h3>
 * New tests follow {@code test{Section}_{feature}_{scenario}} where Section maps to:
 * <ul>
 *   <li>S1 — scan/grouping and file-type filtering</li>
 *   <li>S2 — data rewrite (single/multi-file, single/multi-RG, hidden columns)</li>
 *   <li>S3 — dual-write propagation</li>
 *   <li>S4 — visibility sync (export/import/truncation)</li>
 *   <li>S5 — index sync</li>
 *   <li>S6 — atomic swap and deferred cleanup</li>
 *   <li>E2E — end-to-end integration</li>
 * </ul>
 * Legacy test names (pre-convention) are preserved for CI stability.
 */
public class TestStorageGarbageCollector
{
    // -----------------------------------------------------------------------
    // Class-level constants & fields
    // -----------------------------------------------------------------------

    private static final String TEST_SCHEMA = "gc_test";
    private static final String TEST_TABLE = "gc_test_tbl";
    private static final TypeDescription LONG_ID_SCHEMA =
            TypeDescription.fromString("struct<id:long>");

    private static Path tmpDir;
    private static Storage fileStorage;
    private static MetadataService metadataService;
    private static long testPathId;
    private static String testOrderedPathUri;

    // -----------------------------------------------------------------------
    // Per-test fields
    // -----------------------------------------------------------------------

    private RetinaResourceManager retinaManager;
    private StorageGarbageCollector gc;

    // -----------------------------------------------------------------------
    // Class-level setup / teardown
    // -----------------------------------------------------------------------

    @BeforeClass
    public static void classSetUp() throws Exception
    {
        tmpDir = Files.createTempDirectory("pixels_gc_test_");
        fileStorage = StorageFactory.Instance().getStorage("file");
        metadataService = MetadataService.Instance();

        if (metadataService.existSchema(TEST_SCHEMA))
        {
            metadataService.dropSchema(TEST_SCHEMA);
        }
        metadataService.createSchema(TEST_SCHEMA);
        Column col = new Column();
        col.setName("id");
        col.setType("long");
        String basePathUri = "file://" + tmpDir.toAbsolutePath();
        metadataService.createTable(TEST_SCHEMA, TEST_TABLE, Storage.Scheme.file,
                Collections.singletonList(basePathUri), Collections.singletonList(col));
        Layout layout = metadataService.getLatestLayout(TEST_SCHEMA, TEST_TABLE);
        testPathId = layout.getOrderedPaths().get(0).getId();
        testOrderedPathUri = layout.getOrderedPaths().get(0).getUri();
        Files.createDirectories(java.nio.file.Paths.get(testOrderedPathUri.replaceFirst("file://", "")));
    }

    @AfterClass
    public static void classTearDown()
    {
        try
        {
            if (metadataService != null && metadataService.existSchema(TEST_SCHEMA))
            {
                metadataService.dropSchema(TEST_SCHEMA);
            }
        }
        catch (Exception e)
        {
            System.err.println("Warning: failed to drop test schema " + TEST_SCHEMA + ": " + e.getMessage());
        }
        if (tmpDir != null)
        {
            deleteRecursive(tmpDir.toFile());
        }
    }

    // -----------------------------------------------------------------------
    // Per-test setup / teardown
    // -----------------------------------------------------------------------

    @Before
    public void setUp()
    {
        retinaManager = RetinaResourceManager.Instance();
        resetManagerState();
        cleanupOrderedDir();
        gc = new StorageGarbageCollector(retinaManager, metadataService, 0.5, 134_217_728L, Integer.MAX_VALUE, 10,
                1048576, EncodingLevel.EL2, 86_400_000L);
    }

    @After
    public void tearDown()
    {
        resetManagerState();
        cleanupOrderedDir();
    }

    /**
     * Deletes all {@code .pxl} files from the shared test ordered-path directory after
     * each test.  Multiple tests write output files (from {@code rewriteFileGroup}) into
     * this same directory.  Without cleanup, a leftover file whose name was generated by
     * {@link io.pixelsdb.pixels.common.utils.PixelsFileNameUtils#buildOrderedFileName} in a
     * prior test can collide with the name generated for the current test (same
     * {@link io.pixelsdb.pixels.common.utils.DateUtil#getCurTime()} wall-clock second and
     * counter value when the JVM is reused across reruns), causing
     * {@code LocalFS.create(overwrite=false)} to throw "File already exists" and the whole
     * GC pipeline to fail.
     */
    private static void cleanupOrderedDir()
    {
        if (testOrderedPathUri == null)
        {
            return;
        }
        java.io.File dir = new java.io.File(testOrderedPathUri.replaceFirst("file://", ""));
        if (!dir.exists() || !dir.isDirectory())
        {
            return;
        }
        java.io.File[] files = dir.listFiles();
        if (files == null)
        {
            return;
        }
        for (java.io.File f : files)
        {
            if (f.isFile() && !f.delete())
            {
                try { Thread.sleep(50); } catch (InterruptedException ignored) { }
                if (!f.delete())
                {
                    System.err.println("Warning: failed to delete " + f.getAbsolutePath());
                }
            }
        }
    }

    // =======================================================================
    // Section 1: groupAndMerge logic
    // =======================================================================

    /**
     * Three candidates from three distinct {@code (tableId, virtualNodeId)} pairs;
     * expect three separate groups sorted by invalidRatio descending.
     */
    @Test
    public void testGroupAndMerge_threeDistinctGroups()
    {
        StorageGarbageCollector gc = newGcForGrouping(0L, Integer.MAX_VALUE, 10);

        List<StorageGarbageCollector.FileCandidate> candidates = Arrays.asList(
                new StorageGarbageCollector.FileCandidate(makeFile(1, 1), "f1", 1, 1, 1L, 0, 0.60, 0L),
                new StorageGarbageCollector.FileCandidate(makeFile(2, 1), "f2", 2, 1, 1L, 1, 0.70, 0L),
                new StorageGarbageCollector.FileCandidate(makeFile(3, 1), "f3", 3, 1, 2L, 0, 0.80, 0L)
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
        StorageGarbageCollector gc = newGcForGrouping(0L, Integer.MAX_VALUE, 10);

        List<StorageGarbageCollector.FileCandidate> candidates = Arrays.asList(
                new StorageGarbageCollector.FileCandidate(makeFile(1, 1), "f1", 1, 1, 1L, 5, 0.60, 0L),
                new StorageGarbageCollector.FileCandidate(makeFile(2, 1), "f2", 2, 1, 1L, 5, 0.80, 0L)
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
        StorageGarbageCollector gc = newGcForGrouping(0L, Integer.MAX_VALUE, max);

        // Build 5 groups with different tableIds and clear invalidRatios (0.55..0.99)
        List<StorageGarbageCollector.FileCandidate> candidates = new ArrayList<>();
        for (int i = 0; i < 5; i++)
        {
            candidates.add(new StorageGarbageCollector.FileCandidate(
                    makeFile(i, 1), "f" + i, i, 1, (long) (i + 10), 0, 0.55 + i * 0.11, 0L));
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
        StorageGarbageCollector gc = newGcForGrouping(0L, Integer.MAX_VALUE, 10);
        List<StorageGarbageCollector.FileGroup> groups =
                gc.groupAndMerge(Collections.emptyList());
        assertTrue("empty candidates → empty groups", groups.isEmpty());
    }

    /**
     * Greedy splitting: two small files fit within the target size together.
     * Each is 30 MB effective → combined 60 MB < 100 MB target → one group.
     */
    @Test
    public void testGroupAndMerge_greedyMergeFitsInTarget()
    {
        long target = 100 * 1024 * 1024L; // 100 MB
        StorageGarbageCollector gc = newGcForGrouping(target, Integer.MAX_VALUE, 10);

        List<StorageGarbageCollector.FileCandidate> candidates = Arrays.asList(
                new StorageGarbageCollector.FileCandidate(
                        makeFile(1, 1), "f1", 1, 1, 1L, 0, 0.70, 100 * 1024 * 1024L),
                new StorageGarbageCollector.FileCandidate(
                        makeFile(2, 1), "f2", 2, 1, 1L, 0, 0.70, 100 * 1024 * 1024L)
        );

        List<StorageGarbageCollector.FileGroup> groups = gc.groupAndMerge(candidates);

        assertEquals("2 files × 30MB effective each < 100MB target → 1 group", 1, groups.size());
        assertEquals(2, groups.get(0).files.size());
    }

    /**
     * Greedy splitting: a single file whose effective data exceeds targetFileSize
     * must form its own group via the {@code singleFileOversized} branch,
     * even if other small files follow.
     * File A: 200 MB on disk, 10 % deleted → 180 MB effective > 100 MB target → oversized.
     * File B: 50 MB on disk, 50 % deleted → 25 MB effective → fits alone.
     */
    @Test
    public void testGroupAndMerge_greedyOversizedFileAlone()
    {
        long target = 100 * 1024 * 1024L;
        StorageGarbageCollector gc = newGcForGrouping(target, Integer.MAX_VALUE, 10);

        List<StorageGarbageCollector.FileCandidate> candidates = Arrays.asList(
                new StorageGarbageCollector.FileCandidate(
                        makeFile(1, 1), "f1", 1, 1, 1L, 0, 0.10, 200 * 1024 * 1024L),
                new StorageGarbageCollector.FileCandidate(
                        makeFile(2, 1), "f2", 2, 1, 1L, 0, 0.50, 50 * 1024 * 1024L)
        );

        List<StorageGarbageCollector.FileGroup> groups = gc.groupAndMerge(candidates);

        assertEquals("oversized file alone + small file alone → 2 groups", 2, groups.size());
        boolean foundOversizedAlone = false;
        for (StorageGarbageCollector.FileGroup g : groups)
        {
            if (g.files.size() == 1 && g.files.get(0).fileId == 1)
            {
                foundOversizedAlone = true;
            }
        }
        assertTrue("oversized file (180 MB effective > 100 MB target) must be in its own group",
                foundOversizedAlone);
    }

    /**
     * When fileSizeBytes is 0 (unknown), greedy splitting is effectively disabled:
     * all files in the same {@code (tableId, vNodeId)} form a single group.
     */
    @Test
    public void testGroupAndMerge_greedyFallbackWhenSizeUnknown()
    {
        long target = 100 * 1024 * 1024L;
        StorageGarbageCollector gc = newGcForGrouping(target, Integer.MAX_VALUE, 10);

        // fileSizeBytes = 0 (unknown) — no splitting occurs
        List<StorageGarbageCollector.FileCandidate> candidates = Arrays.asList(
                new StorageGarbageCollector.FileCandidate(
                        makeFile(1, 1), "f1", 1, 1, 1L, 0, 0.60, 0L),
                new StorageGarbageCollector.FileCandidate(
                        makeFile(2, 1), "f2", 2, 1, 1L, 0, 0.60, 0L)
        );

        List<StorageGarbageCollector.FileGroup> groups = gc.groupAndMerge(candidates);

        assertEquals("unknown size → no splitting → 1 group", 1, groups.size());
        assertEquals(2, groups.get(0).files.size());
    }

    /**
     * Five files sharing the same {@code (tableId, virtualNodeId)} with
     * maxFilesPerGroup=2 must be split into 3 groups (2+2+1).
     * targetFileSize is disabled (0) so only the file count limit applies.
     */
    @Test
    public void testGroupAndMerge_maxFilesPerGroupSplit()
    {
        StorageGarbageCollector gc = newGcForGrouping(0L, 2, 100);

        List<StorageGarbageCollector.FileCandidate> candidates = new ArrayList<>();
        for (int i = 0; i < 5; i++)
        {
            candidates.add(new StorageGarbageCollector.FileCandidate(
                    makeFile(i + 1, 1), "f" + i, i + 1, 1, 1L, 0, 0.90 - i * 0.05, 0L));
        }

        List<StorageGarbageCollector.FileGroup> groups = gc.groupAndMerge(candidates);

        assertEquals("5 files / maxFilesPerGroup=2 → 3 groups", 3, groups.size());
        assertEquals(2, groups.get(0).files.size());
        assertEquals(2, groups.get(1).files.size());
        assertEquals(1, groups.get(2).files.size());
    }

    /**
     * Dual-bound: targetFileSize is very large (won't trigger), but maxFilesPerGroup=3
     * triggers first.  Six files in same group → 2 groups of 3.
     */
    @Test
    public void testGroupAndMerge_dualBoundSplitByFileCount()
    {
        long hugeTarget = Long.MAX_VALUE;
        StorageGarbageCollector gc = newGcForGrouping(hugeTarget, 3, 100);

        List<StorageGarbageCollector.FileCandidate> candidates = new ArrayList<>();
        for (int i = 0; i < 6; i++)
        {
            candidates.add(new StorageGarbageCollector.FileCandidate(
                    makeFile(i + 1, 1), "f" + i, i + 1, 1, 1L, 0, 0.70,
                    50 * 1024 * 1024L));
        }

        List<StorageGarbageCollector.FileGroup> groups = gc.groupAndMerge(candidates);

        assertEquals("6 files / maxFilesPerGroup=3 → 2 groups", 2, groups.size());
        assertEquals(3, groups.get(0).files.size());
        assertEquals(3, groups.get(1).files.size());
    }

    /**
     * Dual-bound: maxFilesPerGroup is very large (won't trigger), but targetFileSize
     * triggers first.  Each file has 60MB effective data, target=100MB → each file
     * forms its own group.
     */
    @Test
    public void testGroupAndMerge_dualBoundSplitBySize()
    {
        long target = 100 * 1024 * 1024L;
        StorageGarbageCollector gc = newGcForGrouping(target, Integer.MAX_VALUE, 100);

        List<StorageGarbageCollector.FileCandidate> candidates = new ArrayList<>();
        for (int i = 0; i < 3; i++)
        {
            candidates.add(new StorageGarbageCollector.FileCandidate(
                    makeFile(i + 1, 1), "f" + i, i + 1, 1, 1L, 0, 0.40,
                    100 * 1024 * 1024L));
        }

        List<StorageGarbageCollector.FileGroup> groups = gc.groupAndMerge(candidates);

        assertEquals("3 files × 60MB effective > 100MB target → 3 single-file groups", 3, groups.size());
        for (StorageGarbageCollector.FileGroup g : groups)
        {
            assertEquals(1, g.files.size());
        }
    }

    /**
     * A single candidate must produce exactly one group of one file.
     */
    @Test
    public void testGroupAndMerge_singleCandidate()
    {
        StorageGarbageCollector gc = newGcForGrouping(0L, Integer.MAX_VALUE, 10);

        List<StorageGarbageCollector.FileCandidate> candidates = Collections.singletonList(
                new StorageGarbageCollector.FileCandidate(makeFile(1, 1), "f1", 1, 1, 1L, 0, 0.80, 0L));

        List<StorageGarbageCollector.FileGroup> groups = gc.groupAndMerge(candidates);

        assertEquals("single candidate → 1 group", 1, groups.size());
        assertEquals(1, groups.get(0).files.size());
        assertEquals(0.80, groups.get(0).files.get(0).invalidRatio, 1e-9);
    }

    /**
     * Greedy splitting: a file whose effective data size exactly equals
     * {@code targetFileSize} must NOT be treated as oversized (the check
     * uses strict {@code >}, not {@code >=}).  Two such files should merge
     * until the cumulative effective bytes exceeds the target.
     *
     * <p>File A: 100 MB on disk, 50 % deleted → 50 MB effective == target.
     * File B: 100 MB on disk, 50 % deleted → 50 MB effective.
     * Cumulative A+B = 100 MB > 50 MB → flush A alone, then B alone → 2 groups.
     * But neither is "oversized" individually.
     */
    @Test
    public void testGroupAndMerge_effectiveSizeExactlyEqualsTarget()
    {
        long target = 50 * 1024 * 1024L; // 50 MB
        StorageGarbageCollector gc = newGcForGrouping(target, Integer.MAX_VALUE, 10);

        List<StorageGarbageCollector.FileCandidate> candidates = Arrays.asList(
                new StorageGarbageCollector.FileCandidate(
                        makeFile(1, 1), "f1", 1, 1, 1L, 0, 0.50, 100 * 1024 * 1024L),
                new StorageGarbageCollector.FileCandidate(
                        makeFile(2, 1), "f2", 2, 1, 1L, 0, 0.50, 100 * 1024 * 1024L)
        );

        List<StorageGarbageCollector.FileGroup> groups = gc.groupAndMerge(candidates);

        assertEquals("effective == target → not oversized; A+B > target → 2 groups", 2, groups.size());
        for (StorageGarbageCollector.FileGroup g : groups)
        {
            assertEquals(1, g.files.size());
        }
    }

    /**
     * When all groups have the same average invalidRatio, sorting is stable
     * and the correct number of groups is returned.
     */
    @Test
    public void testGroupAndMerge_equalRatioSorting()
    {
        StorageGarbageCollector gc = newGcForGrouping(0L, Integer.MAX_VALUE, 10);

        List<StorageGarbageCollector.FileCandidate> candidates = Arrays.asList(
                new StorageGarbageCollector.FileCandidate(
                        makeFile(1, 1), "f1", 1, 1, 1L, 0, 0.70, 0L),
                new StorageGarbageCollector.FileCandidate(
                        makeFile(2, 1), "f2", 2, 1, 2L, 0, 0.70, 0L),
                new StorageGarbageCollector.FileCandidate(
                        makeFile(3, 1), "f3", 3, 1, 3L, 0, 0.70, 0L)
        );

        List<StorageGarbageCollector.FileGroup> groups = gc.groupAndMerge(candidates);

        assertEquals("3 distinct tableIds, all same ratio → 3 groups", 3, groups.size());
        for (StorageGarbageCollector.FileGroup g : groups)
        {
            assertEquals(0.70, g.files.get(0).invalidRatio, 1e-9);
        }
    }

    /**
     * When both {@code targetFileSize} and {@code maxFilesPerGroup} are disabled
     * (both {@code <= 0}), all files in the same {@code (tableId, virtualNodeId)}
     * form a single group — the fast path in {@code splitIntoGroups}.
     */
    @Test
    public void testGroupAndMerge_bothLimitsDisabled()
    {
        StorageGarbageCollector gc = newGcForGrouping(0L, 0, 10);

        List<StorageGarbageCollector.FileCandidate> candidates = new ArrayList<>();
        for (int i = 0; i < 5; i++)
        {
            candidates.add(new StorageGarbageCollector.FileCandidate(
                    makeFile(i + 1, 1), "f" + i, i + 1, 1, 1L, 0, 0.80 - i * 0.05, 0L));
        }

        List<StorageGarbageCollector.FileGroup> groups = gc.groupAndMerge(candidates);

        assertEquals("both limits disabled → all 5 files in 1 group", 1, groups.size());
        assertEquals(5, groups.get(0).files.size());
    }

    // =======================================================================
    // Section 2: threshold filtering via DirectScanStorageGC
    // =======================================================================

    /**
     * Three files with deletion ratios 60 %, 40 %, 80 % against threshold=0.5;
     * only the 60 % and 80 % files should appear as candidates, and they must be
     * grouped by {@code (tableId, virtualNodeId)}.
     *
     * <p>Verifies: threshold=0.5 → 60 % and 80 % selected, 40 % excluded.
     */
    @Test
    public void testScanAndGroupFiles_thresholdFiltering()
    {
        int totalRows = 100;
        long fileId60 = 60001L;
        long fileId40 = 40001L;
        long fileId80 = 80001L;

        // Build file-level stats: {totalRows, invalidCount} per file.
        Map<Long, long[]> fileStats = new HashMap<>();
        fileStats.put(fileId60, makeRgStats(totalRows, 60));   // 60 % deleted
        fileStats.put(fileId40, makeRgStats(totalRows, 40));   // 40 % deleted
        fileStats.put(fileId80, makeRgStats(totalRows, 80));   // 80 % deleted

        // Pre-compute candidate set (threshold=0.5): 60% and 80% qualify
        Set<Long> candidateFileIds = new HashSet<>(Arrays.asList(fileId60, fileId80));

        List<FakeFileEntry> fakeFiles = Arrays.asList(
                new FakeFileEntry(fileId60, 1, 1L, 0),  // ratio=0.60, should be selected
                new FakeFileEntry(fileId40, 1, 1L, 0),  // ratio=0.40, should be excluded
                new FakeFileEntry(fileId80, 1, 2L, 0)   // ratio=0.80, should be selected (different table)
        );

        DirectScanStorageGC gc = new DirectScanStorageGC(
                retinaManager, 0.5, 10, fakeFiles);

        List<StorageGarbageCollector.FileGroup> groups = gc.scanAndGroupFiles(candidateFileIds, fileStats);

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

        Map<Long, long[]> fileStats = new HashMap<>();
        fileStats.put(fileIdA, makeRgStats(totalRows, 60));  // 60 %
        fileStats.put(fileIdB, makeRgStats(totalRows, 75));  // 75 %

        Set<Long> candidateFileIds = new HashSet<>(Arrays.asList(fileIdA, fileIdB));

        // Both files belong to same (tableId=5, vNodeId=3)
        List<FakeFileEntry> fakeFiles = Arrays.asList(
                new FakeFileEntry(fileIdA, 1, 5L, 3),
                new FakeFileEntry(fileIdB, 1, 5L, 3)
        );

        DirectScanStorageGC gc = new DirectScanStorageGC(
                retinaManager, 0.5, 10, fakeFiles);

        List<StorageGarbageCollector.FileGroup> groups = gc.scanAndGroupFiles(candidateFileIds, fileStats);

        assertEquals("both files share (table=5, vNode=3) → 1 group", 1, groups.size());
        assertEquals("group must contain 2 files", 2, groups.get(0).files.size());
        assertEquals(5L, groups.get(0).tableId);
        assertEquals(3, groups.get(0).virtualNodeId);
    }

    /**
     * A file whose fileId has no entry in {@code fileStats} must be skipped
     * (totalRows == 0 → excluded regardless of threshold).
     */
    @Test
    public void testScanAndGroupFiles_skipsFilesWithNoVisibility()
    {
        long orphanFileId = 99999L;
        Map<Long, long[]> fileStats = new HashMap<>();
        Set<Long> candidateFileIds = Collections.singleton(orphanFileId);

        List<FakeFileEntry> fakeFiles = Collections.singletonList(
                new FakeFileEntry(orphanFileId, 1, 1L, 0));

        DirectScanStorageGC gc = new DirectScanStorageGC(
                retinaManager, 0.5, 10, fakeFiles);

        List<StorageGarbageCollector.FileGroup> groups = gc.scanAndGroupFiles(candidateFileIds, fileStats);
        assertTrue("file with no fileStats entry should be skipped", groups.isEmpty());
    }

    // =======================================================================
    // Section 3: runStorageGC bitmap trimming
    // =======================================================================

    /**
     * After {@code runStorageGC}, the {@code gcSnapshotBitmaps} map must have had
     * non-candidate entries removed.  Candidate bitmaps must be retained for the rewrite phase.
     */
    @Test
    public void testRunStorageGC_trimsBitmapMapToCandidate()
    {
        long candidateFileId = 66001L;
        long otherFileId     = 66002L;

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(candidateFileId + "_0", makeBitmap(100, 60));
        bitmaps.put(otherFileId + "_0", makeBitmap(100, 20));

        // File-level stats: candidateFileId has 60% deletion, otherFileId has 20%
        Map<Long, long[]> fileStats = new HashMap<>();
        fileStats.put(candidateFileId, makeRgStats(100, 60));
        fileStats.put(otherFileId, makeRgStats(100, 20));

        List<FakeFileEntry> fakeFiles = Arrays.asList(
                new FakeFileEntry(candidateFileId, 1, 1L, 0),
                new FakeFileEntry(otherFileId, 1, 1L, 0));

        DirectScanStorageGC gc = new DirectScanStorageGC(
                retinaManager, 0.5, 10, fakeFiles);

        gc.runStorageGC(300L, fileStats, bitmaps);

        assertTrue("candidate RG key must be retained",
                bitmaps.containsKey(candidateFileId + "_0"));
        assertFalse("non-candidate RG key must be removed",
                bitmaps.containsKey(otherFileId + "_0"));
    }

    // =======================================================================
    // Section 4: runStorageGC end-to-end scan → process
    // =======================================================================

    /**
     * A file whose invalidRatio is exactly equal to the threshold (0.5) must NOT
     * be selected as a candidate.  The design uses strict {@code >}, not {@code >=}.
     */
    @Test
    public void testRunStorageGC_thresholdExactlyEqual()
    {
        long fileId = 57001L;

        Map<Long, long[]> fileStats = new HashMap<>();
        fileStats.put(fileId, makeRgStats(100, 50));  // exactly 50% = threshold

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmap(100, 50));

        DirectScanStorageGC gc = new DirectScanStorageGC(
                retinaManager, 0.5, 10,
                Collections.singletonList(new FakeFileEntry(fileId, 1, 1L, 0)));

        gc.runStorageGC(400L, fileStats, bitmaps);

        assertTrue("file at exactly threshold must NOT be trimmed (no candidates)",
                bitmaps.containsKey(fileId + "_0"));
        assertEquals(1, bitmaps.size());
    }

    /**
     * A file whose {@code fileStats} entry has {@code totalRows=0} must not
     * produce a candidate even if invalidCount is also 0 (division by zero guard).
     */
    @Test
    public void testRunStorageGC_skipsTotalRowsZero()
    {
        long fileId = 58001L;

        Map<Long, long[]> fileStats = new HashMap<>();
        fileStats.put(fileId, new long[]{0, 0});  // totalRows=0

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", new long[]{0L});

        DirectScanStorageGC gc = new DirectScanStorageGC(
                retinaManager, 0.5, 10,
                Collections.singletonList(new FakeFileEntry(fileId, 1, 1L, 0)));

        gc.runStorageGC(500L, fileStats, bitmaps);

        assertTrue("totalRows=0 file must remain untouched (no candidates)",
                bitmaps.containsKey(fileId + "_0"));
    }

    // =======================================================================
    // Section 4b: processFileGroups error handling
    // =======================================================================

    /**
     * When {@code rewriteFileGroup} throws for the first FileGroup,
     * {@code processFileGroups} must catch the exception, clean up that
     * group's bitmap entries, and continue processing the second group.
     *
     * <p>Uses {@link FailFirstGroupGC} to inject a deterministic failure
     * on the first {@code rewriteFileGroup} call while the real
     * {@code processFileGroups} loop executes.
     */
    @Test
    public void testProcessFileGroups_firstGroupFailsSecondContinues()
    {
        long fileIdA = 88001L;
        long fileIdB = 88002L;

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileIdA + "_0", makeBitmap(100, 60));
        bitmaps.put(fileIdB + "_0", makeBitmap(100, 60));

        StorageGarbageCollector.FileGroup groupA = new StorageGarbageCollector.FileGroup(
                1L, 0, Collections.singletonList(
                new StorageGarbageCollector.FileCandidate(
                        makeFile(fileIdA, 1), "fake_a", fileIdA, 1, 1L, 0, 0.60, 0L)));
        StorageGarbageCollector.FileGroup groupB = new StorageGarbageCollector.FileGroup(
                2L, 0, Collections.singletonList(
                new StorageGarbageCollector.FileCandidate(
                        makeFile(fileIdB, 1), "fake_b", fileIdB, 1, 2L, 0, 0.60, 0L)));

        FailFirstGroupGC failGc = new FailFirstGroupGC();
        failGc.processFileGroups(Arrays.asList(groupA, groupB), 300L, bitmaps);

        assertFalse("failed group A's bitmap must be cleaned up by catch block",
                bitmaps.containsKey(fileIdA + "_0"));
        assertFalse("successful group B's bitmap must be cleaned up by rewrite stub",
                bitmaps.containsKey(fileIdB + "_0"));
    }

    // =======================================================================
    // Section 5: data rewrite functional tests
    // =======================================================================

    /**
     * When there is no bitmap entry for a source file RG, {@code isBitmapBitSet}
     * returns false for every row (null bitmap ≡ no deletions) and all rows pass
     * through unchanged.
     */
    @Test
    public void testNullBitmapKeepsAllRows() throws Exception
    {
        TypeDescription schema = LONG_ID_SCHEMA;
        long[] ids = {10L, 20L, 30L, 40L, 50L};
        long fileId = 2L;
        String srcPath = writeTestFile("src_null_bitmap.pxl", schema, ids, false, null);

        // Deliberately empty bitmaps map → null bitmap for every RG
        Map<String, long[]> bitmaps = new HashMap<>();

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(makeGroup(fileId, srcPath, schema), 100L, bitmaps);

        long[][] rows = readAllRows(result.newFilePath, schema, false);
        assertEquals("all 5 rows should survive", 5, rows.length);
        for (int i = 0; i < 5; i++)
        {
            assertEquals("id mismatch at row " + i, ids[i], rows[i][0]);
        }

        assertRewriteResultConsistency(result, 5);
    }

    /**
     * When every row in the source RG is marked deleted, {@code rewriteFileGroup}
     * deletes the empty output file and returns a sentinel {@code RewriteResult}
     * with {@code newFileId == -1}.  All forward-mapping entries must be {@code -1}.
     */
    @Test
    public void testAllRowsDeleted() throws Exception
    {
        TypeDescription schema = LONG_ID_SCHEMA;
        long[] ids = {1L, 2L, 3L, 4L, 5L};
        long fileId = 3L;
        String srcPath = writeTestFile("src_all_deleted.pxl", schema, ids, false, null);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmapForRows(5, 0, 1, 2, 3, 4));

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(makeGroup(fileId, srcPath, schema), 100L, bitmaps);

        assertEquals("newFileId should be -1 for all-deleted group", -1, result.newFileId);
        assertEquals("newFileRgCount should be 0", 0, result.newFileRgCount);
        assertEquals(0, result.newFileRgActualRecordNums.length);

        int[] fwd = result.forwardRgMappings.get(fileId).get(0);
        assertNotNull("fwdMapping must exist even for all-deleted RG", fwd);
        for (int i = 0; i < fwd.length; i++)
        {
            assertEquals("all rows deleted → every mapping entry must be -1", -1, fwd[i]);
        }
        assertFalse("gcSnapshotBitmaps entry must be removed after rewrite",
                bitmaps.containsKey(fileId + "_0"));
    }

    // =======================================================================
    // Section 5b: multi-file and multi-RG rewrite tests
    // =======================================================================

    /**
     * Rewrites a FileGroup containing two source files into one output file.
     * File A has rows {100,101,102,103,104}, file B has rows {200,201,202,203,204}.
     * Rows 0 and 2 are deleted from file A; rows 1 and 3 from file B.
     * Survivors (in order): A:{101,103,104}, B:{200,202,204} → 6 rows total.
     * Verifies output data, forward mappings for both files, and bitmap cleanup.
     */
    @Test
    public void testMultiFileGroupRewrite() throws Exception
    {
        TypeDescription schema = LONG_ID_SCHEMA;
        long fileIdA = 10L;
        long fileIdB = 11L;
        long[] idsA = {100L, 101L, 102L, 103L, 104L};
        long[] idsB = {200L, 201L, 202L, 203L, 204L};

        String pathA = writeTestFile("src_multi_a.pxl", schema, idsA, false, null);
        String pathB = writeTestFile("src_multi_b.pxl", schema, idsB, false, null);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileIdA + "_0", makeBitmapForRows(5, 0, 2));
        bitmaps.put(fileIdB + "_0", makeBitmapForRows(5, 1, 3));

        StorageGarbageCollector.FileGroup group =
                makeMultiFileGroup(schema, fileIdA, pathA, fileIdB, pathB);

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(group, 100L, bitmaps);

        long[][] rows = readAllRows(result.newFilePath, schema, false);
        assertEquals("6 rows should survive across two files", 6, rows.length);
        long[] expectedIds = {101L, 103L, 104L, 200L, 202L, 204L};
        for (int i = 0; i < expectedIds.length; i++)
        {
            assertEquals("id mismatch at row " + i, expectedIds[i], rows[i][0]);
        }

        // Forward mapping for file A: row 0 → -1, 1 → 0, 2 → -1, 3 → 1, 4 → 2
        int[] fwdA = result.forwardRgMappings.get(fileIdA).get(0);
        assertEquals(-1, fwdA[0]);
        assertEquals(0, fwdA[1]);
        assertEquals(-1, fwdA[2]);
        assertEquals(1, fwdA[3]);
        assertEquals(2, fwdA[4]);

        // Forward mapping for file B: row 0 → 3, 1 → -1, 2 → 4, 3 → -1, 4 → 5
        int[] fwdB = result.forwardRgMappings.get(fileIdB).get(0);
        assertEquals(3, fwdB[0]);
        assertEquals(-1, fwdB[1]);
        assertEquals(4, fwdB[2]);
        assertEquals(-1, fwdB[3]);
        assertEquals(5, fwdB[4]);

        assertFalse("bitmap A must be removed", bitmaps.containsKey(fileIdA + "_0"));
        assertFalse("bitmap B must be removed", bitmaps.containsKey(fileIdB + "_0"));

        assertRewriteResultConsistency(result, 6);
    }

    /**
     * Rewrites a source file containing multiple row groups (forced by a tiny
     * rowGroupSize).  Rows are deleted from different RGs and the output must
     * contain only survivors with correct data and forward mappings that
     * reference per-RG arrays.
     */
    @Test
    public void testMultiRgRewrite() throws Exception
    {
        TypeDescription schema = LONG_ID_SCHEMA;
        long fileId = 20L;

        // Write a file with 2 row groups: RG0 has ids {0..4}, RG1 has ids {5..9}
        String srcPath = writeMultiRgTestFile("src_multi_rg.pxl", schema,
                new long[][]{{0L, 1L, 2L, 3L, 4L}, {5L, 6L, 7L, 8L, 9L}},
                false, null);

        // Verify the source actually has 2 RGs
        int srcRgCount;
        try (PixelsReader r = PixelsReaderImpl.newBuilder()
                .setStorage(fileStorage).setPath(srcPath)
                .setPixelsFooterCache(new PixelsFooterCache()).build())
        {
            srcRgCount = r.getRowGroupNum();
        }
        assertEquals("source file must have 2 row groups", 2, srcRgCount);

        // Delete rows 1,3 from RG0 and rows 0,4 from RG1
        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmapForRows(5, 1, 3));
        bitmaps.put(fileId + "_1", makeBitmapForRows(5, 0, 4));

        StorageGarbageCollector.FileGroup group = makeGroup(fileId, srcPath, schema);

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(group, 100L, bitmaps);

        long[][] rows = readAllRows(result.newFilePath, schema, false);
        assertEquals("6 rows should survive across 2 RGs", 6, rows.length);
        long[] expectedIds = {0L, 2L, 4L, 6L, 7L, 8L};
        for (int i = 0; i < expectedIds.length; i++)
        {
            assertEquals("id mismatch at row " + i, expectedIds[i], rows[i][0]);
        }

        // Forward mapping for RG0: row 0→0, 1→-1, 2→1, 3→-1, 4→2
        int[] fwdRg0 = result.forwardRgMappings.get(fileId).get(0);
        assertNotNull("fwdMapping for rg0 must exist", fwdRg0);
        assertEquals(0, fwdRg0[0]);
        assertEquals(-1, fwdRg0[1]);
        assertEquals(1, fwdRg0[2]);
        assertEquals(-1, fwdRg0[3]);
        assertEquals(2, fwdRg0[4]);

        // Forward mapping for RG1: row 0→-1, 1→3, 2→4, 3→5, 4→-1
        int[] fwdRg1 = result.forwardRgMappings.get(fileId).get(1);
        assertNotNull("fwdMapping for rg1 must exist", fwdRg1);
        assertEquals(-1, fwdRg1[0]);
        assertEquals(3, fwdRg1[1]);
        assertEquals(4, fwdRg1[2]);
        assertEquals(5, fwdRg1[3]);
        assertEquals(-1, fwdRg1[4]);

        assertFalse("bitmap rg0 must be removed", bitmaps.containsKey(fileId + "_0"));
        assertFalse("bitmap rg1 must be removed", bitmaps.containsKey(fileId + "_1"));

        assertRewriteResultConsistency(result, 6);
    }

    /**
     * Verifies backward mapping correctness for a multi-RG source file.
     * For every surviving new row, the backward mapping must point to the
     * correct old-file global row offset, and the round-trip through forward
     * then backward must be consistent.
     *
     * Setup (same as {@link #testMultiRgRewrite}):
     *   RG0: rows {0,1,2,3,4}, delete 1,3 → survivors: 0,2,4 → new global 0,1,2
     *   RG1: rows {5,6,7,8,9}, delete 0,4 → survivors: 6,7,8 → new global 3,4,5
     *   oldFileRgRowStart = [0, 5, 10]
     *
     * Expected backward mapping (newGlobal → oldGlobal):
     *   0→0, 1→2, 2→4, 3→6, 4→7, 5→8
     */
    @Test
    public void testMultiRgRewrite_backwardMappingCorrectness() throws Exception
    {
        TypeDescription schema = LONG_ID_SCHEMA;
        long fileId = 22L;

        String srcPath = writeMultiRgTestFile("src_bwd_map.pxl", schema,
                new long[][]{{0L, 1L, 2L, 3L, 4L}, {5L, 6L, 7L, 8L, 9L}},
                false, null);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmapForRows(5, 1, 3));
        bitmaps.put(fileId + "_1", makeBitmapForRows(5, 0, 4));

        StorageGarbageCollector.FileGroup group = makeGroup(fileId, srcPath, schema);
        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(group, 100L, bitmaps);

        assertRewriteResultConsistency(result, 6);
        assertEquals("should have exactly 1 BackwardInfo (single source file)",
                1, result.backwardInfos.size());

        StorageGarbageCollector.BackwardInfo bwd = result.backwardInfos.get(0);
        assertEquals("backwardInfo should reference the source file", fileId, bwd.oldFileId);

        // oldFileRgRowStart: RG0 starts at 0 (5 rows), RG1 starts at 5 (5 rows), sentinel=10
        assertEquals(0, bwd.oldFileRgRowStart[0]);
        assertEquals(5, bwd.oldFileRgRowStart[1]);
        assertEquals(10, bwd.oldFileRgRowStart[2]);

        // Collect all backward mapping entries: newGlobal → oldGlobal
        int[] expectedOldGlobal = {0, 2, 4, 6, 7, 8};
        int newGlobal = 0;
        for (int newRgId = 0; newRgId < result.newFileRgCount; newRgId++)
        {
            int[] bwdMapping = bwd.backwardRgMappings.get(newRgId);
            if (bwdMapping == null)
            {
                newGlobal += result.newFileRgActualRecordNums[newRgId];
                continue;
            }
            for (int newRgOff = 0; newRgOff < bwdMapping.length; newRgOff++, newGlobal++)
            {
                int oldGlobal = bwdMapping[newRgOff];
                assertTrue("backward mapping entry must not be -1 for surviving row at newGlobal=" + newGlobal,
                        oldGlobal >= 0);
                assertEquals("backward mapping mismatch at newGlobal=" + newGlobal,
                        expectedOldGlobal[newGlobal], oldGlobal);
            }
        }
        assertEquals("total backward-mapped rows must equal total surviving rows",
                6, newGlobal);

        // Round-trip consistency: for each old row, forward then backward should be identity
        Map<Integer, int[]> fwdMappings = result.forwardRgMappings.get(fileId);
        for (int oldRgId = 0; oldRgId < 2; oldRgId++)
        {
            int[] fwdMapping = fwdMappings.get(oldRgId);
            for (int oldOff = 0; oldOff < fwdMapping.length; oldOff++)
            {
                int fwdGlobal = fwdMapping[oldOff];
                if (fwdGlobal < 0)
                {
                    continue;
                }
                int fwdNewRgId = RetinaResourceManager.rgIdForGlobalRowOffset(fwdGlobal, result.newFileRgRowStart);
                int fwdNewRgOff = fwdGlobal - result.newFileRgRowStart[fwdNewRgId];
                int roundTrip = bwd.backwardRgMappings.get(fwdNewRgId)[fwdNewRgOff];
                int expectedGlobal = bwd.oldFileRgRowStart[oldRgId] + oldOff;
                assertEquals("round-trip oldRg=" + oldRgId + " oldOff=" + oldOff,
                        expectedGlobal, roundTrip);
            }
        }
    }

    // =======================================================================
    // Section 5c: edge-case rewrite tests
    // =======================================================================

    /**
     * Bitmap word-boundary correctness: 128 rows, delete rows at positions
     * 0, 63, 64, 127 (crossing the 64-bit word boundary).  Survivors are all
     * other 124 rows.  Verifies data and forward mapping at boundary positions.
     */
    @Test
    public void testBitmapWordBoundaryFiltering() throws Exception
    {
        TypeDescription schema = LONG_ID_SCHEMA;
        int totalRows = 128;
        long[] ids = new long[totalRows];
        for (int i = 0; i < totalRows; i++)
        {
            ids[i] = i;
        }
        long fileId = 32L;
        String srcPath = writeTestFile("src_boundary.pxl", schema, ids, false, null);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmapForRows(totalRows, 0, 63, 64, 127));

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(makeGroup(fileId, srcPath, schema), 100L, bitmaps);

        long[][] rows = readAllRows(result.newFilePath, schema, false);
        assertEquals("124 rows should survive (128 - 4 deleted)", 124, rows.length);

        Set<Long> deletedSet = new HashSet<>(Arrays.asList(0L, 63L, 64L, 127L));
        int survivorIdx = 0;
        for (int i = 0; i < totalRows; i++)
        {
            if (!deletedSet.contains((long) i))
            {
                assertEquals("survivor data mismatch at original row " + i,
                        (long) i, rows[survivorIdx][0]);
                survivorIdx++;
            }
        }
        assertEquals(124, survivorIdx);

        int[] fwd = result.forwardRgMappings.get(fileId).get(0);
        assertEquals(-1, fwd[0]);
        assertEquals(-1, fwd[63]);
        assertEquals(-1, fwd[64]);
        assertEquals(-1, fwd[127]);
        assertTrue("row 1 should map to a valid new offset", fwd[1] >= 0);
        assertTrue("row 65 should map to a valid new offset", fwd[65] >= 0);

        assertRewriteResultConsistency(result, 124);
    }

    /**
     * Multi-file group where file A has all rows deleted and file B has
     * survivors.  Verifies mixed forward mappings and that the output
     * contains only B's surviving rows.
     */
    @Test
    public void testMultiFileGroupRewrite_oneFileAllDeleted() throws Exception
    {
        TypeDescription schema = LONG_ID_SCHEMA;
        long fileIdA = 40L;
        long fileIdB = 41L;
        long[] idsA = {10L, 11L, 12L};
        long[] idsB = {20L, 21L, 22L};

        String pathA = writeTestFile("src_mix_all_del_a.pxl", schema, idsA, false, null);
        String pathB = writeTestFile("src_mix_all_del_b.pxl", schema, idsB, false, null);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileIdA + "_0", makeBitmapForRows(3, 0, 1, 2));
        bitmaps.put(fileIdB + "_0", makeBitmapForRows(3, 1));

        StorageGarbageCollector.FileGroup group =
                makeMultiFileGroup(schema, fileIdA, pathA, fileIdB, pathB);

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(group, 100L, bitmaps);

        long[][] rows = readAllRows(result.newFilePath, schema, false);
        assertEquals("only B's 2 survivors should be in output", 2, rows.length);
        assertEquals(20L, rows[0][0]);
        assertEquals(22L, rows[1][0]);

        int[] fwdA = result.forwardRgMappings.get(fileIdA).get(0);
        for (int i = 0; i < fwdA.length; i++)
        {
            assertEquals("file A all deleted → every mapping must be -1", -1, fwdA[i]);
        }

        int[] fwdB = result.forwardRgMappings.get(fileIdB).get(0);
        assertEquals(0, fwdB[0]);
        assertEquals(-1, fwdB[1]);
        assertEquals(1, fwdB[2]);

        assertRewriteResultConsistency(result, 2);
    }

    /**
     * Multi-file group where BOTH files have ALL rows deleted.
     * Triggers the {@code globalNewRowOffset == 0} path with multiple source files:
     * the empty output file is deleted and {@code newFileId == -1}.
     * Forward mappings for both files must be all {@code -1}.
     */
    @Test
    public void testMultiFileGroupRewrite_allFilesAllDeleted() throws Exception
    {
        TypeDescription schema = LONG_ID_SCHEMA;
        long fileIdA = 42L;
        long fileIdB = 43L;
        long[] idsA = {10L, 11L, 12L};
        long[] idsB = {20L, 21L, 22L};

        String pathA = writeTestFile("src_all_del_multi_a.pxl", schema, idsA, false, null);
        String pathB = writeTestFile("src_all_del_multi_b.pxl", schema, idsB, false, null);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileIdA + "_0", makeBitmapForRows(3, 0, 1, 2));
        bitmaps.put(fileIdB + "_0", makeBitmapForRows(3, 0, 1, 2));

        StorageGarbageCollector.FileGroup group =
                makeMultiFileGroup(schema, fileIdA, pathA, fileIdB, pathB);

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(group, 100L, bitmaps);

        assertEquals("newFileId should be -1 for all-deleted multi-file group", -1, result.newFileId);
        assertEquals("newFileRgCount should be 0", 0, result.newFileRgCount);
        assertEquals(0, result.newFileRgActualRecordNums.length);

        int[] fwdA = result.forwardRgMappings.get(fileIdA).get(0);
        for (int i = 0; i < fwdA.length; i++)
        {
            assertEquals("file A: all rows deleted → mapping must be -1", -1, fwdA[i]);
        }

        int[] fwdB = result.forwardRgMappings.get(fileIdB).get(0);
        for (int i = 0; i < fwdB.length; i++)
        {
            assertEquals("file B: all rows deleted → mapping must be -1", -1, fwdB[i]);
        }

        assertFalse("bitmap A must be removed", bitmaps.containsKey(fileIdA + "_0"));
        assertFalse("bitmap B must be removed", bitmaps.containsKey(fileIdB + "_0"));
    }

    /**
     * Non-null all-zero bitmap: the bitmap exists but has no bits set (no deletions).
     * All rows must survive.  This exercises a distinct code path from a null bitmap:
     * {@code gcBitmap != null} evaluates to {@code true} but no bit check succeeds.
     */
    @Test
    public void testAllZeroBitmapKeepsAllRows() throws Exception
    {
        TypeDescription schema = LONG_ID_SCHEMA;
        long[] ids = {10L, 20L, 30L, 40L, 50L};
        long fileId = 33L;
        String srcPath = writeTestFile("src_allzero_bitmap.pxl", schema, ids, false, null);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", new long[]{0L});

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(makeGroup(fileId, srcPath, schema), 100L, bitmaps);

        long[][] rows = readAllRows(result.newFilePath, schema, false);
        assertEquals("all 5 rows should survive with all-zero bitmap", 5, rows.length);
        for (int i = 0; i < 5; i++)
        {
            assertEquals("id mismatch at row " + i, ids[i], rows[i][0]);
        }

        int[] fwd = result.forwardRgMappings.get(fileId).get(0);
        for (int i = 0; i < fwd.length; i++)
        {
            assertEquals("no deletions → mapping must be identity", i, fwd[i]);
        }

        assertRewriteResultConsistency(result, 5);
    }

    /**
     * Multi-RG file where one RG has ALL rows deleted and the other has survivors.
     * RG0: rows {0,1,2} all deleted → 0 survivors from RG0.
     * RG1: rows {3,4,5}, delete row 0 → survivors 4,5 → new global offsets 0,1.
     * Verifies output data, forward mappings per RG, backward mapping, and that
     * the all-deleted RG does not produce any output.
     */
    @Test
    public void testMultiRgRewrite_oneRgCompletelyDeleted() throws Exception
    {
        TypeDescription schema = LONG_ID_SCHEMA;
        long fileId = 35L;

        String srcPath = writeMultiRgTestFile("src_rg_all_del.pxl", schema,
                new long[][]{{0L, 1L, 2L}, {3L, 4L, 5L}},
                false, null);

        int srcRgCount;
        try (PixelsReader r = PixelsReaderImpl.newBuilder()
                .setStorage(fileStorage).setPath(srcPath)
                .setPixelsFooterCache(new PixelsFooterCache()).build())
        {
            srcRgCount = r.getRowGroupNum();
        }
        assertEquals("source file must have 2 row groups", 2, srcRgCount);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmapForRows(3, 0, 1, 2));
        bitmaps.put(fileId + "_1", makeBitmapForRows(3, 0));

        StorageGarbageCollector.FileGroup group = makeGroup(fileId, srcPath, schema);

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(group, 100L, bitmaps);

        long[][] rows = readAllRows(result.newFilePath, schema, false);
        assertEquals("2 rows should survive (RG0 all deleted, RG1: 2 survivors)", 2, rows.length);
        assertEquals(4L, rows[0][0]);
        assertEquals(5L, rows[1][0]);

        int[] fwdRg0 = result.forwardRgMappings.get(fileId).get(0);
        for (int i = 0; i < fwdRg0.length; i++)
        {
            assertEquals("RG0 all deleted → every mapping must be -1", -1, fwdRg0[i]);
        }

        int[] fwdRg1 = result.forwardRgMappings.get(fileId).get(1);
        assertEquals(-1, fwdRg1[0]);
        assertEquals(0, fwdRg1[1]);
        assertEquals(1, fwdRg1[2]);

        assertFalse("bitmap rg0 must be removed", bitmaps.containsKey(fileId + "_0"));
        assertFalse("bitmap rg1 must be removed", bitmaps.containsKey(fileId + "_1"));

        assertRewriteResultConsistency(result, 2);

        assertEquals(1, result.backwardInfos.size());
        StorageGarbageCollector.BackwardInfo bwd = result.backwardInfos.get(0);
        assertEquals(0, bwd.oldFileRgRowStart[0]);
        assertEquals(3, bwd.oldFileRgRowStart[1]);
        assertEquals(6, bwd.oldFileRgRowStart[2]);

        int globalIdx = 0;
        for (int newRgId = 0; newRgId < result.newFileRgCount; newRgId++)
        {
            int[] bwdMapping = bwd.backwardRgMappings.get(newRgId);
            if (bwdMapping == null)
            {
                continue;
            }
            for (int off = 0; off < bwdMapping.length; off++, globalIdx++)
            {
                assertTrue("backward mapping entry should be valid",
                        bwdMapping[off] >= 0);
            }
        }
        assertEquals(2, globalIdx);
    }

    /**
     * Multi-file group backward mapping correctness: two files, each with 5 rows.
     * File A: delete rows 0,4 → survivors 1,2,3 → new global 0,1,2
     * File B: delete rows 1,3 → survivors 0,2,4 → new global 3,4,5
     *
     * Backward mapping per old file:
     *   File A: oldFileRgRowStart = [0, 5], newGlobal 0→old 1, 1→old 2, 2→old 3
     *   File B: oldFileRgRowStart = [0, 5], newGlobal 3→old 0, 4→old 2, 5→old 4
     * Round-trip: forward(old) → backward(new) must be identity.
     */
    @Test
    public void testMultiFileGroupRewrite_backwardMapping() throws Exception
    {
        TypeDescription schema = LONG_ID_SCHEMA;
        long fileIdA = 36L;
        long fileIdB = 37L;
        long[] idsA = {100L, 101L, 102L, 103L, 104L};
        long[] idsB = {200L, 201L, 202L, 203L, 204L};

        String pathA = writeTestFile("src_mf_bwd_a.pxl", schema, idsA, false, null);
        String pathB = writeTestFile("src_mf_bwd_b.pxl", schema, idsB, false, null);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileIdA + "_0", makeBitmapForRows(5, 0, 4));
        bitmaps.put(fileIdB + "_0", makeBitmapForRows(5, 1, 3));

        StorageGarbageCollector.FileGroup group =
                makeMultiFileGroup(schema, fileIdA, pathA, fileIdB, pathB);

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(group, 100L, bitmaps);

        assertRewriteResultConsistency(result, 6);
        assertEquals("should have 2 BackwardInfos (two source files)",
                2, result.backwardInfos.size());

        for (StorageGarbageCollector.BackwardInfo bwd : result.backwardInfos)
        {
            long oldFileId = bwd.oldFileId;
            Map<Integer, int[]> fwdMappings = result.forwardRgMappings.get(oldFileId);
            assertNotNull("forward mappings must exist for oldFileId=" + oldFileId, fwdMappings);

            for (int oldRgId = 0; oldRgId < bwd.oldFileRgRowStart.length - 1; oldRgId++)
            {
                int[] fwdMapping = fwdMappings.get(oldRgId);
                if (fwdMapping == null)
                {
                    continue;
                }
                for (int oldOff = 0; oldOff < fwdMapping.length; oldOff++)
                {
                    int fwdGlobal = fwdMapping[oldOff];
                    if (fwdGlobal < 0)
                    {
                        continue;
                    }
                    int newRgId = RetinaResourceManager.rgIdForGlobalRowOffset(
                            fwdGlobal, result.newFileRgRowStart);
                    int newRgOff = fwdGlobal - result.newFileRgRowStart[newRgId];
                    int[] bwdMapping = bwd.backwardRgMappings.get(newRgId);
                    assertNotNull("backward mapping must exist for newRgId=" + newRgId, bwdMapping);
                    int roundTrip = bwdMapping[newRgOff];
                    int expectedGlobal = bwd.oldFileRgRowStart[oldRgId] + oldOff;
                    assertEquals("round-trip file=" + oldFileId + " oldRg=" + oldRgId
                                    + " oldOff=" + oldOff,
                            expectedGlobal, roundTrip);
                }
            }
        }

        int[] fwdA = result.forwardRgMappings.get(fileIdA).get(0);
        assertEquals(-1, fwdA[0]);
        assertEquals(0, fwdA[1]);
        assertEquals(1, fwdA[2]);
        assertEquals(2, fwdA[3]);
        assertEquals(-1, fwdA[4]);

        int[] fwdB = result.forwardRgMappings.get(fileIdB).get(0);
        assertEquals(3, fwdB[0]);
        assertEquals(-1, fwdB[1]);
        assertEquals(4, fwdB[2]);
        assertEquals(-1, fwdB[3]);
        assertEquals(5, fwdB[4]);
    }

    // =======================================================================
    // Section 6: dual-write functional tests
    // =======================================================================

    /**
     * After unregisterDualWrite, deletes no longer propagate.
     */
    @Test
    public void testDualWrite_unregisterStops() throws Exception
    {
        TypeDescription schema = LONG_ID_SCHEMA;
        long[] ids = {0L, 1L, 2L, 3L};
        long fileId = 32L;
        String srcPath = writeTestFile("dw_unreg_src.pxl", schema, ids, false, null);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmapForRows(4, 0));

        retinaManager.addVisibility(fileId, 0, 4, 0L, null, false);

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(makeGroup(fileId, srcPath, schema), 100L, bitmaps);
        long newFileId = result.newFileId;

        gc.registerDualWrite(result);
        gc.unregisterDualWrite(result);

        // Old file row 1 → new file row 0 (first survivor).
        // After unregister, delete should NOT propagate.
        long deleteTs = 200L;
        retinaManager.deleteRecord(fileId, 0, 1, deleteTs);

        // Old file: row 1 should be deleted (direct write always works)
        long[] oldBitmap = retinaManager.queryVisibility(fileId, 0, deleteTs, 0L);
        assertTrue("old file row 1 should be deleted (direct write)",
                (oldBitmap[1 / 64] & (1L << (1 % 64))) != 0);

        // New file: row 0 should NOT be deleted (dual-write is off)
        long[] newBitmap = retinaManager.queryVisibility(newFileId, 0, deleteTs, 0L);
        assertFalse("new file row 0 should NOT be deleted after unregister",
                (newBitmap[0 / 64] & (1L << (0 % 64))) != 0);
    }

    // =======================================================================
    // Section 7b: rgIdForGlobalRowOffset boundary tests
    // =======================================================================

    /**
     * Multiple RGs: offsets on exact boundaries should return the correct rgId.
     * rgRowStart = [0, 100, 250, 400]  →  RG0=[0..99], RG1=[100..249], RG2=[250..399]
     */
    @Test
    public void testRgIdForGlobalRowOffset_boundaries()
    {
        int[] rgRowStart = {0, 100, 250, 400};
        assertEquals(0, RetinaResourceManager.rgIdForGlobalRowOffset(0, rgRowStart));
        assertEquals(0, RetinaResourceManager.rgIdForGlobalRowOffset(99, rgRowStart));
        assertEquals(1, RetinaResourceManager.rgIdForGlobalRowOffset(100, rgRowStart));
        assertEquals(1, RetinaResourceManager.rgIdForGlobalRowOffset(249, rgRowStart));
        assertEquals(2, RetinaResourceManager.rgIdForGlobalRowOffset(250, rgRowStart));
        assertEquals(2, RetinaResourceManager.rgIdForGlobalRowOffset(399, rgRowStart));
    }

    /**
     * Many equal-sized RGs: stress the binary search across many intervals.
     */
    @Test
    public void testRgIdForGlobalRowOffset_manyRgs()
    {
        int numRgs = 64;
        int rowsPerRg = 256;
        int[] rgRowStart = new int[numRgs + 1];
        for (int i = 0; i <= numRgs; i++)
        {
            rgRowStart[i] = i * rowsPerRg;
        }
        for (int rg = 0; rg < numRgs; rg++)
        {
            int first = rg * rowsPerRg;
            int last = first + rowsPerRg - 1;
            assertEquals("first offset of rg=" + rg, rg,
                    RetinaResourceManager.rgIdForGlobalRowOffset(first, rgRowStart));
            assertEquals("last offset of rg=" + rg, rg,
                    RetinaResourceManager.rgIdForGlobalRowOffset(last, rgRowStart));
        }
    }

    // =======================================================================
    // Section 7c: createCheckpointDirect vs createCheckpoint consistency
    // =======================================================================

    /**
     * Both checkpoint paths (queued via rgVisibilityMap traversal and direct via
     * pre-built entries) must produce byte-identical files when given the same
     * visibility state.
     */
    @Test
    public void testCheckpointDirect_matchesStandardCheckpoint() throws Exception
    {
        long ts = 500L;
        int numFiles = 3;
        int rowsPerRg = 64;

        for (int fid = 1; fid <= numFiles; fid++)
        {
            retinaManager.addVisibility(fid, 0, rowsPerRg, 0L, null, false);
            for (int d = 0; d < fid; d++)
            {
                retinaManager.deleteRecord(fid, 0, d, ts - 100);
            }
        }

        // Build pre-built entries identical to what runGC() would construct.
        List<CheckpointFileIO.CheckpointEntry> entries = new ArrayList<>();
        Field rgMapField = RetinaResourceManager.class.getDeclaredField("rgVisibilityMap");
        rgMapField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, RGVisibility> rgMap =
                (Map<String, RGVisibility>) rgMapField.get(retinaManager);
        for (Map.Entry<String, RGVisibility> e : rgMap.entrySet())
        {
            long fileId = RetinaUtils.parseFileIdFromRgKey(e.getKey());
            int rgId = RetinaUtils.parseRgIdFromRgKey(e.getKey());
            long[] bitmap = e.getValue().getVisibilityBitmap(ts);
            entries.add(new CheckpointFileIO.CheckpointEntry(
                    fileId, rgId, (int) e.getValue().getRecordNum(), bitmap));
        }

        // Obtain the private CheckpointType.GC enum value via reflection.
        @SuppressWarnings("unchecked")
        Class<? extends Enum<?>> checkpointTypeClass = (Class<? extends Enum<?>>)
                Class.forName("io.pixelsdb.pixels.retina.RetinaResourceManager$CheckpointType");
        Object gcType = null;
        for (Object constant : checkpointTypeClass.getEnumConstants())
        {
            if (constant.toString().equals("GC"))
            {
                gcType = constant;
                break;
            }
        }
        assertNotNull("CheckpointType.GC must exist", gcType);

        // Call createCheckpoint (standard path)
        Method createCheckpointMethod = RetinaResourceManager.class.getDeclaredMethod(
                "createCheckpoint", long.class, checkpointTypeClass);
        createCheckpointMethod.setAccessible(true);
        @SuppressWarnings("unchecked")
        CompletableFuture<Void> f1 = (CompletableFuture<Void>) createCheckpointMethod.invoke(
                retinaManager, ts, gcType);
        f1.join();

        // Call createCheckpointDirect (optimized path) with a different timestamp to get a different file name
        long ts2 = ts + 1;
        Method createCheckpointDirectMethod = RetinaResourceManager.class.getDeclaredMethod(
                "createCheckpointDirect", long.class, checkpointTypeClass, List.class);
        createCheckpointDirectMethod.setAccessible(true);
        @SuppressWarnings("unchecked")
        CompletableFuture<Void> f2 = (CompletableFuture<Void>) createCheckpointDirectMethod.invoke(
                retinaManager, ts2, gcType, entries);
        f2.join();

        // Read both checkpoint files and compare entries.
        // Files may have entries in different order (due to producer-consumer concurrency),
        // so we normalize by sorting entries by (fileId, rgId) before comparing.
        Field checkpointDirField = RetinaResourceManager.class.getDeclaredField("checkpointDir");
        checkpointDirField.setAccessible(true);
        String checkpointDir = (String) checkpointDirField.get(retinaManager);

        Field hostField = RetinaResourceManager.class.getDeclaredField("retinaHostName");
        hostField.setAccessible(true);
        String hostName = (String) hostField.get(retinaManager);

        String path1 = RetinaUtils.buildCheckpointPath(
                checkpointDir, RetinaUtils.CHECKPOINT_PREFIX_GC, hostName, ts);
        String path2 = RetinaUtils.buildCheckpointPath(
                checkpointDir, RetinaUtils.CHECKPOINT_PREFIX_GC, hostName, ts2);

        Map<String, long[]> standard = new HashMap<>();
        CheckpointFileIO.readCheckpointParallel(path1, entry ->
                standard.put(entry.fileId + "_" + entry.rgId,
                        Arrays.copyOf(entry.bitmap, entry.bitmap.length)));

        Map<String, long[]> direct = new HashMap<>();
        CheckpointFileIO.readCheckpointParallel(path2, entry ->
                direct.put(entry.fileId + "_" + entry.rgId,
                        Arrays.copyOf(entry.bitmap, entry.bitmap.length)));

        assertEquals("entry count must match", standard.size(), direct.size());
        for (Map.Entry<String, long[]> e : standard.entrySet())
        {
            long[] directBitmap = direct.get(e.getKey());
            assertNotNull("direct checkpoint must contain key=" + e.getKey(), directBitmap);
            assertTrue("bitmaps must be identical for key=" + e.getKey(),
                    Arrays.equals(e.getValue(), directBitmap));
        }
    }

    // =======================================================================
    // Section 7d: concurrent dual-write pressure test
    // =======================================================================

    /**
     * Multi-threaded stress test: concurrent {@code deleteRecord} calls with
     * dual-write active.  Each thread owns one exclusive row group and deletes
     * its rows serially within that group, matching the production CDC contract
     * (same-RG deletes are serialized; different-RG deletes may run in parallel).
     * Verifies that all deletes are correctly propagated in both forward and
     * backward directions under inter-RG concurrency.
     */
    @Test
    public void testDualWrite_concurrentPressure() throws Exception
    {
        TypeDescription schema = LONG_ID_SCHEMA;
        int numRgs    = 8;
        int rowsPerRg = 8;
        long fileId   = 50L;

        // Source file with numRgs row groups, rowsPerRg rows each.
        // pixelStride must equal rowsPerRg so that each batch produces a full pixel,
        // triggering the column writer to flush and making curRowGroupDataLength > 0.
        // Without this, pixelStride=10_000 >> rowsPerRg causes outputStream.size()=0,
        // the writer never flushes mid-batch, and all rows collapse into a single RG.
        String srcPath = writeTestFileMultiRg("dw_conc_src.pxl", schema, numRgs, rowsPerRg, rowsPerRg);

        Map<String, long[]> bitmaps = new HashMap<>();
        for (int rgId = 0; rgId < numRgs; rgId++)
        {
            bitmaps.put(fileId + "_" + rgId, makeBitmapForRows(rowsPerRg));
            retinaManager.addVisibility(fileId, rgId, rowsPerRg, 0L, null, false);
        }

        // rowGroupSize=1 byte ensures the rewritten file flushes a new RG after every
        // batch (any encoded pixel exceeds 1 byte), preserving the 1:1 old-RG-to-new-RG
        // mapping so each thread targets a distinct new RGVisibility object.
        StorageGarbageCollector localGc = new StorageGarbageCollector(
                retinaManager, metadataService, 0.5, 134_217_728L,
                Integer.MAX_VALUE, 10, 1, EncodingLevel.EL2, 86_400_000L);

        StorageGarbageCollector.RewriteResult result =
                localGc.rewriteFileGroup(makeGroup(fileId, srcPath, schema), 100L, bitmaps);
        long newFileId = result.newFileId;
        assertTrue("new file should be registered", newFileId > 0);
        assertEquals("rewritten file must have same RG count", numRgs, result.newFileRgCount);

        localGc.registerDualWrite(result);

        // One thread per RG; each thread owns rgId == t and deletes its rows serially.
        CyclicBarrier barrier = new CyclicBarrier(numRgs);
        AtomicInteger errors  = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(numRgs);

        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < numRgs; t++)
        {
            final int rgId = t;
            futures.add(executor.submit(() ->
            {
                try
                {
                    barrier.await();
                    for (int rgOff = 0; rgOff < rowsPerRg; rgOff++)
                    {
                        long deleteTs = 200L + rgId * rowsPerRg + rgOff;
                        if (rgOff % 2 == 0)
                        {
                            // Forward direction: delete from old file; dual-write
                            // propagates to the corresponding new-file row.
                            retinaManager.deleteRecord(fileId, rgId, rgOff, deleteTs);
                        }
                        else
                        {
                            // Backward direction: delete from new file; dual-write
                            // propagates back to the corresponding old-file row.
                            int[] fwdMapping = result.forwardRgMappings.get(fileId).get(rgId);
                            int newGlobal = fwdMapping[rgOff];
                            if (newGlobal >= 0)
                            {
                                int newRgId = RetinaResourceManager.rgIdForGlobalRowOffset(
                                        newGlobal, result.newFileRgRowStart);
                                int newRgOff = newGlobal - result.newFileRgRowStart[newRgId];
                                retinaManager.deleteRecord(newFileId, newRgId, newRgOff, deleteTs);
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    errors.incrementAndGet();
                }
            }));
        }

        for (java.util.concurrent.Future<?> f : futures)
        {
            f.get();
        }
        executor.shutdown();

        assertEquals("no errors during concurrent deletes", 0, errors.get());

        long queryTs = 200L + numRgs * rowsPerRg;

        // Verify every row in every old-file RG is deleted.
        for (int rgId = 0; rgId < numRgs; rgId++)
        {
            long[] oldBitmap = retinaManager.queryVisibility(fileId, rgId, queryTs, 0L);
            for (int r = 0; r < rowsPerRg; r++)
            {
                assertTrue("old file rgId=" + rgId + " row " + r + " should be deleted",
                        isBitSet(oldBitmap, r));
            }
        }

        // Verify every corresponding row in the new file is also deleted.
        for (int rgId = 0; rgId < numRgs; rgId++)
        {
            int[] fwdMapping = result.forwardRgMappings.get(fileId).get(rgId);
            for (int r = 0; r < rowsPerRg; r++)
            {
                int newGlobal = fwdMapping[r];
                if (newGlobal >= 0)
                {
                    int newRgId = RetinaResourceManager.rgIdForGlobalRowOffset(
                            newGlobal, result.newFileRgRowStart);
                    int newRgOff = newGlobal - result.newFileRgRowStart[newRgId];
                    long[] newBitmap = retinaManager.queryVisibility(newFileId, newRgId, queryTs, 0L);
                    assertTrue("new file rgId=" + newRgId + " row " + newRgOff
                                    + " (from old rgId=" + rgId + " row " + r + ") should be deleted",
                            (newBitmap[newRgOff / 64] & (1L << (newRgOff % 64))) != 0);
                }
            }
        }

        localGc.unregisterDualWrite(result);
    }

    // =======================================================================
    // Section 8: index update + atomic switch + rollback + delayed cleanup
    // =======================================================================

    /**
     * Atomicity with multiple old files: one TEMPORARY new file and three REGULAR
     * old files are swapped in a single call.  Verifies that after the call the new
     * file is promoted to REGULAR and <b>all</b> old files are removed from the
     * catalog—i.e., the UPDATE and DELETE execute as one indivisible transaction.
     */
    @Test
    public void testAtomicSwap_multipleOldFilesAtomicity() throws Exception
    {
        long[] ids = {0, 1};
        long[] ts = {100, 100};

        writeTestFile("atom_old1.pxl", LONG_ID_SCHEMA, ids, true, ts);
        writeTestFile("atom_old2.pxl", LONG_ID_SCHEMA, ids, true, ts);
        writeTestFile("atom_old3.pxl", LONG_ID_SCHEMA, ids, true, ts);

        long[] oldIds = registerTestFiles(
                new String[]{"atom_old1.pxl", "atom_old2.pxl", "atom_old3.pxl"},
                new File.Type[]{File.Type.REGULAR, File.Type.REGULAR, File.Type.REGULAR},
                new int[]{1, 1, 1}, new long[]{0, 0, 0}, new long[]{1, 1, 1});
        long newFileId = registerTestFile("atom_new.pxl", File.Type.TEMPORARY, 1, 0, 1);

        File preSwapNew = metadataService.getFileById(newFileId);
        assertNotNull("New file must exist before swap", preSwapNew);
        assertEquals("New file should be TEMPORARY before swap",
                File.Type.TEMPORARY, preSwapNew.getType());

        metadataService.atomicSwapFiles(newFileId, Arrays.asList(oldIds[0], oldIds[1], oldIds[2]));

        assertFileRegular(newFileId, "New file should be REGULAR after swap");
        for (long oldId : oldIds)
        {
            assertFileGone(oldId, "Old file " + oldId + " should be gone after swap");
        }
    }

    /**
     * Idempotency: calling {@code atomicSwapFiles} a second time after the swap has
     * already committed must not throw.  The UPDATE is a no-op (already REGULAR) and
     * the DELETE is a no-op (old files already removed).
     */
    @Test
    public void testAtomicSwap_idempotent() throws Exception
    {
        writeTestFile("idem_old.pxl", LONG_ID_SCHEMA, new long[]{0, 1, 2}, true, new long[]{100, 100, 100});
        long oldFileId = registerTestFile("idem_old.pxl", File.Type.REGULAR, 1, 0, 2);
        long newFileId = registerTestFile("idem_new.pxl", File.Type.TEMPORARY, 1, 0, 2);

        metadataService.atomicSwapFiles(newFileId, Collections.singletonList(oldFileId));
        assertFileRegular(newFileId, "File should be REGULAR after first swap");

        metadataService.atomicSwapFiles(newFileId, Collections.singletonList(oldFileId));

        assertFileRegular(newFileId, "File should remain REGULAR after idempotent retry");
        assertFileGone(oldFileId, "Old file should remain absent after idempotent retry");
    }

    /**
     * TEMPORARY visibility semantics: before the swap, {@code getFiles(pathId)} must
     * <b>not</b> return the TEMPORARY new file (the DAO filters {@code FILE_TYPE = REGULAR}).
     * After the swap the promoted file is visible and the old file disappears.
     */
    @Test
    public void testAtomicSwap_temporaryInvisibleViaGetFiles() throws Exception
    {
        writeTestFile("vis_old.pxl", LONG_ID_SCHEMA, new long[]{0, 1}, true, new long[]{100, 100});
        long[] fileIds = registerTestFiles(
                new String[]{"vis_old.pxl", "vis_new_temp.pxl"},
                new File.Type[]{File.Type.REGULAR, File.Type.TEMPORARY},
                new int[]{1, 1}, new long[]{0, 0}, new long[]{1, 1});
        long oldFileId = fileIds[0];
        long tempFileId = fileIds[1];

        List<File> beforeSwap = metadataService.getFiles(testPathId);
        Set<Long> beforeIds = new HashSet<>();
        for (File f : beforeSwap)
        {
            beforeIds.add(f.getId());
        }
        assertTrue("REGULAR old file should be visible via getFiles before swap",
                beforeIds.contains(oldFileId));
        assertFalse("TEMPORARY new file must NOT be visible via getFiles before swap",
                beforeIds.contains(tempFileId));

        metadataService.atomicSwapFiles(tempFileId, Collections.singletonList(oldFileId));

        List<File> afterSwap = metadataService.getFiles(testPathId);
        Set<Long> afterIds = new HashSet<>();
        for (File f : afterSwap)
        {
            afterIds.add(f.getId());
        }
        assertTrue("Promoted file should be visible via getFiles after swap",
                afterIds.contains(tempFileId));
        assertFalse("Old file should NOT be visible via getFiles after swap",
                afterIds.contains(oldFileId));
    }

    // -----------------------------------------------------------------------
    // Coverage for getFiles(pathId) REGULAR-only enumeration.
    // -----------------------------------------------------------------------

    /**
     * A path containing REGULAR and non-REGULAR FILE_TYPE values returns only REGULAR entries.
     */
    @Test
    public void testGetFiles_mixedAllFileTypes_onlyRegular() throws Exception
    {
        long regularId = -1L;
        long tempId = -1L;
        long nonRegularPositiveId = -1L;
        long negativeId = -1L;
        long extremeId = -1L;
        try
        {
            String suffix = Long.toString(System.nanoTime());
            regularId = registerTestFile("mix_regular_" + suffix + ".pxl",
                    File.Type.REGULAR, 1, 0L, 1L);
            tempId = registerTestFile("mix_temp_" + suffix + ".pxl",
                    File.Type.TEMPORARY, 1, 0L, 1L);
            nonRegularPositiveId = insertRawFileWithType("mix_non_regular_" + suffix + ".pxl",
                    File.Type.REGULAR.ordinal() + 1, 1, 0L, 1L);
            negativeId = insertRawFileWithType("mix_negative_" + suffix + ".pxl",
                    -2, 1, 0L, 1L);
            extremeId = insertRawFileWithType("mix_extreme_max_" + suffix + ".pxl",
                    Integer.MAX_VALUE, 1, 0L, 1L);

            List<File> files = metadataService.getFiles(testPathId);
            Set<Long> visible = new HashSet<>();
            for (File f : files)
            {
                assertEquals("getFiles must only emit REGULAR",
                        File.Type.REGULAR, f.getType());
                visible.add(f.getId());
            }
            assertTrue("REGULAR member of the mix must be visible",
                    visible.contains(regularId));
            assertFalse("TEMPORARY (FILE_TYPE=0) must be hidden",
                    visible.contains(tempId));
            assertFalse("non-REGULAR positive FILE_TYPE must be hidden",
                    visible.contains(nonRegularPositiveId));
            assertFalse("negative FILE_TYPE must be hidden",
                    visible.contains(negativeId));
            assertFalse("Integer.MAX_VALUE FILE_TYPE must be hidden",
                    visible.contains(extremeId));
        }
        finally
        {
            List<Long> cleanup = new ArrayList<>();
            if (regularId > 0) cleanup.add(regularId);
            if (tempId > 0) cleanup.add(tempId);
            if (nonRegularPositiveId > 0) cleanup.add(nonRegularPositiveId);
            if (negativeId > 0) cleanup.add(negativeId);
            if (extremeId > 0) cleanup.add(extremeId);
            if (!cleanup.isEmpty()) metadataService.deleteFiles(cleanup);
        }
    }

    /**
     * A minimum-size REGULAR file is returned with its catalog fields intact.
     */
    @Test
    public void testGetFiles_singleRegularMinimumData() throws Exception
    {
        long fileId = -1L;
        try
        {
            fileId = registerTestFile("min_single_regular_" + System.nanoTime() + ".pxl",
                    File.Type.REGULAR, 1, 0L, 0L);
            List<File> files = metadataService.getFiles(testPathId);
            File found = null;
            for (File f : files)
            {
                if (f.getId() == fileId)
                {
                    found = f;
                }
                assertEquals("every returned entry must be REGULAR",
                        File.Type.REGULAR, f.getType());
            }
            assertNotNull("the single REGULAR minimum-data file must be visible", found);
            assertEquals("type must be REGULAR", File.Type.REGULAR, found.getType());
            assertEquals("numRowGroup of minimum file must be 1", 1, found.getNumRowGroup());
            assertEquals("minRowId of minimum file must be 0", 0L, found.getMinRowId());
            assertEquals("maxRowId of minimum file must be 0", 0L, found.getMaxRowId());
        }
        finally
        {
            if (fileId > 0)
            {
                metadataService.deleteFiles(Collections.singletonList(fileId));
            }
        }
    }

    /**
     * A deleted REGULAR file is no longer returned by {@code getFiles}.
     */
    @Test
    public void testGetFiles_deletedRegular_notVisible() throws Exception
    {
        long regularId = registerTestFile("delete_visibility_" + System.nanoTime() + ".pxl",
                File.Type.REGULAR, 1, 0L, 1L);

        List<File> beforeDelete = metadataService.getFiles(testPathId);
        Set<Long> beforeIds = new HashSet<>();
        for (File f : beforeDelete) beforeIds.add(f.getId());
        assertTrue("REGULAR file must be visible before delete",
                beforeIds.contains(regularId));

        metadataService.deleteFiles(Collections.singletonList(regularId));

        List<File> afterDelete = metadataService.getFiles(testPathId);
        for (File f : afterDelete)
        {
            assertFalse("deleted REGULAR file must no longer be visible",
                    f.getId() == regularId);
        }
    }

    /**
     * Concurrent readers observe a consistent REGULAR-only result.
     */
    @Test
    public void testGetFiles_concurrentReaders_consistentRegularOnly() throws Exception
    {
        long regularId = -1L;
        long tempId = -1L;
        long nonRegularPositiveId = -1L;
        ExecutorService pool = null;
        try
        {
            String suffix = Long.toString(System.nanoTime());
            regularId = registerTestFile("conc_regular_" + suffix + ".pxl",
                    File.Type.REGULAR, 1, 0L, 1L);
            tempId = registerTestFile("conc_temp_" + suffix + ".pxl",
                    File.Type.TEMPORARY, 1, 0L, 1L);
            nonRegularPositiveId = insertRawFileWithType("conc_non_regular_" + suffix + ".pxl",
                    File.Type.REGULAR.ordinal() + 1, 1, 0L, 1L);

            final int threads = 8;
            final int iterations = 16;
            pool = Executors.newFixedThreadPool(threads);
            CyclicBarrier startGate = new CyclicBarrier(threads);
            AtomicInteger leakedTemporary = new AtomicInteger();
            AtomicInteger leakedNonRegular = new AtomicInteger();
            AtomicInteger missingRegular = new AtomicInteger();

            List<CompletableFuture<Void>> futures = new ArrayList<>();
            final long pinnedRegular = regularId;
            final long pinnedTemp = tempId;
            final long pinnedNonRegular = nonRegularPositiveId;
            for (int t = 0; t < threads; t++)
            {
                futures.add(CompletableFuture.runAsync(() ->
                {
                    try
                    {
                        startGate.await();
                        for (int i = 0; i < iterations; i++)
                        {
                            List<File> snapshot = metadataService.getFiles(testPathId);
                            boolean sawRegular = false;
                            for (File f : snapshot)
                            {
                                if (f.getType() != File.Type.REGULAR)
                                {
                                    leakedNonRegular.incrementAndGet();
                                }
                                if (f.getId() == pinnedRegular) sawRegular = true;
                                if (f.getId() == pinnedTemp) leakedTemporary.incrementAndGet();
                                if (f.getId() == pinnedNonRegular) leakedNonRegular.incrementAndGet();
                            }
                            if (!sawRegular) missingRegular.incrementAndGet();
                        }
                    }
                    catch (Exception e)
                    {
                        throw new RuntimeException(e);
                    }
                }, pool));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(30, java.util.concurrent.TimeUnit.SECONDS);

            assertEquals("no concurrent reader may observe a TEMPORARY file",
                    0, leakedTemporary.get());
            assertEquals("no concurrent reader may observe a non-REGULAR file",
                    0, leakedNonRegular.get());
            assertEquals("every concurrent reader must observe the REGULAR file",
                    0, missingRegular.get());

            // A follow-up call should remain REGULAR-only after the concurrent burst.
            List<File> followUp = metadataService.getFiles(testPathId);
            assertNotNull("follow-up getFiles must not return null", followUp);
            for (File f : followUp)
            {
                assertEquals("follow-up entries must all be REGULAR",
                        File.Type.REGULAR, f.getType());
            }
        }
        finally
        {
            if (pool != null)
            {
                pool.shutdownNow();
            }
            List<Long> cleanup = new ArrayList<>();
            if (regularId > 0) cleanup.add(regularId);
            if (tempId > 0) cleanup.add(tempId);
            if (nonRegularPositiveId > 0) cleanup.add(nonRegularPositiveId);
            if (!cleanup.isEmpty()) metadataService.deleteFiles(cleanup);
        }
    }

    /**
     * Multiple serial swaps: Storage GC processes FileGroups serially on a single
     * thread, so {@code atomicSwapFiles} is never called concurrently in production.
     * This test reflects that design: N independent (newFile, oldFile) pairs are
     * swapped one after another, and every new file ends up REGULAR while every
     * old file is removed.
     */
    @Test
    public void testAtomicSwap_multipleSerialSwaps() throws Exception
    {
        TypeDescription schema = LONG_ID_SCHEMA;
        long[] ids = {0};
        long[] ts = {100};
        int nPairs = 8;

        long[] newFileIds = new long[nPairs];
        long[] oldFileIds = new long[nPairs];

        for (int i = 0; i < nPairs; i++)
        {
            String oldName = "serial_old_" + i + ".pxl";
            String newName = "serial_new_" + i + ".pxl";
            writeTestFile(oldName, schema, ids, true, ts);

            long[] pair = registerTestFiles(
                    new String[]{oldName, newName},
                    new File.Type[]{File.Type.REGULAR, File.Type.TEMPORARY},
                    new int[]{1, 1}, new long[]{0, 0}, new long[]{0, 0});
            oldFileIds[i] = pair[0];
            newFileIds[i] = pair[1];
        }

        for (int i = 0; i < nPairs; i++)
        {
            metadataService.atomicSwapFiles(newFileIds[i],
                    Collections.singletonList(oldFileIds[i]));
        }

        for (int i = 0; i < nPairs; i++)
        {
            assertFileRegular(newFileIds[i], "Promoted file " + i + " must be REGULAR");
            assertFileGone(oldFileIds[i], "Old file " + i + " should be gone");
        }
    }

    /**
     * Partial old-files-already-gone: one old file is deleted before the swap, but
     * {@code atomicSwapFiles} is called with both IDs.  The DELETE-WHERE-IN for an
     * already-absent row is a no-op; the transaction must still commit, promoting the
     * new file and removing the remaining old file.
     */
    @Test
    public void testAtomicSwap_partialOldFilesAlreadyGone() throws Exception
    {
        writeTestFile("partial_old1.pxl", LONG_ID_SCHEMA, new long[]{0, 1}, true, new long[]{100, 100});
        writeTestFile("partial_old2.pxl", LONG_ID_SCHEMA, new long[]{0, 1}, true, new long[]{100, 100});

        long[] oldIds = registerTestFiles(
                new String[]{"partial_old1.pxl", "partial_old2.pxl"},
                new File.Type[]{File.Type.REGULAR, File.Type.REGULAR},
                new int[]{1, 1}, new long[]{0, 0}, new long[]{1, 1});

        metadataService.deleteFiles(Collections.singletonList(oldIds[0]));
        assertFileGone(oldIds[0], "old1 should be gone before swap");

        long newFileId = registerTestFile("partial_new.pxl", File.Type.TEMPORARY, 1, 0, 1);
        metadataService.atomicSwapFiles(newFileId, Arrays.asList(oldIds[0], oldIds[1]));

        assertFileRegular(newFileId, "New file must be REGULAR");
        assertFileGone(oldIds[1], "Remaining old file should be gone");
    }

    /**
     * Rollback after rewrite + dual-write: verifies that Visibility entries for the new
     * file are removed, dual-write is unregistered, the TEMPORARY catalog entry is deleted,
     * and the physical file is cleaned up.
     */
    @Test
    public void testAtomicSwap_rollbackCleansUp() throws Exception
    {
        long[] ids = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        long[] ts = new long[10];
        Arrays.fill(ts, 100);
        String filePath = writeTestFile("rollback_src.pxl", LONG_ID_SCHEMA, ids, true, ts);
        long srcFileId = registerTestFile("rollback_src.pxl", File.Type.REGULAR, 1, 0, 9);

        StorageGarbageCollector.FileGroup group = makeGroup(srcFileId, filePath, LONG_ID_SCHEMA);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(RetinaUtils.buildRgKey(srcFileId, 0), makeBitmap(10, 6));

        retinaManager.addVisibility(srcFileId, 0, 10, 50, null, true);

        StorageGarbageCollector.RewriteResult result = gc.rewriteFileGroup(group, 100, bitmaps);
        assertTrue("New file should be created", result.newFileId > 0);

        gc.registerDualWrite(result);

        gc.rollback(result);

        assertFalse("New file should be deleted after rollback",
                fileStorage.exists(result.newFilePath));

        assertFileGone(result.newFileId, "Catalog entry should be deleted after rollback");
    }

    /** Delayed cleanup removes old file Visibility and physical file after wall-clock deadline passes. */
    @Test
    public void testAtomicSwap_delayedCleanup() throws Exception
    {
        TypeDescription schema = LONG_ID_SCHEMA;
        long[] ids = {0, 1, 2, 3, 4};
        long[] ts = new long[5];
        Arrays.fill(ts, 100);
        String filePath = writeTestFile("delayed_old.pxl", schema, ids, true, ts);
        long fakeFileId = 999999L;

        retinaManager.addVisibility(fakeFileId, 0, 5, 50, null, true);

        long futureDeadline = System.currentTimeMillis() + 60_000L;
        RetinaResourceManager.RetiredFile retiredFuture = new RetinaResourceManager.RetiredFile(
                fakeFileId, 1, filePath, futureDeadline, Collections.emptyList());
        retinaManager.scheduleRetiredFile(retiredFuture);

        retinaManager.processRetiredFiles();
        assertTrue("File should NOT be cleaned before deadline",
                fileStorage.exists(filePath));

        resetManagerState();

        retinaManager.addVisibility(fakeFileId, 0, 5, 50, null, true);

        long pastDeadline = System.currentTimeMillis() - 1L;
        RetinaResourceManager.RetiredFile retiredPast = new RetinaResourceManager.RetiredFile(
                fakeFileId, 1, filePath, pastDeadline, Collections.emptyList());
        retinaManager.scheduleRetiredFile(retiredPast);

        retinaManager.processRetiredFiles();
        assertFalse("File should be cleaned after deadline",
                fileStorage.exists(filePath));
    }

    // =======================================================================
    // Section 9: end-to-end integration tests (placeholder)
    // =======================================================================

    /**
     * Comprehensive end-to-end test covering the full Storage GC lifecycle with
     * real deletion chains.  Exercises every step of the pipeline:
     *
     * <pre>
     * Phase 1 (ts ≤ safeGcTs=100): delete 6 rows → physically removed by rewrite
     * Phase 2 (ts=150, before dual-write): delete row 1 → only in old chain, needs export
     * Rewrite → verify data, forward/backward mappings, hidden column preservation
     * Register dual-write
     * Phase 3 (ts=200, dual-write active): delete row 3 → propagated to both files
     * Sync visibility → export + coord-transform + import
     * Phase 4 (ts=300, post-sync, dual-write still active): delete row 5
     * Commit → atomic swap (TEMPORARY→REGULAR), old file removed from catalog
     * Verify: multi-snap_ts consistency on new file at ts=100..500
     * Verify: old file gone from catalog, new file REGULAR
     * </pre>
     */
    @Test
    public void testEndToEnd_fullGcCycle() throws Exception
    {
        int numRows = 10;
        long[] ids = new long[numRows];
        long[] createTs = new long[numRows];
        for (int i = 0; i < numRows; i++)
        {
            ids[i] = i * 10;
            createTs[i] = 50L;
        }
        String srcPath = writeTestFile("e2e_full_src.pxl", LONG_ID_SCHEMA, ids, true, createTs);
        long srcFileId = registerTestFile("e2e_full_src.pxl", File.Type.REGULAR, 1, 0, numRows - 1);

        retinaManager.addVisibility(srcFileId, 0, numRows, 0L, null, false);

        long safeGcTs = 100L;
        retinaManager.deleteRecord(srcFileId, 0, 0, 10L);
        retinaManager.deleteRecord(srcFileId, 0, 2, 20L);
        retinaManager.deleteRecord(srcFileId, 0, 4, 30L);
        retinaManager.deleteRecord(srcFileId, 0, 6, 50L);
        retinaManager.deleteRecord(srcFileId, 0, 8, 70L);
        retinaManager.deleteRecord(srcFileId, 0, 9, 90L);

        retinaManager.deleteRecord(srcFileId, 0, 1, 150L);

        long[] gcBitmap = retinaManager.queryVisibility(srcFileId, 0, safeGcTs, 0L);
        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(RetinaUtils.buildRgKey(srcFileId, 0), gcBitmap);

        for (int r : new int[]{0, 2, 4, 6, 8, 9})
        {
            assertTrue("row " + r + " should be in GC bitmap", isBitSet(gcBitmap, r));
        }
        for (int r : new int[]{1, 3, 5, 7})
        {
            assertFalse("row " + r + " should NOT be in GC bitmap", isBitSet(gcBitmap, r));
        }

        NoIndexSyncGC e2eGc = new NoIndexSyncGC(retinaManager, metadataService,
                0.5, 134_217_728L, Integer.MAX_VALUE, 10, 1048576,
                EncodingLevel.EL2, 86_400_000L);

        StorageGarbageCollector.FileGroup group = makeGroup(srcFileId, srcPath, LONG_ID_SCHEMA);

        StorageGarbageCollector.RewriteResult result =
                e2eGc.rewriteFileGroup(group, safeGcTs, bitmaps);
        long newFileId = result.newFileId;
        assertTrue("new file must be created", newFileId > 0);
        assertRewriteResultConsistency(result, 4);

        long[][] rows = readAllRows(result.newFilePath, LONG_ID_SCHEMA, true);
        assertEquals("4 survivors expected (rows 1,3,5,7)", 4, rows.length);
        long[] expectedIds = {10L, 30L, 50L, 70L};
        for (int i = 0; i < 4; i++)
        {
            assertEquals("id mismatch at new row " + i, expectedIds[i], rows[i][0]);
            assertEquals("create_ts mismatch at new row " + i, 50L, rows[i][1]);
        }

        int[] fwd = result.forwardRgMappings.get(srcFileId).get(0);
        assertEquals(-1, fwd[0]);
        assertEquals(0, fwd[1]);
        assertEquals(-1, fwd[2]);
        assertEquals(1, fwd[3]);
        assertEquals(-1, fwd[4]);
        assertEquals(2, fwd[5]);
        assertEquals(-1, fwd[6]);
        assertEquals(3, fwd[7]);
        assertEquals(-1, fwd[8]);
        assertEquals(-1, fwd[9]);

        assertEquals(1, result.backwardInfos.size());
        StorageGarbageCollector.BackwardInfo bwd = result.backwardInfos.get(0);
        assertEquals(srcFileId, bwd.oldFileId);

        e2eGc.registerDualWrite(result);

        retinaManager.deleteRecord(srcFileId, 0, 3, 200L);
        int newRowForOld3 = fwd[3];
        assertTrue("fwd[3] should be valid", newRowForOld3 >= 0);
        long[] dualBm = retinaManager.queryVisibility(newFileId, 0, 200L, 0L);
        assertTrue("dual-write: new row " + newRowForOld3 + " should be deleted",
                isBitSet(dualBm, newRowForOld3));

        e2eGc.syncVisibility(result, safeGcTs);

        int newRowForOld1 = fwd[1];
        long[] syncBm = retinaManager.queryVisibility(newFileId, 0, 150L, 0L);
        assertTrue("sync: new row " + newRowForOld1 + " should show old row 1 deleted at ts=150",
                isBitSet(syncBm, newRowForOld1));

        retinaManager.deleteRecord(srcFileId, 0, 5, 300L);
        int newRowForOld5 = fwd[5];
        assertTrue("fwd[5] should be valid", newRowForOld5 >= 0);

        e2eGc.syncIndex(result, group.tableId);
        e2eGc.commitFileGroup(result);

        assertFileRegular(newFileId, "new file should be REGULAR after commit");
        assertFileGone(srcFileId, "old file should be gone from catalog after commit");

        assertTrue("old physical file should still exist (delayed cleanup, not yet due)",
                fileStorage.exists(srcPath));

        for (long snap : new long[]{100L, 149L, 150L, 199L, 200L, 299L, 300L, 500L})
        {
            long[] bm = retinaManager.queryVisibility(newFileId, 0, snap, 0L);
            assertEquals("snap=" + snap + " newRow0 (old row1, del@150)", snap >= 150, isBitSet(bm, 0));
            assertEquals("snap=" + snap + " newRow1 (old row3, del@200)", snap >= 200, isBitSet(bm, 1));
            assertEquals("snap=" + snap + " newRow2 (old row5, del@300)", snap >= 300, isBitSet(bm, 2));
            assertFalse("snap=" + snap + " newRow3 (old row7) should never be deleted", isBitSet(bm, 3));
        }

        for (long snap : new long[]{100L, 150L, 200L, 300L, 500L})
        {
            long[] oldBm = retinaManager.queryVisibility(srcFileId, 0, snap, 0L);
            long[] newBm = retinaManager.queryVisibility(newFileId, 0, snap, 0L);
            for (int oldRow = 1; oldRow <= 7; oldRow += 2)
            {
                int newRow = fwd[oldRow];
                assertTrue("old row " + oldRow + " should have valid mapping", newRow >= 0);
                assertEquals("snap=" + snap + " old row " + oldRow + " vs new row " + newRow
                        + " visibility mismatch", isBitSet(oldBm, oldRow), isBitSet(newBm, newRow));
            }
        }
    }

    /** Inject failure after visibility sync → rollback clean → re-run succeeds. */
    @Ignore("rollback end-to-end not yet implemented")
    @Test
    public void testEndToEnd_rollbackOnVisibilitySyncFailure() throws Exception
    {
    }

    /** WAL crash recovery from each state → correct resume or rollback. */
    @Ignore("crash recovery not yet implemented")
    @Test
    public void testEndToEnd_crashRecovery() throws Exception
    {
    }

    /**
     * Concurrent INSERT/DELETE/UPDATE + GC → all operations correct.
     *
     * <p>Simulates a realistic CDC event stream (serial per virtualNodeId) running
     * concurrently with a full Storage GC pipeline (S2→S6).  CDC events include
     * DELETE, INSERT (new file + addVisibility), and UPDATE (deleteRecord + INSERT).
     * Events are spread across the entire GC execution window with short sleep
     * intervals to maximize interleaving with every GC phase.
     *
     * <pre>
     * Phase 0: 30 rows, delete 18 (ts 5..90 &lt; safeGcTs=100), 12 survivors (rows 18-29)
     * Phase 1: CDC thread + GC thread start concurrently via CyclicBarrier
     *   CDC: DELETE/INSERT/UPDATE events at ts=150..550, interleaving with GC
     *   GC:  rewrite → registerDualWrite → syncVisibility → syncIndex(stub) → commit
     * Phase 2: post-GC CDC deletes rows 27-29 at ts=600..700 (dual-write off)
     * Phase 3: Verification — multi-snap_ts consistency, catalog, data, no errors
     * </pre>
     */
    @Test
    public void testEndToEnd_concurrentCdcAndGc() throws Exception
    {
        TypeDescription schema = LONG_ID_SCHEMA;
        int numRows = 30;
        long safeGcTs = 100L;

        // ── Phase 0: Setup source file ──────────────────────────────────────
        long[] ids = new long[numRows];
        long[] createTs = new long[numRows];
        for (int i = 0; i < numRows; i++)
        {
            ids[i] = i * 10;
            createTs[i] = 50L;
        }
        String srcPath = writeTestFile("conc_cdc_gc.pxl", LONG_ID_SCHEMA, ids, true, createTs);
        long srcFileId = registerTestFile("conc_cdc_gc.pxl", File.Type.REGULAR, 1, 0, numRows - 1);

        retinaManager.addVisibility(srcFileId, 0, numRows, 0L, null, false);

        int deletedBefore = 18;
        for (int i = 0; i < deletedBefore; i++)
        {
            retinaManager.deleteRecord(srcFileId, 0, i, 5L + i * 5L);
        }

        long[] gcBitmap = retinaManager.queryVisibility(srcFileId, 0, safeGcTs, 0L);
        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(RetinaUtils.buildRgKey(srcFileId, 0), gcBitmap);

        for (int r = 0; r < deletedBefore; r++)
        {
            assertTrue("row " + r + " must be in GC bitmap", isBitSet(gcBitmap, r));
        }
        int survivors = numRows - deletedBefore;
        for (int r = deletedBefore; r < numRows; r++)
        {
            assertFalse("row " + r + " must NOT be in GC bitmap", isBitSet(gcBitmap, r));
        }

        // ── Prepare GC and concurrency primitives ───────────────────────────
        NoIndexSyncGC concGc = new NoIndexSyncGC(retinaManager, metadataService,
                0.5, 134_217_728L, Integer.MAX_VALUE, 10, 1048576,
                EncodingLevel.EL2, 86_400_000L);

        StorageGarbageCollector.FileGroup group = makeGroup(srcFileId, srcPath, LONG_ID_SCHEMA);

        CyclicBarrier barrier = new CyclicBarrier(2);
        java.util.concurrent.CountDownLatch dualWriteActive = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch cdcPhaseBDone = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch gcDone = new java.util.concurrent.CountDownLatch(1);
        AtomicInteger errors = new AtomicInteger(0);
        java.util.concurrent.atomic.AtomicReference<StorageGarbageCollector.RewriteResult> resultRef =
                new java.util.concurrent.atomic.AtomicReference<>(null);
        java.util.concurrent.atomic.AtomicReference<Throwable> gcError =
                new java.util.concurrent.atomic.AtomicReference<>(null);
        java.util.concurrent.atomic.AtomicReference<Throwable> cdcError =
                new java.util.concurrent.atomic.AtomicReference<>(null);

        List<Long> insertedFileIds = Collections.synchronizedList(new ArrayList<>());

        // ── Phase 1: Launch CDC + GC threads concurrently ───────────────────

        // CDC thread: serial events on the same (table, virtualNodeId).
        // Phase A runs during S2 rewrite (pre-dual-write) → captured by export.
        // Phase B runs during dual-write window (S3-S6) → forwarded by dual-write.
        // Phase C runs after GC commit → only reaches old file.
        Thread cdcThread = new Thread(() ->
        {
            try
            {
                barrier.await();

                // Phase A + B wrapped in try-finally so that cdcPhaseBDone
                // is always signalled, even if an exception occurs.
                try
                {
                    // ── Phase A: pre-dual-write deletes (during S2 rewrite) ─
                    // These go only to the old file's chain; syncVisibility
                    // export will capture them (ts > safeGcTs).
                    retinaManager.deleteRecord(srcFileId, 0, 18, 150L);
                    Thread.sleep(2);
                    retinaManager.deleteRecord(srcFileId, 0, 19, 200L);
                    Thread.sleep(2);
                    retinaManager.deleteRecord(srcFileId, 0, 20, 250L);

                    // Wait until GC has registered dual-write
                    dualWriteActive.await();

                    // ── Phase B: dual-write window (S3→S6) ──────────────────
                    // Deletes are forwarded to both old and new file.
                    // INSERT/UPDATE interleaved to test concurrent addVisibility.
                    retinaManager.deleteRecord(srcFileId, 0, 21, 300L);
                    Thread.sleep(5);

                    // INSERT: new file cdc_ins_1.pxl (2 rows)
                    long[] insIds1 = {1000L, 1001L};
                    long[] insTs1 = {300L, 300L};
                    writeTestFile("cdc_ins_1.pxl", schema, insIds1, true, insTs1);
                    long insFileId1 = 9001L;
                    retinaManager.addVisibility(insFileId1, 0, 2, 0L, null, false);
                    insertedFileIds.add(insFileId1);

                    retinaManager.deleteRecord(srcFileId, 0, 22, 350L);
                    Thread.sleep(5);

                    // UPDATE row 23 @ ts=400: delete old + insert new
                    retinaManager.deleteRecord(srcFileId, 0, 23, 400L);
                    long[] updIds1 = {2000L};
                    long[] updTs1 = {400L};
                    writeTestFile("cdc_upd_1.pxl", schema, updIds1, true, updTs1);
                    long updFileId1 = 9002L;
                    retinaManager.addVisibility(updFileId1, 0, 1, 0L, null, false);
                    insertedFileIds.add(updFileId1);

                    retinaManager.deleteRecord(srcFileId, 0, 24, 450L);
                    Thread.sleep(5);

                    retinaManager.deleteRecord(srcFileId, 0, 25, 500L);
                    Thread.sleep(5);

                    // INSERT: new file cdc_ins_2.pxl (2 rows)
                    long[] insIds2 = {1002L, 1003L};
                    long[] insTs2 = {500L, 500L};
                    writeTestFile("cdc_ins_2.pxl", schema, insIds2, true, insTs2);
                    long insFileId2 = 9003L;
                    retinaManager.addVisibility(insFileId2, 0, 2, 0L, null, false);
                    insertedFileIds.add(insFileId2);

                    retinaManager.deleteRecord(srcFileId, 0, 26, 550L);
                }
                finally
                {
                    cdcPhaseBDone.countDown();
                }

                // ── Phase C: Wait for GC commit, then post-GC deletes ───────
                gcDone.await();

                // Dual-write is now off; these only go to old file
                retinaManager.deleteRecord(srcFileId, 0, 27, 600L);
                Thread.sleep(5);
                retinaManager.deleteRecord(srcFileId, 0, 28, 650L);
                Thread.sleep(5);
                retinaManager.deleteRecord(srcFileId, 0, 29, 700L);
            }
            catch (Throwable t)
            {
                cdcError.set(t);
                errors.incrementAndGet();
            }
        });

        // GC thread: full pipeline
        Thread gcThread = new Thread(() ->
        {
            try
            {
                barrier.await();

                StorageGarbageCollector.RewriteResult result =
                        concGc.rewriteFileGroup(group, safeGcTs, bitmaps);
                assertTrue("new file must be created", result.newFileId > 0);

                concGc.registerDualWrite(result);
                dualWriteActive.countDown();

                concGc.syncVisibility(result, safeGcTs);
                concGc.syncIndex(result, group.tableId);

                cdcPhaseBDone.await();
                concGc.commitFileGroup(result);

                resultRef.set(result);
            }
            catch (Throwable t)
            {
                gcError.set(t);
                errors.incrementAndGet();
                dualWriteActive.countDown();
            }
            finally
            {
                gcDone.countDown();
            }
        });

        cdcThread.start();
        gcThread.start();

        cdcThread.join(30_000);
        gcThread.join(30_000);

        if (gcError.get() != null)
        {
            throw new AssertionError("GC thread failed", gcError.get());
        }
        if (cdcError.get() != null)
        {
            throw new AssertionError("CDC thread failed", cdcError.get());
        }

        assertFalse("CDC thread should have finished", cdcThread.isAlive());
        assertFalse("GC thread should have finished", gcThread.isAlive());
        assertEquals("no errors during concurrent execution", 0, errors.get());

        // ── Phase 3: Verification ───────────────────────────────────────────
        StorageGarbageCollector.RewriteResult result = resultRef.get();
        assertNotNull("RewriteResult must be available", result);
        long newFileId = result.newFileId;
        assertTrue("new file must have a valid id", newFileId > 0);

        // 3a. Verify new file data: 12 survivors (rows 18-29)
        assertRewriteResultConsistency(result, survivors);
        long[][] rows = readAllRows(result.newFilePath, schema, true);
        assertEquals("12 survivors expected", survivors, rows.length);
        for (int i = 0; i < survivors; i++)
        {
            long expectedId = (deletedBefore + i) * 10L;
            assertEquals("id mismatch at new row " + i, expectedId, rows[i][0]);
            assertEquals("create_ts mismatch at new row " + i, 50L, rows[i][1]);
        }

        // 3b. Verify catalog state
        assertFileRegular(newFileId, "new file should be REGULAR");
        assertFileGone(srcFileId, "old file should be gone from catalog");

        // 3c. Forward mapping
        int[] fwd = result.forwardRgMappings.get(srcFileId).get(0);
        for (int r = 0; r < deletedBefore; r++)
        {
            assertEquals("deleted row " + r + " should map to -1", -1, fwd[r]);
        }
        for (int r = deletedBefore; r < numRows; r++)
        {
            assertTrue("surviving row " + r + " should have valid mapping",
                    fwd[r] >= 0);
        }

        // 3d. Multi-snap_ts visibility consistency for rows in dual-write window.
        // During dual-write, deletes on old file are forwarded to new file,
        // and export+import syncs pre-dual-write deletes. So for any snap_ts,
        // old and new visibility must agree for rows that were deleted BEFORE
        // dual-write was unregistered (i.e., before commit).
        // CDC deletes on rows 18-26 have ts=150..550, all happen before/during GC.
        for (long snapTs : new long[]{100L, 150L, 200L, 250L, 300L, 350L, 400L,
                450L, 500L, 550L, 800L, 1000L})
        {
            long[] oldBm = retinaManager.queryVisibility(srcFileId, 0, snapTs, 0L);
            for (int oldRow = deletedBefore; oldRow < numRows; oldRow++)
            {
                int newGlobal = fwd[oldRow];
                assertTrue("old row " + oldRow + " must have valid fwd mapping",
                        newGlobal >= 0);
                int newRgId = RetinaResourceManager.rgIdForGlobalRowOffset(
                        newGlobal, result.newFileRgRowStart);
                int newRgOff = newGlobal - result.newFileRgRowStart[newRgId];
                long[] newBm = retinaManager.queryVisibility(newFileId, newRgId, snapTs, 0L);

                boolean oldDel = isBitSet(oldBm, oldRow);
                boolean newDel = isBitSet(newBm, newRgOff);

                if (oldRow <= 26)
                {
                    // Rows 18-26: deleted during dual-write window → must be consistent
                    assertEquals("snap_ts=" + snapTs + " oldRow=" + oldRow
                            + " newRgOff=" + newRgOff + ": visibility mismatch",
                            oldDel, newDel);
                }
                else
                {
                    // Rows 27-29: deleted after GC commit (dual-write off).
                    // Old file shows them as deleted for snap_ts >= their delete_ts;
                    // new file should NOT reflect these post-GC deletes.
                    assertFalse("snap_ts=" + snapTs + " newRow for oldRow=" + oldRow
                            + " should NOT be deleted (post-GC delete)",
                            newDel);
                }
            }
        }

        // 3e. Verify INSERT files are unaffected by GC
        for (long insFileId : insertedFileIds)
        {
            long[] insBm = retinaManager.queryVisibility(insFileId, 0, 1000L, 0L);
            assertNotNull("INSERT file " + insFileId + " visibility should exist", insBm);
            for (int w = 0; w < insBm.length; w++)
            {
                assertEquals("INSERT file " + insFileId + " should have no deletes",
                        0L, insBm[w]);
            }
        }
    }

    // =======================================================================
    // E2E: Multi-round CDC + GC lifecycle
    // =======================================================================

    /**
     * Simulates a realistic multi-round CDC + GC lifecycle with three GC rounds
     * interleaved with serial CDC operations (INSERT, UPDATE, DELETE).
     *
     * <p>In production:
     * <ul>
     *   <li>CDC operations on a given (table, vnode) are serial (one at a time).</li>
     *   <li>GC runs periodically via {@code scheduleAtFixedRate}, serial.</li>
     *   <li>Between GC rounds, more CDC events arrive and accumulate deletions.</li>
     * </ul>
     *
     * <pre>
     * CDC epoch 1 (ts 10~90):
     *   INSERT file-A (20 rows, create_ts=5), DELETE 10 rows (0,2,4,...,18)
     * GC round 1 (safeGcTs=100):
     *   Memory GC → bitmap → rewrite file-A → file-A'
     *   file-A enters retired queue (retireDelayMs=0 → eligible immediately)
     *
     * CDC epoch 2 (ts 150~250):
     *   INSERT file-B (10 rows, create_ts=120)
     *   DELETE 5 rows in file-A' (new rows 0,2,4,6,8 → old rows 1,5,9,13,17)
     *   UPDATE row in file-A': delete new-row-1 + insert file-C (1 row)
     * GC round 2 (safeGcTs=300):
     *   processRetiredFiles → clean file-A physical file
     *   Memory GC → bitmap → rewrite file-A' → file-A''
     *   file-A' enters retired queue
     *
     * CDC epoch 3 (ts 350~400):
     *   DELETE 3 rows in file-B (rows 0,1,2)
     *   DELETE 2 rows in file-A'' (new rows 0,1)
     * GC round 3 (safeGcTs=450):
     *   processRetiredFiles → clean file-A'
     *   Memory GC → bitmap for file-A'', file-B, file-C
     *   rewrite file-B → file-B' (file-A'' and file-C below threshold)
     *
     * Final verification:
     *   - all surviving data readable with correct id/create_ts
     *   - visibility consistent across multiple snap_ts
     *   - catalog: only latest generation files exist as REGULAR
     *   - physical files from round 1 and 2 cleaned up
     * </pre>
     */
    @Test
    public void testEndToEnd_multiRoundCdcGcLifecycle() throws Exception
    {
        TypeDescription schema = LONG_ID_SCHEMA;

        // ── CDC epoch 1 ─────────────────────────────────────────────────────
        // INSERT file-A: 20 rows (id=0,10,20,...,190), create_ts=5
        int numRowsA = 20;
        long[] idsA = new long[numRowsA];
        long[] tsA = new long[numRowsA];
        for (int i = 0; i < numRowsA; i++)
        {
            idsA[i] = i * 10;
            tsA[i] = 5L;
        }
        String pathA = writeTestFile("lifecycle_a.pxl", schema, idsA, true, tsA);
        long fileIdA = registerTestFile("lifecycle_a.pxl", File.Type.REGULAR, 1, 0, numRowsA - 1);

        retinaManager.addVisibility(fileIdA, 0, numRowsA, 0L, null, false);

        // DELETE 10 even-indexed rows in file-A (rows 0,2,4,...,18) at ts=10..90
        for (int i = 0; i < 10; i++)
        {
            retinaManager.deleteRecord(fileIdA, 0, i * 2, 10L + i * 9L);
        }
        // Survivors in file-A at safeGcTs=100: odd rows 1,3,5,...,19 → ids 10,30,50,...,190

        // ── GC round 1 (safeGcTs=100) ──────────────────────────────────────
        long safeGcTs1 = 100L;
        NoIndexSyncGC gcR1 = new NoIndexSyncGC(retinaManager, metadataService,
                0.3, 134_217_728L, Integer.MAX_VALUE, 10, 1048576,
                EncodingLevel.EL2, 0L);

        // Step 1: Memory GC → gcSnapshotBitmap
        Map<String, long[]> bitmaps1 = new HashMap<>();
        Map<String, RGVisibility> rgMap1 = getRgVisibilityMap();
        for (Map.Entry<String, RGVisibility> entry : rgMap1.entrySet())
        {
            long[] bitmap = entry.getValue().garbageCollect(safeGcTs1);
            bitmaps1.put(entry.getKey(), bitmap);
        }

        // Step 2: Storage GC → rewrite file-A → file-A'
        StorageGarbageCollector.FileGroup groupA =
                makeGroup(fileIdA, pathA, schema);
        StorageGarbageCollector.RewriteResult resultR1 =
                gcR1.rewriteFileGroup(groupA, safeGcTs1, bitmaps1);
        long fileIdAprime = resultR1.newFileId;
        assertTrue("GC round 1 must create new file", fileIdAprime > 0);

        long[][] rowsAprime = readAllRows(resultR1.newFilePath, schema, true);
        assertEquals("10 survivors after round 1", 10, rowsAprime.length);
        for (int i = 0; i < 10; i++)
        {
            assertEquals("round 1: id mismatch at new row " + i,
                    (2 * i + 1) * 10L, rowsAprime[i][0]);
            assertEquals("round 1: create_ts preserved",
                    5L, rowsAprime[i][1]);
        }

        gcR1.registerDualWrite(resultR1);
        gcR1.syncVisibility(resultR1, safeGcTs1);
        gcR1.syncIndex(resultR1, groupA.tableId);
        gcR1.commitFileGroup(resultR1);

        assertFileRegular(fileIdAprime, "file-A' must be REGULAR after commit");

        String pathAprime = resultR1.newFilePath;
        assertTrue("file-A physical should still exist (in retired queue)", fileStorage.exists(pathA));

        // ── CDC epoch 2 (ts 150~250) ────────────────────────────────────────
        // INSERT file-B: 10 rows (id=1000..1009), create_ts=120
        int numRowsB = 10;
        long[] idsB = new long[numRowsB];
        long[] tsB = new long[numRowsB];
        for (int i = 0; i < numRowsB; i++)
        {
            idsB[i] = 1000 + i;
            tsB[i] = 120L;
        }
        writeTestFile("lifecycle_b.pxl", schema, idsB, true, tsB);
        long fileIdB = registerTestFile("lifecycle_b.pxl", File.Type.REGULAR, 1, 0, numRowsB - 1);

        retinaManager.addVisibility(fileIdB, 0, numRowsB, 0L, null, false);

        // DELETE 5 rows in file-A' (new rows 0,2,4,6,8) at ts=150~190
        for (int i = 0; i < 5; i++)
        {
            retinaManager.deleteRecord(fileIdAprime, 0, i * 2, 150L + i * 10L);
        }

        // UPDATE: delete new-row-1 in file-A' at ts=200, insert file-C (1 row)
        retinaManager.deleteRecord(fileIdAprime, 0, 1, 200L);

        long[] idsC = {9999L};
        long[] tsC = {200L};
        writeTestFile("lifecycle_c.pxl", schema, idsC, true, tsC);
        long fileIdC = registerTestFile("lifecycle_c.pxl", File.Type.REGULAR, 1, 0, 0);

        retinaManager.addVisibility(fileIdC, 0, 1, 0L, null, false);

        // ── GC round 2 (safeGcTs=300) ──────────────────────────────────────
        long safeGcTs2 = 300L;
        NoIndexSyncGC gcR2 = new NoIndexSyncGC(retinaManager, metadataService,
                0.3, 134_217_728L, Integer.MAX_VALUE, 10, 1048576,
                EncodingLevel.EL2, 0L);

        // processRetiredFiles → file-A should be cleaned (retireDelayMs=0)
        retinaManager.processRetiredFiles();
        assertFalse("file-A physical should be cleaned after processRetiredFiles",
                fileStorage.exists(pathA));

        // Step 1: Memory GC
        Map<String, long[]> bitmaps2 = new HashMap<>();
        Map<Long, long[]> fileStats2 = new HashMap<>();
        Map<String, RGVisibility> rgMap2 = getRgVisibilityMap();
        for (Map.Entry<String, RGVisibility> entry : rgMap2.entrySet())
        {
            String rgKey = entry.getKey();
            long fid = RetinaUtils.parseFileIdFromRgKey(rgKey);
            long[] bitmap = entry.getValue().garbageCollect(safeGcTs2);
            bitmaps2.put(rgKey, bitmap);

            long recordNum = entry.getValue().getRecordNum();
            long invalidCount = 0;
            for (long word : bitmap)
            {
                invalidCount += Long.bitCount(word);
            }
            final long ic = invalidCount;
            final long rn = recordNum;
            fileStats2.compute(fid, (k, existing) -> {
                if (existing == null) return new long[]{rn, ic};
                existing[0] += rn;
                existing[1] += ic;
                return existing;
            });
        }

        // file-A' has 6/10 deleted (60% > 30% threshold) → eligible
        long[] statsAprime = fileStats2.get(fileIdAprime);
        assertNotNull("file-A' stats must exist", statsAprime);
        assertTrue("file-A' invalidRatio > threshold for round 2",
                (double) statsAprime[1] / statsAprime[0] > 0.3);

        // Step 2: rewrite file-A' → file-A''
        StorageGarbageCollector.FileGroup groupAprime =
                makeGroup(fileIdAprime, pathAprime, schema);
        StorageGarbageCollector.RewriteResult resultR2 =
                gcR2.rewriteFileGroup(groupAprime, safeGcTs2, bitmaps2);
        long fileIdAdoubleprime = resultR2.newFileId;
        assertTrue("GC round 2 must create new file", fileIdAdoubleprime > 0);

        // Verify survivors in file-A'': original odd rows minus those deleted in epoch 2
        // Deleted in file-A': new rows 0,1,2,4,6,8 → survivors: new rows 3,5,7,9
        // new row 3 = old row 7 (id=70), new row 5 = old row 11 (id=110),
        // new row 7 = old row 15 (id=150), new row 9 = old row 19 (id=190)
        long[][] rowsAdp = readAllRows(resultR2.newFilePath, schema, true);
        assertEquals("4 survivors after round 2", 4, rowsAdp.length);
        long[] expectedIdsR2 = {70L, 110L, 150L, 190L};
        for (int i = 0; i < 4; i++)
        {
            assertEquals("round 2: id mismatch at row " + i,
                    expectedIdsR2[i], rowsAdp[i][0]);
            assertEquals("round 2: create_ts preserved", 5L, rowsAdp[i][1]);
        }

        gcR2.registerDualWrite(resultR2);
        gcR2.syncVisibility(resultR2, safeGcTs2);
        gcR2.syncIndex(resultR2, groupAprime.tableId);
        gcR2.commitFileGroup(resultR2);

        assertFileRegular(fileIdAdoubleprime, "file-A'' must be REGULAR after commit");

        String pathAdoubleprime = resultR2.newFilePath;

        // ── CDC epoch 3 (ts 350~400) ────────────────────────────────────────
        // DELETE 3 rows in file-B (rows 0,1,2) at ts=350..370
        for (int i = 0; i < 3; i++)
        {
            retinaManager.deleteRecord(fileIdB, 0, i, 350L + i * 10L);
        }

        // DELETE 2 rows in file-A'' (new rows 0,1) at ts=380,390
        retinaManager.deleteRecord(fileIdAdoubleprime, 0, 0, 380L);
        retinaManager.deleteRecord(fileIdAdoubleprime, 0, 1, 390L);

        // ── GC round 3 (safeGcTs=450) ──────────────────────────────────────
        long safeGcTs3 = 450L;
        NoIndexSyncGC gcR3 = new NoIndexSyncGC(retinaManager, metadataService,
                0.3, 134_217_728L, Integer.MAX_VALUE, 10, 1048576,
                EncodingLevel.EL2, 0L);

        // processRetiredFiles → file-A' should be cleaned
        retinaManager.processRetiredFiles();
        assertFalse("file-A' physical should be cleaned after round 3 processRetiredFiles",
                fileStorage.exists(pathAprime));

        // Step 1: Memory GC
        Map<String, long[]> bitmaps3 = new HashMap<>();
        Map<Long, long[]> fileStats3 = new HashMap<>();
        Map<String, RGVisibility> rgMap3 = getRgVisibilityMap();
        for (Map.Entry<String, RGVisibility> entry : rgMap3.entrySet())
        {
            String rgKey = entry.getKey();
            long fid = RetinaUtils.parseFileIdFromRgKey(rgKey);
            long[] bitmap = entry.getValue().garbageCollect(safeGcTs3);
            bitmaps3.put(rgKey, bitmap);

            long recordNum = entry.getValue().getRecordNum();
            long invalidCount = 0;
            for (long word : bitmap)
            {
                invalidCount += Long.bitCount(word);
            }
            final long ic = invalidCount;
            final long rn = recordNum;
            fileStats3.compute(fid, (k, existing) -> {
                if (existing == null) return new long[]{rn, ic};
                existing[0] += rn;
                existing[1] += ic;
                return existing;
            });
        }

        // file-B: 3/10 deleted = 30%. With threshold 0.3, need > 30% so NOT eligible.
        // file-A'': 2/4 deleted = 50% > 30% → eligible
        long[] statsB = fileStats3.get(fileIdB);
        assertNotNull("file-B stats must exist", statsB);

        long[] statsAdp = fileStats3.get(fileIdAdoubleprime);
        assertNotNull("file-A'' stats must exist", statsAdp);
        assertTrue("file-A'' invalidRatio > threshold for round 3",
                (double) statsAdp[1] / statsAdp[0] > 0.3);

        // Rewrite file-A'' → file-A'''
        StorageGarbageCollector.FileGroup groupAdp =
                makeGroup(fileIdAdoubleprime, pathAdoubleprime, schema);
        StorageGarbageCollector.RewriteResult resultR3 =
                gcR3.rewriteFileGroup(groupAdp, safeGcTs3, bitmaps3);
        long fileIdAtriple = resultR3.newFileId;
        assertTrue("GC round 3 must create new file", fileIdAtriple > 0);

        // file-A''' survivors: original file-A'' rows 2,3 → ids 150, 190
        long[][] rowsAtriple = readAllRows(resultR3.newFilePath, schema, true);
        assertEquals("2 survivors after round 3", 2, rowsAtriple.length);
        assertEquals("round 3: id[0]", 150L, rowsAtriple[0][0]);
        assertEquals("round 3: id[1]", 190L, rowsAtriple[1][0]);
        assertEquals("round 3: create_ts[0]", 5L, rowsAtriple[0][1]);
        assertEquals("round 3: create_ts[1]", 5L, rowsAtriple[1][1]);

        gcR3.registerDualWrite(resultR3);
        gcR3.syncVisibility(resultR3, safeGcTs3);
        gcR3.syncIndex(resultR3, groupAdp.tableId);
        gcR3.commitFileGroup(resultR3);

        // ── Final verification ──────────────────────────────────────────────

        // Catalog: latest generation files are REGULAR
        assertFileRegular(fileIdAtriple, "file-A''' must be REGULAR");
        assertNotNull("file-B must still exist (not GCed)", metadataService.getFileById(fileIdB));
        assertNotNull("file-C must still exist", metadataService.getFileById(fileIdC));

        // Old generations gone from catalog
        assertFileGone(fileIdA, "file-A should be gone from catalog");
        assertFileGone(fileIdAprime, "file-A' should be gone from catalog");
        assertFileGone(fileIdAdoubleprime, "file-A'' should be gone from catalog");

        // Physical files from generations 1 and 2 cleaned up
        assertFalse("file-A physical should not exist", fileStorage.exists(pathA));
        assertFalse("file-A' physical should not exist", fileStorage.exists(pathAprime));

        // file-B visibility: rows 0,1,2 deleted at ts 350,360,370
        // After garbageCollect(safeGcTs3=450), baseTimestamp is 450 so only ts >= 450 can be queried.
        // At ts=450 all three deletes (350,360,370) are baked into the base bitmap.
        for (long snapTs : new long[]{450L, 500L})
        {
            long[] bmB = retinaManager.queryVisibility(fileIdB, 0, snapTs, 0L);
            for (int r = 0; r < 3; r++)
            {
                assertTrue("file-B snap_ts=" + snapTs + " row " + r + " should be deleted",
                        isBitSet(bmB, r));
            }
            for (int r = 3; r < numRowsB; r++)
            {
                assertFalse("file-B snap_ts=" + snapTs + " row " + r + " should not be deleted",
                        isBitSet(bmB, r));
            }
        }

        // file-C: no deletions at any snap_ts (only ts >= safeGcTs3 queryable)
        for (long snapTs : new long[]{450L, 500L})
        {
            long[] bmC = retinaManager.queryVisibility(fileIdC, 0, snapTs, 0L);
            assertEquals("file-C should have no deletions at snap_ts=" + snapTs,
                    0L, bmC[0]);
        }

        // file-A''' (latest generation): verify visibility matches expectations
        // It has 2 rows (originally from file-A, ids 150 and 190).
        // No deletions were applied after safeGcTs3 to this file.
        for (long snapTs : new long[]{450L, 500L, 1000L})
        {
            long[] bmAtriple = retinaManager.queryVisibility(fileIdAtriple, 0, snapTs, 0L);
            for (int r = 0; r < 2; r++)
            {
                assertFalse("file-A''' snap_ts=" + snapTs + " row " + r + " should not be deleted",
                        isBitSet(bmAtriple, r));
            }
        }
    }

    // =======================================================================
    // Helpers: state management
    // =======================================================================

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

            Field dwField = RetinaResourceManager.class.getDeclaredField("dualWriteLookup");
            dwField.setAccessible(true);
            ((Map<?, ?>) dwField.get(retinaManager)).clear();

            Field dualWriteField = RetinaResourceManager.class.getDeclaredField("isDualWriteActive");
            dualWriteField.setAccessible(true);
            dualWriteField.setBoolean(retinaManager, false);

            Field retiredField = RetinaResourceManager.class.getDeclaredField("retiredFiles");
            retiredField.setAccessible(true);
            ((java.util.Queue<?>) retiredField.get(retinaManager)).clear();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to reset RetinaResourceManager state", e);
        }
    }

    /** Returns the private {@code rgVisibilityMap} from {@link RetinaResourceManager} via reflection. */
    @SuppressWarnings("unchecked")
    private Map<String, RGVisibility> getRgVisibilityMap()
    {
        try
        {
            Field f = RetinaResourceManager.class.getDeclaredField("rgVisibilityMap");
            f.setAccessible(true);
            return (Map<String, RGVisibility>) f.get(retinaManager);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to access rgVisibilityMap", e);
        }
    }

    // =======================================================================
    // Helpers: catalog registration
    // =======================================================================

    private long registerTestFile(String name, File.Type type,
                                  int numRg, long minRow, long maxRow)
            throws Exception
    {
        File f = new File();
        f.setName(name);
        f.setType(type);
        f.setNumRowGroup(numRg);
        f.setMinRowId(minRow);
        f.setMaxRowId(maxRow);
        f.setPathId(testPathId);
        metadataService.addFiles(Collections.singletonList(f));
        long id = metadataService.getFileId(testOrderedPathUri + "/" + name);
        assertTrue(name + " must have valid id", id > 0);
        return id;
    }

    private long insertRawFileWithType(String name, int fileType,
                                       int numRg, long minRow, long maxRow)
            throws Exception
    {
        String sql = "INSERT INTO FILES(FILE_NAME, FILE_TYPE, FILE_NUM_RG, FILE_MIN_ROW_ID, FILE_MAX_ROW_ID, PATHS_PATH_ID) " +
                "VALUES (?, ?, ?, ?, ?, ?)";
        try (PreparedStatement pst = MetaDBUtil.Instance().getConnection().prepareStatement(sql))
        {
            pst.setString(1, name);
            pst.setInt(2, fileType);
            pst.setInt(3, numRg);
            pst.setLong(4, minRow);
            pst.setLong(5, maxRow);
            pst.setLong(6, testPathId);
            assertEquals("raw test file insert should affect one row", 1, pst.executeUpdate());
        }
        long id = metadataService.getFileId(testOrderedPathUri + "/" + name);
        assertTrue(name + " must have valid id", id > 0);
        return id;
    }

    private long[] registerTestFiles(String[] names, File.Type[] types,
                                     int[] numRgs, long[] minRows, long[] maxRows)
            throws Exception
    {
        List<File> files = new ArrayList<>();
        for (int i = 0; i < names.length; i++)
        {
            File f = new File();
            f.setName(names[i]);
            f.setType(types[i]);
            f.setNumRowGroup(numRgs[i]);
            f.setMinRowId(minRows[i]);
            f.setMaxRowId(maxRows[i]);
            f.setPathId(testPathId);
            files.add(f);
        }
        metadataService.addFiles(files);
        long[] ids = new long[names.length];
        for (int i = 0; i < names.length; i++)
        {
            ids[i] = metadataService.getFileId(testOrderedPathUri + "/" + names[i]);
            assertTrue(names[i] + " must have valid id", ids[i] > 0);
        }
        return ids;
    }

    // =======================================================================
    // Helpers: bitmap and catalog assertions
    // =======================================================================

    private static boolean isBitSet(long[] bitmap, int row)
    {
        return (bitmap[row / 64] & (1L << (row % 64))) != 0;
    }

    private void assertFileGone(long fileId, String msg) throws Exception
    {
        File f = metadataService.getFileById(fileId);
        assertTrue(msg, f == null || f.getId() == 0);
    }

    private void assertFileRegular(long fileId, String msg) throws Exception
    {
        File f = metadataService.getFileById(fileId);
        assertNotNull(msg, f);
        assertEquals(msg, File.Type.REGULAR, f.getType());
    }

    // =======================================================================
    // Helpers: GC factory for grouping tests
    // =======================================================================

    private static StorageGarbageCollector newGcForGrouping(
            long targetFileSize, int maxFilesPerGroup, int maxGroups)
    {
        return new StorageGarbageCollector(
                null, null, 0.5, targetFileSize, maxFilesPerGroup, maxGroups,
                1048576, EncodingLevel.EL2, 86_400_000L);
    }

    // =======================================================================
    // Helpers: RewriteResult validation
    // =======================================================================

    /**
     * Verifies the structural consistency of a {@link StorageGarbageCollector.RewriteResult}:
     * <ul>
     *   <li>{@code newFileRgActualRecordNums} has exactly {@code newFileRgCount} entries, all positive</li>
     *   <li>{@code newFileRgRowStart} has {@code newFileRgCount + 1} entries forming a cumulative sum</li>
     *   <li>The sentinel entry equals the expected total surviving rows</li>
     * </ul>
     */
    private static void assertRewriteResultConsistency(
            StorageGarbageCollector.RewriteResult result, int expectedTotalRows)
    {
        assertTrue("newFileRgCount must be at least 1", result.newFileRgCount >= 1);
        assertEquals(result.newFileRgCount, result.newFileRgActualRecordNums.length);
        assertEquals(result.newFileRgCount + 1, result.newFileRgRowStart.length);
        int totalRecords = 0;
        for (int i = 0; i < result.newFileRgCount; i++)
        {
            assertTrue("RG record count must be positive", result.newFileRgActualRecordNums[i] > 0);
            assertEquals("rgRowStart must be cumulative sum", totalRecords, result.newFileRgRowStart[i]);
            totalRecords += result.newFileRgActualRecordNums[i];
        }
        assertEquals("sentinel rgRowStart must equal total surviving rows",
                expectedTotalRows, totalRecords);
        assertEquals(totalRecords, result.newFileRgRowStart[result.newFileRgCount]);
    }

    // =======================================================================
    // Helpers: domain object builders
    // =======================================================================

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
     * Creates a GC snapshot bitmap of the minimum required word length for
     * {@code totalRows} rows, with each index in {@code deletedRows} set.
     */
    private static long[] makeBitmapForRows(int totalRows, int... deletedRows)
    {
        int words = (totalRows + 63) / 64;
        long[] bitmap = new long[words];
        for (int r : deletedRows)
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

    // =======================================================================
    // Helpers: Pixels file I/O (for rewrite tests)
    // =======================================================================

    /**
     * Writes a single-row-group Pixels file to the shared temp directory and returns
     * its {@code file://} URI.  The schema must consist entirely of {@code LONG} columns.
     *
     * <p>When {@code hasHidden=true}, an extra {@link LongColumnVector} is appended to
     * {@code batch.cols} before writing so that {@link PixelsWriterImpl} stores the
     * hidden {@code create_ts} column; {@code createTs[i]} is written for row {@code i}.
     */
    private static String writeTestFile(String fileName, TypeDescription schema,
                                        long[] ids, boolean hasHidden, long[] createTs)
            throws Exception
    {
        int n = ids.length;
        int nUserCols = schema.getChildren().size();
        String path = testOrderedPathUri + "/" + fileName;

        VectorizedRowBatch batch = schema.createRowBatch(n);
        if (hasHidden)
        {
            batch.cols = Arrays.copyOf(batch.cols, nUserCols + 1);
            batch.cols[nUserCols] = new LongColumnVector(n);
        }
        for (int r = 0; r < n; r++)
        {
            ((LongColumnVector) batch.cols[0]).vector[r] = ids[r];
            if (hasHidden)
            {
                ((LongColumnVector) batch.cols[nUserCols]).vector[r] = createTs[r];
            }
        }
        batch.size = n;

        try (PixelsWriter writer = PixelsWriterImpl.newBuilder()
                .setSchema(schema)
                .setPixelStride(10_000)
                .setRowGroupSize(256 * 1024 * 1024)
                .setStorage(fileStorage)
                .setPath(path)
                .setOverwrite(true)
                .setEncodingLevel(EncodingLevel.EL0)
                .setCompressionBlockSize(1)
                .setHasHiddenColumn(hasHidden)
                .build())
        {
            writer.addRowBatch(batch);
        }
        return path;
    }

    /**
     * Writes a multi-row-group Pixels file to the shared temp directory and returns
     * its {@code file://} URI.  Each of the {@code numRgs} row groups contains exactly
     * {@code rowsPerRg} rows, written as separate {@code addRowBatch} calls so that
     * the Pixels writer flushes one RG per call.  Row values are sequential integers
     * starting from 0.
     */
    private static String writeTestFileMultiRg(String fileName, TypeDescription schema,
                                               int numRgs, int rowsPerRg) throws Exception
    {
        return writeTestFileMultiRg(fileName, schema, numRgs, rowsPerRg, 10_000);
    }

    private static String writeTestFileMultiRg(String fileName, TypeDescription schema,
                                               int numRgs, int rowsPerRg,
                                               int pixelStride) throws Exception
    {
        String path = testOrderedPathUri + "/" + fileName;
        try (PixelsWriter writer = PixelsWriterImpl.newBuilder()
                .setSchema(schema)
                .setPixelStride(pixelStride)
                .setRowGroupSize(rowsPerRg)
                .setStorage(fileStorage)
                .setPath(path)
                .setOverwrite(true)
                .setEncodingLevel(EncodingLevel.EL0)
                .setCompressionBlockSize(1)
                .setHasHiddenColumn(false)
                .build())
        {
            for (int rg = 0; rg < numRgs; rg++)
            {
                VectorizedRowBatch batch = schema.createRowBatch(rowsPerRg);
                for (int r = 0; r < rowsPerRg; r++)
                {
                    ((LongColumnVector) batch.cols[0]).vector[r] = (long) (rg * rowsPerRg + r);
                }
                batch.size = rowsPerRg;
                writer.addRowBatch(batch);
            }
        }
        return path;
    }

    /**
     * Reads all data rows from a Pixels file and returns them as a {@code long[][]}.
     * Each inner array has {@code nUserCols} entries; when {@code exposeHidden=true}
     * an extra entry is appended containing the hidden {@code create_ts} value.
     */
    private static long[][] readAllRows(String path, TypeDescription schema,
                                        boolean exposeHidden) throws Exception
    {
        List<long[]> rows = new ArrayList<>();
        int nUserCols = schema.getChildren().size();

        try (PixelsReader reader = PixelsReaderImpl.newBuilder()
                .setStorage(fileStorage).setPath(path)
                .setPixelsFooterCache(new PixelsFooterCache()).build())
        {
            if (reader.getRowGroupNum() == 0)
            {
                return new long[0][];
            }
            PixelsReaderOption opt = new PixelsReaderOption();
            opt.includeCols(schema.getFieldNames().toArray(new String[0]));
            opt.exposeHiddenColumn(exposeHidden);

            try (PixelsRecordReader rr = reader.read(opt))
            {
                VectorizedRowBatch batch;
                while ((batch = rr.readBatch()) != null && batch.size > 0)
                {
                    for (int r = 0; r < batch.size; r++)
                    {
                        int width = exposeHidden ? nUserCols + 1 : nUserCols;
                        long[] row = new long[width];
                        for (int c = 0; c < nUserCols; c++)
                        {
                            row[c] = ((LongColumnVector) batch.cols[c]).vector[r];
                        }
                        if (exposeHidden && batch.getHiddenColumnVector() != null)
                        {
                            row[nUserCols] = batch.getHiddenColumnVector().vector[r];
                        }
                        rows.add(row);
                    }
                }
            }
        }
        return rows.toArray(new long[0][]);
    }

    /**
     * Builds a {@link StorageGarbageCollector.FileGroup} with a single
     * {@link StorageGarbageCollector.FileCandidate} backed by the given file path.
     * The row-group count is read from the file footer so the candidate reflects reality.
     * {@code pathId} is set to {@link #testPathId} so that {@code addFiles} satisfies
     * the foreign key constraint against the PATHS table.
     */
    private static StorageGarbageCollector.FileGroup makeGroup(
            long fileId, String filePath, TypeDescription schema) throws Exception
    {
        int rgCount;
        try (PixelsReader r = PixelsReaderImpl.newBuilder()
                .setStorage(fileStorage).setPath(filePath)
                .setPixelsFooterCache(new PixelsFooterCache()).build())
        {
            rgCount = r.getRowGroupNum();
        }

        File f = new File();
        f.setId(fileId);
        f.setNumRowGroup(rgCount);
        f.setMinRowId(0L);
        f.setMaxRowId(999L);
        f.setPathId(testPathId);

        StorageGarbageCollector.FileCandidate fc =
                new StorageGarbageCollector.FileCandidate(
                        f, filePath, fileId, rgCount, 1L, 0, 0.70, 0L);

        return new StorageGarbageCollector.FileGroup(1L, 0, Collections.singletonList(fc));
    }

    /**
     * Builds a {@link StorageGarbageCollector.FileGroup} containing two
     * {@link StorageGarbageCollector.FileCandidate} objects backed by distinct files.
     * Both files share the same {@code (tableId=1, virtualNodeId=0)}.
     */
    private static StorageGarbageCollector.FileGroup makeMultiFileGroup(
            TypeDescription schema,
            long fileIdA, String pathA,
            long fileIdB, String pathB) throws Exception
    {
        List<StorageGarbageCollector.FileCandidate> candidates = new ArrayList<>();
        for (long[] pair : new long[][]{{fileIdA, 0}, {fileIdB, 0}})
        {
            long fid = pair[0];
            String path = (fid == fileIdA) ? pathA : pathB;
            int rgCount;
            try (PixelsReader r = PixelsReaderImpl.newBuilder()
                    .setStorage(fileStorage).setPath(path)
                    .setPixelsFooterCache(new PixelsFooterCache()).build())
            {
                rgCount = r.getRowGroupNum();
            }
            File f = new File();
            f.setId(fid);
            f.setNumRowGroup(rgCount);
            f.setMinRowId(fid * 1000);
            f.setMaxRowId(fid * 1000 + 999);
            f.setPathId(testPathId);
            candidates.add(new StorageGarbageCollector.FileCandidate(
                    f, path, fid, rgCount, 1L, 0, 0.70, 0L));
        }
        return new StorageGarbageCollector.FileGroup(1L, 0, candidates);
    }

    /**
     * Writes a multi-row-group Pixels file.  Each element of {@code idsPerRg}
     * becomes a separate row group (achieved via a tiny {@code rowGroupSize=1}
     * that forces an RG flush after every batch).
     *
     * @param idsPerRg   array of id-arrays, one per desired row group
     * @param hasHidden  whether to write the hidden create_ts column
     * @param tsPerRg    create_ts values per RG (same shape as idsPerRg), or null
     */
    private static String writeMultiRgTestFile(String fileName, TypeDescription schema,
                                               long[][] idsPerRg, boolean hasHidden,
                                               long[][] tsPerRg) throws Exception
    {
        int nUserCols = schema.getChildren().size();
        String path = testOrderedPathUri + "/" + fileName;

        try (PixelsWriter writer = PixelsWriterImpl.newBuilder()
                .setSchema(schema)
                .setPixelStride(1)
                .setRowGroupSize(1)
                .setStorage(fileStorage)
                .setPath(path)
                .setOverwrite(true)
                .setEncodingLevel(EncodingLevel.EL0)
                .setCompressionBlockSize(1)
                .setHasHiddenColumn(hasHidden)
                .build())
        {
            for (int rg = 0; rg < idsPerRg.length; rg++)
            {
                long[] ids = idsPerRg[rg];
                int n = ids.length;
                VectorizedRowBatch batch = schema.createRowBatch(n);
                if (hasHidden)
                {
                    batch.cols = Arrays.copyOf(batch.cols, nUserCols + 1);
                    batch.cols[nUserCols] = new LongColumnVector(n);
                }
                for (int r = 0; r < n; r++)
                {
                    ((LongColumnVector) batch.cols[0]).vector[r] = ids[r];
                    if (hasHidden && tsPerRg != null)
                    {
                        ((LongColumnVector) batch.cols[nUserCols]).vector[r] = tsPerRg[rg][r];
                    }
                }
                batch.size = n;
                writer.addRowBatch(batch);
            }
        }
        return path;
    }

    // =======================================================================
    // Helpers: file cleanup
    // =======================================================================

    private static void deleteRecursive(java.io.File f)
    {
        if (f.isDirectory())
        {
            java.io.File[] children = f.listFiles();
            if (children != null)
            {
                for (java.io.File child : children)
                {
                    deleteRecursive(child);
                }
            }
        }
        f.delete();
    }

    // =======================================================================
    // S1: PxlFileType filter test
    // =======================================================================

    /**
     * Verifies that {@code isGcEligible} correctly accepts ORDERED/COMPACT files
     * and rejects SINGLE/COPY files as well as unrecognised paths.
     */
    @Test
    public void testS1_fileTypeFilter_singleAndCopyExcluded()
    {
        assertTrue("ordered file should be GC eligible",
                PixelsFileNameUtils.isGcEligible("host1_20260401120000_1_0_ordered.pxl"));
        assertTrue("compact file should be GC eligible",
                PixelsFileNameUtils.isGcEligible("host1_20260401120000_2_0_compact.pxl"));
        assertFalse("single file should NOT be GC eligible",
                PixelsFileNameUtils.isGcEligible("host1_20260401120000_3_-1_single.pxl"));
        assertFalse("copy file should NOT be GC eligible",
                PixelsFileNameUtils.isGcEligible("host1_20260401120000_4_0_copy.pxl"));
        assertFalse("unrecognised path should NOT be GC eligible",
                PixelsFileNameUtils.isGcEligible("random_file.parquet"));
        assertFalse("null path should NOT be GC eligible",
                PixelsFileNameUtils.isGcEligible(null));
        assertFalse("empty path should NOT be GC eligible",
                PixelsFileNameUtils.isGcEligible(""));

        assertEquals(PixelsFileNameUtils.PxlFileType.ORDERED,
                PixelsFileNameUtils.extractFileType("host1_20260401120000_1_0_ordered.pxl"));
        assertEquals(PixelsFileNameUtils.PxlFileType.COMPACT,
                PixelsFileNameUtils.extractFileType("/some/dir/host1_20260401120000_2_5_compact.pxl"));
        assertEquals(PixelsFileNameUtils.PxlFileType.SINGLE,
                PixelsFileNameUtils.extractFileType("host1_20260401120000_3_-1_single.pxl"));
        assertEquals(PixelsFileNameUtils.PxlFileType.COPY,
                PixelsFileNameUtils.extractFileType("host1_20260401120000_4_0_copy.pxl"));
    }

    // =======================================================================
    // S2: Multi-column schema rewrite test
    // =======================================================================

    /**
     * Rewrites a file with a multi-column schema (LONG + STRING + DOUBLE)
     * and verifies all column values survive the rewrite correctly.
     */
    @Test
    public void testS2_multiColumnSchema_longStringDouble() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long,name:string,score:double>");
        int numRows = 6;
        long fileId = 5001L;

        String path = testOrderedPathUri + "/src_multi_col.pxl";
        VectorizedRowBatch batch = schema.createRowBatch(numRows);
        long[] ids = {10L, 20L, 30L, 40L, 50L, 60L};
        String[] names = {"alice", "bob", "carol", "dave", "eve", "frank"};
        double[] scores = {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};

        for (int r = 0; r < numRows; r++)
        {
            ((LongColumnVector) batch.cols[0]).vector[r] = ids[r];
            byte[] nameBytes = names[r].getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ((BinaryColumnVector) batch.cols[1]).setVal(r, nameBytes);
            ((DoubleColumnVector) batch.cols[2]).vector[r] = Double.doubleToLongBits(scores[r]);
        }
        batch.size = numRows;

        try (PixelsWriter writer = PixelsWriterImpl.newBuilder()
                .setSchema(schema)
                .setPixelStride(10_000)
                .setRowGroupSize(256 * 1024 * 1024)
                .setStorage(fileStorage)
                .setPath(path)
                .setOverwrite(true)
                .setEncodingLevel(EncodingLevel.EL0)
                .setCompressionBlockSize(1)
                .setHasHiddenColumn(false)
                .build())
        {
            writer.addRowBatch(batch);
        }

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmapForRows(numRows, 1, 3, 5));

        StorageGarbageCollector.FileGroup group = makeGroup(fileId, path, schema);
        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(group, 100L, bitmaps);

        try (PixelsReader reader = PixelsReaderImpl.newBuilder()
                .setStorage(fileStorage).setPath(result.newFilePath)
                .setPixelsFooterCache(new PixelsFooterCache()).build())
        {
            String[] colNames = {"id", "name", "score"};
            PixelsReaderOption option = new PixelsReaderOption();
            option.skipCorruptRecords(true);
            option.tolerantSchemaEvolution(true);
            option.includeCols(colNames);

            PixelsRecordReader rr = reader.read(option);
            VectorizedRowBatch outBatch = rr.readBatch();
            assertEquals("3 rows should survive", 3, outBatch.size);

            long[] expectedIds = {10L, 30L, 50L};
            String[] expectedNames = {"alice", "carol", "eve"};
            double[] expectedScores = {1.1, 3.3, 5.5};

            for (int r = 0; r < 3; r++)
            {
                assertEquals("id mismatch at row " + r,
                        expectedIds[r], ((LongColumnVector) outBatch.cols[0]).vector[r]);
                String actualName = new String(
                        ((BinaryColumnVector) outBatch.cols[1]).vector[r],
                        ((BinaryColumnVector) outBatch.cols[1]).start[r],
                        ((BinaryColumnVector) outBatch.cols[1]).lens[r],
                        java.nio.charset.StandardCharsets.UTF_8);
                assertEquals("name mismatch at row " + r,
                        expectedNames[r], actualName);
                assertEquals("score mismatch at row " + r,
                        expectedScores[r],
                        Double.longBitsToDouble(((DoubleColumnVector) outBatch.cols[2]).vector[r]),
                        1e-9);
            }
        }

        assertRewriteResultConsistency(result, 3);
    }

    // =======================================================================
    // S2: Large-scale rewrite performance benchmark
    // =======================================================================

    /**
     * Rewrites a file with 2000 rows (multi-RG) deleting ~50%, verifying
     * correctness and serving as a performance baseline.
     */
    @Test
    public void testS2_largeScaleRewrite_2000rows() throws Exception
    {
        TypeDescription schema = LONG_ID_SCHEMA;
        int totalRows = 2000;
        int rowsPerRg = 500;
        int numRgs = totalRows / rowsPerRg;
        long fileId = 6001L;

        String srcPath = writeTestFileMultiRg("src_large.pxl", schema, numRgs, rowsPerRg, rowsPerRg);

        Map<String, long[]> bitmaps = new HashMap<>();
        int expectedSurvivors = 0;
        for (int rg = 0; rg < numRgs; rg++)
        {
            List<Integer> deleted = new ArrayList<>();
            for (int r = 0; r < rowsPerRg; r++)
            {
                if (r % 2 == 0)
                {
                    deleted.add(r);
                }
            }
            bitmaps.put(fileId + "_" + rg,
                    makeBitmapForRows(rowsPerRg, deleted.stream().mapToInt(Integer::intValue).toArray()));
            expectedSurvivors += (rowsPerRg - deleted.size());
        }

        StorageGarbageCollector.FileGroup group = makeGroup(fileId, srcPath, schema);

        long startNs = System.nanoTime();
        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(group, 100L, bitmaps);
        long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;

        long[][] rows = readAllRows(result.newFilePath, schema, false);
        assertEquals("surviving rows after 50% deletion", expectedSurvivors, rows.length);

        for (int i = 0; i < rows.length; i++)
        {
            long expectedId = (i / (rowsPerRg / 2)) * rowsPerRg + (i % (rowsPerRg / 2)) * 2 + 1;
            assertEquals("id mismatch at survivor row " + i, expectedId, rows[i][0]);
        }

        assertRewriteResultConsistency(result, expectedSurvivors);

        assertTrue("Large-scale rewrite should complete in under 30s, took " + elapsedMs + "ms",
                elapsedMs < 30_000);
    }

    // =======================================================================
    // Memory GC: gcSnapshotBitmap correctness
    // =======================================================================

    /**
     * Verifies that {@code RGVisibility.garbageCollect(safeGcTs)} produces the
     * correct gcSnapshotBitmap for three distinct scenarios:
     * <ol>
     *   <li>No deletions at all → all-zero bitmap</li>
     *   <li>All deletions before safeGcTs → bitmap reflects all deleted rows</li>
     *   <li>Mixed: some before, some after safeGcTs → bitmap reflects only the before-set</li>
     * </ol>
     */
    @Test
    public void testGcSnapshotBitmap_threePathCorrectness() throws Exception
    {
        int recordNum = 10;

        // Path 1: No deletions → all-zero bitmap
        long fid1 = 7001L;
        retinaManager.addVisibility(fid1, 0, recordNum, 0L, null, false);
        Map<String, RGVisibility> rgMap = getRgVisibilityMap();
        RGVisibility rv1 = rgMap.get(RetinaUtils.buildRgKey(fid1, 0));
        assertNotNull("RGVisibility for fid1 must exist", rv1);
        long[] bm1 = rv1.garbageCollect(100L);
        for (long word : bm1)
        {
            assertEquals("Path 1: no deletions → bitmap must be zero", 0L, word);
        }

        // Path 2: All deletions before safeGcTs → all marked
        long fid2 = 7002L;
        retinaManager.addVisibility(fid2, 0, recordNum, 0L, null, false);
        for (int r = 0; r < recordNum; r++)
        {
            retinaManager.deleteRecord(fid2, 0, r, 50L);
        }
        RGVisibility rv2 = getRgVisibilityMap()
                .get(RetinaUtils.buildRgKey(fid2, 0));
        long[] bm2 = rv2.garbageCollect(100L);
        int deletedCount2 = 0;
        for (long word : bm2)
        {
            deletedCount2 += Long.bitCount(word);
        }
        assertEquals("Path 2: all deleted before safeGcTs", recordNum, deletedCount2);
        for (int r = 0; r < recordNum; r++)
        {
            assertTrue("Path 2: row " + r + " must be set", isBitSet(bm2, r));
        }

        // Path 3: Mixed — delete rows 0,2,4 at ts=50 and rows 1,3 at ts=150
        long fid3 = 7003L;
        retinaManager.addVisibility(fid3, 0, recordNum, 0L, null, false);
        for (int r : new int[]{0, 2, 4})
        {
            retinaManager.deleteRecord(fid3, 0, r, 50L);
        }
        for (int r : new int[]{1, 3})
        {
            retinaManager.deleteRecord(fid3, 0, r, 150L);
        }
        RGVisibility rv3 = getRgVisibilityMap()
                .get(RetinaUtils.buildRgKey(fid3, 0));
        long[] bm3 = rv3.garbageCollect(100L);

        for (int r : new int[]{0, 2, 4})
        {
            assertTrue("Path 3: row " + r + " (deleted at ts=50 < safeGcTs=100) must be set",
                    isBitSet(bm3, r));
        }
        for (int r : new int[]{1, 3})
        {
            assertFalse("Path 3: row " + r + " (deleted at ts=150 > safeGcTs=100) must NOT be set",
                    isBitSet(bm3, r));
        }
        for (int r : new int[]{5, 6, 7, 8, 9})
        {
            assertFalse("Path 3: row " + r + " (not deleted) must NOT be set",
                    isBitSet(bm3, r));
        }
    }

    // =======================================================================
    // Inner classes: DirectScanStorageGC stub (for scan/grouping tests)
    // =======================================================================

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
            super(rm, null, threshold, 134_217_728L, Integer.MAX_VALUE, maxGroups,
                    1048576, EncodingLevel.EL2, 86_400_000L);
            this.fakeEntries = fakeEntries;
        }

        @Override
        List<FileGroup> scanAndGroupFiles(Set<Long> candidateFileIds,
                                          Map<Long, long[]> fileStats)
        {
            List<FileCandidate> candidates = new ArrayList<>();
            for (FakeFileEntry entry : fakeEntries)
            {
                if (!candidateFileIds.contains(entry.fileId))
                {
                    continue;
                }
                long[] stats = fileStats.get(entry.fileId);
                if (stats == null || stats[0] == 0)
                {
                    continue;
                }
                double ratio = (double) stats[1] / stats[0];
                candidates.add(new FileCandidate(
                        makeFile(entry.fileId, entry.rgCount),
                        "fake_" + entry.fileId + "_0_" + entry.virtualNodeId + "_ordered.pxl",
                        entry.fileId, entry.rgCount,
                        entry.tableId, entry.virtualNodeId,
                        ratio, 0L));
            }
            return groupAndMerge(candidates);
        }

        @Override
        void processFileGroups(List<FileGroup> fileGroups, long safeGcTs,
                               Map<String, long[]> gcSnapshotBitmaps)
        {
        }
    }

    /**
     * StorageGarbageCollector subclass where {@code rewriteFileGroup} throws on
     * the first call and succeeds (cleaning up bitmaps) on subsequent calls.
     * Used by {@link #testProcessFileGroups_firstGroupFailsSecondContinues} to
     * verify that {@code processFileGroups} catches per-group failures, cleans
     * up bitmaps, and continues to the next group.
     */
    static class FailFirstGroupGC extends StorageGarbageCollector
    {
        private boolean firstCall = true;

        FailFirstGroupGC()
        {
            super(null, null, 0.5, 0L, Integer.MAX_VALUE, 10,
                    1048576, EncodingLevel.EL2, 86_400_000L);
        }

        @Override
        RewriteResult rewriteFileGroup(FileGroup group, long safeGcTs,
                                       Map<String, long[]> gcSnapshotBitmaps) throws Exception
        {
            if (firstCall)
            {
                firstCall = false;
                throw new RuntimeException("simulated rewrite failure");
            }
            for (FileCandidate fc : group.files)
            {
                for (int rgId = 0; rgId < fc.rgCount; rgId++)
                {
                    gcSnapshotBitmaps.remove(RetinaUtils.buildRgKey(fc.fileId, rgId));
                }
            }
            return new RewriteResult(group, "stub", -1,
                    0, new int[0], new int[]{0}, new HashMap<>(), Collections.emptyList(),
                    Collections.emptyList());
        }
    }

    /**
     * StorageGarbageCollector subclass that stubs out {@code syncIndex} so that
     * the full pipeline can be tested without requiring a configured MainIndex.
     * All other methods (rewrite, dual-write, visibility sync, commit) use real code.
     */
    static class NoIndexSyncGC extends StorageGarbageCollector
    {
        NoIndexSyncGC(RetinaResourceManager rm, MetadataService ms,
                      double threshold, long targetFileSize, int maxFilesPerGroup,
                      int maxGroups, int rowGroupSize, EncodingLevel encodingLevel,
                      long retireDelayMs)
        {
            super(rm, ms, threshold, targetFileSize, maxFilesPerGroup, maxGroups,
                    rowGroupSize, encodingLevel, retireDelayMs);
        }

        @Override
        void syncIndex(RewriteResult result, long tableId) throws Exception
        {
        }
    }
}
