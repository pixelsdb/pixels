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
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link StorageGarbageCollector}, covering scan/grouping, data rewrite,
 * and placeholders for dual-write, visibility sync, index update, atomic switch,
 * and end-to-end integration.
 *
 * <p>All tests use real {@link RetinaResourceManager} (with JNI/C++ native library)
 * and real {@link MetadataService} (requires a running metadata server).
 * Rewrite tests write Pixels files to a local temp directory using {@code file://}
 * URIs resolved by {@link io.pixelsdb.pixels.storage.localfs.LocalFS}.
 */
public class TestStorageGarbageCollector
{
    // -----------------------------------------------------------------------
    // Class-level constants & fields
    // -----------------------------------------------------------------------

    private static final String TEST_SCHEMA = "gc_test";
    private static final String TEST_TABLE = "gc_test_tbl";

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
        gc = new StorageGarbageCollector(retinaManager, metadataService, 0.5, 134_217_728L, Integer.MAX_VALUE, 10,
                1048576, EncodingLevel.EL2);
    }

    @After
    public void tearDown()
    {
        resetManagerState();
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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, 0L, Integer.MAX_VALUE, 10,
                1048576, EncodingLevel.EL2);

        List<StorageGarbageCollector.FileCandidate> candidates = Arrays.asList(
                new StorageGarbageCollector.FileCandidate(makeFile(1, 1), "f1", 1, 1, 1L, 0, 100, 0.60, 0L),
                new StorageGarbageCollector.FileCandidate(makeFile(2, 1), "f2", 2, 1, 1L, 1, 100, 0.70, 0L),
                new StorageGarbageCollector.FileCandidate(makeFile(3, 1), "f3", 3, 1, 2L, 0, 100, 0.80, 0L)
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
                null, null, 0.5, 0L, Integer.MAX_VALUE, 10,
                1048576, EncodingLevel.EL2);

        List<StorageGarbageCollector.FileCandidate> candidates = Arrays.asList(
                new StorageGarbageCollector.FileCandidate(makeFile(1, 1), "f1", 1, 1, 1L, 5, 100, 0.60, 0L),
                new StorageGarbageCollector.FileCandidate(makeFile(2, 1), "f2", 2, 1, 1L, 5, 100, 0.80, 0L)
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
                null, null, 0.5, 0L, Integer.MAX_VALUE, max,
                1048576, EncodingLevel.EL2);

        // Build 5 groups with different tableIds and clear invalidRatios (0.55..0.99)
        List<StorageGarbageCollector.FileCandidate> candidates = new ArrayList<>();
        for (int i = 0; i < 5; i++)
        {
            candidates.add(new StorageGarbageCollector.FileCandidate(
                    makeFile(i, 1), "f" + i, i, 1, (long) (i + 10), 0, 100, 0.55 + i * 0.11, 0L));
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
                null, null, 0.5, 0L, Integer.MAX_VALUE, 10,
                1048576, EncodingLevel.EL2);
        List<StorageGarbageCollector.FileGroup> groups =
                gc.groupAndMerge(Collections.emptyList());
        assertTrue("empty candidates → empty groups", groups.isEmpty());
    }

    /**
     * Greedy splitting: three files sharing the same {@code (tableId, virtualNodeId)},
     * each with 60 MB effective data against a 100 MB target file size.
     * Files A+B (60+60=120 > 100) → first group is A alone (60), then B+C (60+60=120 > 100)
     * → second group is B alone, third group is C alone.
     * Actually: A fills 60, B would push to 120 > 100 so A is flushed first,
     * then B fills 60, C would push to 120 > 100 so B is flushed, then C alone.
     * Result: 3 groups of 1 file each.
     */
    @Test
    public void testGroupAndMerge_greedySplitBySize()
    {
        long target = 100 * 1024 * 1024L; // 100 MB
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, target, Integer.MAX_VALUE, 10,
                1048576, EncodingLevel.EL2);

        // Each file is 100 MB on disk with 40% deleted → 60 MB effective
        List<StorageGarbageCollector.FileCandidate> candidates = Arrays.asList(
                new StorageGarbageCollector.FileCandidate(
                        makeFile(1, 1), "f1", 1, 1, 1L, 0, 1000, 0.40, 100 * 1024 * 1024L),
                new StorageGarbageCollector.FileCandidate(
                        makeFile(2, 1), "f2", 2, 1, 1L, 0, 1000, 0.40, 100 * 1024 * 1024L),
                new StorageGarbageCollector.FileCandidate(
                        makeFile(3, 1), "f3", 3, 1, 1L, 0, 1000, 0.40, 100 * 1024 * 1024L)
        );

        List<StorageGarbageCollector.FileGroup> groups = gc.groupAndMerge(candidates);

        assertEquals("3 files × 60MB effective each, target 100MB → 3 single-file groups", 3, groups.size());
        for (StorageGarbageCollector.FileGroup g : groups)
        {
            assertEquals(1, g.files.size());
        }
    }

    /**
     * Greedy splitting: two small files fit within the target size together.
     * Each is 30 MB effective → combined 60 MB < 100 MB target → one group.
     */
    @Test
    public void testGroupAndMerge_greedyMergeFitsInTarget()
    {
        long target = 100 * 1024 * 1024L; // 100 MB
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, target, Integer.MAX_VALUE, 10,
                1048576, EncodingLevel.EL2);

        List<StorageGarbageCollector.FileCandidate> candidates = Arrays.asList(
                new StorageGarbageCollector.FileCandidate(
                        makeFile(1, 1), "f1", 1, 1, 1L, 0, 500, 0.70, 100 * 1024 * 1024L),
                new StorageGarbageCollector.FileCandidate(
                        makeFile(2, 1), "f2", 2, 1, 1L, 0, 500, 0.70, 100 * 1024 * 1024L)
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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, target, Integer.MAX_VALUE, 10,
                1048576, EncodingLevel.EL2);

        List<StorageGarbageCollector.FileCandidate> candidates = Arrays.asList(
                new StorageGarbageCollector.FileCandidate(
                        makeFile(1, 1), "f1", 1, 1, 1L, 0, 2000, 0.10, 200 * 1024 * 1024L),
                new StorageGarbageCollector.FileCandidate(
                        makeFile(2, 1), "f2", 2, 1, 1L, 0, 500, 0.50, 50 * 1024 * 1024L)
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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, target, Integer.MAX_VALUE, 10,
                1048576, EncodingLevel.EL2);

        // fileSizeBytes = 0 (unknown) — no splitting occurs
        List<StorageGarbageCollector.FileCandidate> candidates = Arrays.asList(
                new StorageGarbageCollector.FileCandidate(
                        makeFile(1, 1), "f1", 1, 1, 1L, 0, 1000, 0.60, 0L),
                new StorageGarbageCollector.FileCandidate(
                        makeFile(2, 1), "f2", 2, 1, 1L, 0, 1000, 0.60, 0L)
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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, 0L, 2, 100,
                1048576, EncodingLevel.EL2);

        List<StorageGarbageCollector.FileCandidate> candidates = new ArrayList<>();
        for (int i = 0; i < 5; i++)
        {
            candidates.add(new StorageGarbageCollector.FileCandidate(
                    makeFile(i + 1, 1), "f" + i, i + 1, 1, 1L, 0, 100, 0.90 - i * 0.05, 0L));
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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, hugeTarget, 3, 100,
                1048576, EncodingLevel.EL2);

        List<StorageGarbageCollector.FileCandidate> candidates = new ArrayList<>();
        for (int i = 0; i < 6; i++)
        {
            candidates.add(new StorageGarbageCollector.FileCandidate(
                    makeFile(i + 1, 1), "f" + i, i + 1, 1, 1L, 0, 100, 0.70,
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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, target, Integer.MAX_VALUE, 100,
                1048576, EncodingLevel.EL2);

        List<StorageGarbageCollector.FileCandidate> candidates = new ArrayList<>();
        for (int i = 0; i < 3; i++)
        {
            candidates.add(new StorageGarbageCollector.FileCandidate(
                    makeFile(i + 1, 1), "f" + i, i + 1, 1, 1L, 0, 1000, 0.40,
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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, 0L, Integer.MAX_VALUE, 10,
                1048576, EncodingLevel.EL2);

        List<StorageGarbageCollector.FileCandidate> candidates = Collections.singletonList(
                new StorageGarbageCollector.FileCandidate(makeFile(1, 1), "f1", 1, 1, 1L, 0, 100, 0.80, 0L));

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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, target, Integer.MAX_VALUE, 10,
                1048576, EncodingLevel.EL2);

        List<StorageGarbageCollector.FileCandidate> candidates = Arrays.asList(
                new StorageGarbageCollector.FileCandidate(
                        makeFile(1, 1), "f1", 1, 1, 1L, 0, 1000, 0.50, 100 * 1024 * 1024L),
                new StorageGarbageCollector.FileCandidate(
                        makeFile(2, 1), "f2", 2, 1, 1L, 0, 1000, 0.50, 100 * 1024 * 1024L)
        );

        List<StorageGarbageCollector.FileGroup> groups = gc.groupAndMerge(candidates);

        assertEquals("effective == target → not oversized; A+B > target → 2 groups", 2, groups.size());
        for (StorageGarbageCollector.FileGroup g : groups)
        {
            assertEquals(1, g.files.size());
        }
    }

    /**
     * With {@code maxFilesPerGroup=1}, every file must form its own group
     * regardless of size.
     */
    @Test
    public void testGroupAndMerge_maxFilesPerGroupOne()
    {
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, 0L, 1, 100,
                1048576, EncodingLevel.EL2);

        List<StorageGarbageCollector.FileCandidate> candidates = Arrays.asList(
                new StorageGarbageCollector.FileCandidate(
                        makeFile(1, 1), "f1", 1, 1, 1L, 0, 100, 0.90, 0L),
                new StorageGarbageCollector.FileCandidate(
                        makeFile(2, 1), "f2", 2, 1, 1L, 0, 100, 0.80, 0L),
                new StorageGarbageCollector.FileCandidate(
                        makeFile(3, 1), "f3", 3, 1, 1L, 0, 100, 0.70, 0L)
        );

        List<StorageGarbageCollector.FileGroup> groups = gc.groupAndMerge(candidates);

        assertEquals("maxFilesPerGroup=1 → every file its own group", 3, groups.size());
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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, 0L, Integer.MAX_VALUE, 10,
                1048576, EncodingLevel.EL2);

        List<StorageGarbageCollector.FileCandidate> candidates = Arrays.asList(
                new StorageGarbageCollector.FileCandidate(
                        makeFile(1, 1), "f1", 1, 1, 1L, 0, 100, 0.70, 0L),
                new StorageGarbageCollector.FileCandidate(
                        makeFile(2, 1), "f2", 2, 1, 2L, 0, 100, 0.70, 0L),
                new StorageGarbageCollector.FileCandidate(
                        makeFile(3, 1), "f3", 3, 1, 3L, 0, 100, 0.70, 0L)
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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, 0L, 0, 10,
                1048576, EncodingLevel.EL2);

        List<StorageGarbageCollector.FileCandidate> candidates = new ArrayList<>();
        for (int i = 0; i < 5; i++)
        {
            candidates.add(new StorageGarbageCollector.FileCandidate(
                    makeFile(i + 1, 1), "f" + i, i + 1, 1, 1L, 0, 100, 0.80 - i * 0.05, 0L));
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

    /**
     * When all files are below the threshold, {@code runStorageGC} is a no-op:
     * the bitmaps map must remain unchanged.
     */
    @Test
    public void testRunStorageGC_noBitmapTrimWhenNoCandidates()
    {
        long fileId = 66003L;

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmap(100, 20));

        Map<Long, long[]> fileStats = new HashMap<>();
        fileStats.put(fileId, makeRgStats(100, 20));

        DirectScanStorageGC gc = new DirectScanStorageGC(
                retinaManager, 0.5, 10,
                Collections.singletonList(new FakeFileEntry(fileId, 1, 1L, 0)));

        gc.runStorageGC(300L, fileStats, bitmaps);

        assertTrue("bitmap must be unchanged when no candidates",
                bitmaps.containsKey(fileId + "_0"));
        assertEquals(1, bitmaps.size());
    }

    // =======================================================================
    // Section 4: runStorageGC end-to-end scan → process
    // =======================================================================

    /**
     * When no file exceeds the threshold (all invalidRatio <= threshold),
     * {@code runStorageGC} must be a no-op: {@code gcSnapshotBitmaps} unchanged.
     */
    @Test
    public void testRunStorageGC_noopWhenNoCandidates()
    {
        long fileId = 55001L;

        // File-level stats: 30% deletion → below 0.5 threshold → no candidates
        Map<Long, long[]> fileStats = new HashMap<>();
        fileStats.put(fileId, makeRgStats(100, 30));

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmap(100, 30));

        DirectScanStorageGC gc = new DirectScanStorageGC(
                retinaManager, 0.5, 10,
                Collections.singletonList(new FakeFileEntry(fileId, 1, 1L, 0)));

        gc.runStorageGC(100L, fileStats, bitmaps);

        assertTrue("bitmap must be unchanged when no candidates",
                bitmaps.containsKey(fileId + "_0"));
        assertEquals("bitmap map size must stay 1", 1, bitmaps.size());
    }

    /**
     * When {@code runStorageGC} finds candidates, non-candidate bitmaps must be trimmed
     * from {@code gcSnapshotBitmaps} while candidate bitmaps are retained for subsequent phases.
     */
    @Test
    public void testRunStorageGC_trimsBitmapsForCandidates()
    {
        long candidateFileId    = 56001L;  // 70 % deleted → above threshold
        long nonCandidateFileId = 56002L;  // 20 % deleted → below threshold

        Map<Long, long[]> fileStats = new HashMap<>();
        fileStats.put(candidateFileId, makeRgStats(100, 70));
        fileStats.put(nonCandidateFileId, makeRgStats(100, 20));

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(candidateFileId    + "_0", makeBitmap(100, 70));
        bitmaps.put(nonCandidateFileId + "_0", makeBitmap(100, 20));

        DirectScanStorageGC gc = new DirectScanStorageGC(
                retinaManager, 0.5, 10,
                Arrays.asList(
                        new FakeFileEntry(candidateFileId,    1, 1L, 0),
                        new FakeFileEntry(nonCandidateFileId, 1, 1L, 0)));

        gc.runStorageGC(200L, fileStats, bitmaps);

        assertTrue("candidate bitmap must be retained for rewrite",
                bitmaps.containsKey(candidateFileId + "_0"));
        assertFalse("non-candidate bitmap must be trimmed",
                bitmaps.containsKey(nonCandidateFileId + "_0"));
    }

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
                        makeFile(fileIdA, 1), "fake_a", fileIdA, 1, 1L, 0, 100, 0.60, 0L)));
        StorageGarbageCollector.FileGroup groupB = new StorageGarbageCollector.FileGroup(
                2L, 0, Collections.singletonList(
                new StorageGarbageCollector.FileCandidate(
                        makeFile(fileIdB, 1), "fake_b", fileIdB, 1, 2L, 0, 100, 0.60, 0L)));

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
     * Basic bitmap filtering: rows 0, 1, 2 are marked deleted; rows 3-9 survive.
     * Verifies output has 7 rows with the expected id values and that the bitmap
     * entry is removed from {@code gcSnapshotBitmaps} after the rewrite.
     */
    @Test
    public void testBasicFiltering() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long[] ids = {0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L};
        long fileId = 1L;
        String srcPath = writeTestFile("src_basic.pxl", schema, ids, false, null);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmapForRows(10, 0, 1, 2));

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(makeGroup(fileId, srcPath, schema), 100L, bitmaps);

        long[][] rows = readAllRows(result.newFilePath, schema, false);
        assertEquals("7 rows should survive", 7, rows.length);
        for (int i = 0; i < 7; i++)
        {
            assertEquals("id mismatch at row " + i, i + 3L, rows[i][0]);
        }
        assertFalse("gcSnapshotBitmaps entry must be removed after rewrite",
                bitmaps.containsKey(fileId + "_0"));

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
        assertEquals("sentinel rgRowStart must equal total surviving rows", 7, totalRecords);
        assertEquals(totalRecords, result.newFileRgRowStart[result.newFileRgCount]);
    }

    /**
     * When there is no bitmap entry for a source file RG, {@code isBitmapBitSet}
     * returns false for every row (null bitmap ≡ no deletions) and all rows pass
     * through unchanged.
     */
    @Test
    public void testNullBitmapKeepsAllRows() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
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
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
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

    /**
     * When {@code hasHiddenColumn=true} the hidden {@code create_ts} column must
     * be preserved in the rewritten file.  The values read back for surviving rows
     * must exactly match those written into the source file.
     */
    @Test
    public void testHiddenColumnPreserved() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long[] ids      = {0L,    1L,    2L,    3L,    4L};
        long[] createTs = {1000L, 1001L, 1002L, 1003L, 1004L};
        long fileId = 4L;
        String srcPath = writeTestFile("src_hidden.pxl", schema, ids, true, createTs);

        // Delete rows 1 and 3; survivors: rows 0, 2, 4
        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmapForRows(5, 1, 3));

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(makeGroup(fileId, srcPath, schema), 100L, bitmaps);

        long[][] rows = readAllRows(result.newFilePath, schema, true);
        assertEquals("3 rows should survive", 3, rows.length);

        long[] expectedIds = {0L, 2L, 4L};
        long[] expectedTs  = {1000L, 1002L, 1004L};
        for (int i = 0; i < 3; i++)
        {
            assertEquals("id mismatch at row "      + i, expectedIds[i], rows[i][0]);
            assertEquals("create_ts mismatch at row " + i, expectedTs[i], rows[i][1]);
        }

        assertRewriteResultConsistency(result, 3);
    }

    /**
     * Forward mapping correctness: rows 0, 2, 4 are deleted; surviving rows 1, 3, 5
     * must be mapped to new global offsets 0, 1, 2 respectively, and the deleted
     * rows must map to -1.
     */
    @Test
    public void testForwardMappingCorrect() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long[] ids = {0L, 1L, 2L, 3L, 4L, 5L};
        long fileId = 5L;
        String srcPath = writeTestFile("src_fwd.pxl", schema, ids, false, null);

        // Delete alternating rows 0, 2, 4
        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmapForRows(6, 0, 2, 4));

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(makeGroup(fileId, srcPath, schema), 100L, bitmaps);

        assertNotNull("forwardRgMappings must contain the source fileId",
                result.forwardRgMappings.get(fileId));
        int[] fwdMapping = result.forwardRgMappings.get(fileId).get(0);
        assertNotNull("fwdMapping for rg0 must be present", fwdMapping);

        assertEquals("row 0 deleted → -1", -1, fwdMapping[0]);
        assertEquals("row 1 survives → 0",  0, fwdMapping[1]);
        assertEquals("row 2 deleted → -1", -1, fwdMapping[2]);
        assertEquals("row 3 survives → 1",  1, fwdMapping[3]);
        assertEquals("row 4 deleted → -1", -1, fwdMapping[4]);
        assertEquals("row 5 survives → 2",  2, fwdMapping[5]);

        assertRewriteResultConsistency(result, 3);
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
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
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
     * Rewrites a multi-file FileGroup where both files carry hidden columns.
     * Verifies that create_ts values are preserved across files in the output.
     */
    @Test
    public void testMultiFileGroupRewrite_hiddenColumn() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long fileIdA = 12L;
        long fileIdB = 13L;
        long[] idsA = {10L, 11L, 12L};
        long[] tsA  = {500L, 501L, 502L};
        long[] idsB = {20L, 21L, 22L};
        long[] tsB  = {600L, 601L, 602L};

        String pathA = writeTestFile("src_multi_hidden_a.pxl", schema, idsA, true, tsA);
        String pathB = writeTestFile("src_multi_hidden_b.pxl", schema, idsB, true, tsB);

        // Delete row 1 from A and row 0 from B
        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileIdA + "_0", makeBitmapForRows(3, 1));
        bitmaps.put(fileIdB + "_0", makeBitmapForRows(3, 0));

        StorageGarbageCollector.FileGroup group =
                makeMultiFileGroup(schema, fileIdA, pathA, fileIdB, pathB);

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(group, 100L, bitmaps);

        long[][] rows = readAllRows(result.newFilePath, schema, true);
        assertEquals("4 rows should survive", 4, rows.length);
        long[] expectedIds = {10L, 12L, 21L, 22L};
        long[] expectedTs  = {500L, 502L, 601L, 602L};
        for (int i = 0; i < 4; i++)
        {
            assertEquals("id mismatch at row " + i, expectedIds[i], rows[i][0]);
            assertEquals("create_ts mismatch at row " + i, expectedTs[i], rows[i][1]);
        }

        assertRewriteResultConsistency(result, 4);
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
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
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
     * Multi-RG source file with hidden column.  Verifies create_ts is preserved
     * across row group boundaries in the rewritten output.
     */
    @Test
    public void testMultiRgRewrite_hiddenColumn() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long fileId = 21L;

        String srcPath = writeMultiRgTestFile("src_multi_rg_hidden.pxl", schema,
                new long[][]{{0L, 1L, 2L}, {3L, 4L, 5L}},
                true,
                new long[][]{{100L, 101L, 102L}, {200L, 201L, 202L}});

        // Delete row 0 from RG0, row 2 from RG1
        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmapForRows(3, 0));
        bitmaps.put(fileId + "_1", makeBitmapForRows(3, 2));

        StorageGarbageCollector.FileGroup group = makeGroup(fileId, srcPath, schema);

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(group, 100L, bitmaps);

        long[][] rows = readAllRows(result.newFilePath, schema, true);
        assertEquals("4 rows should survive", 4, rows.length);
        long[] expectedIds = {1L, 2L, 3L, 4L};
        long[] expectedTs  = {101L, 102L, 200L, 201L};
        for (int i = 0; i < 4; i++)
        {
            assertEquals("id mismatch at row " + i, expectedIds[i], rows[i][0]);
            assertEquals("create_ts mismatch at row " + i, expectedTs[i], rows[i][1]);
        }

        assertRewriteResultConsistency(result, 4);
    }

    // =======================================================================
    // Section 5c: edge-case rewrite tests
    // =======================================================================

    /**
     * Single-row file, row is deleted → all-deleted path.
     */
    @Test
    public void testSingleRowFileRewrite_deleted() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long fileId = 30L;
        String srcPath = writeTestFile("src_single_del.pxl", schema, new long[]{42L}, false, null);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmapForRows(1, 0));

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(makeGroup(fileId, srcPath, schema), 100L, bitmaps);

        assertEquals(-1, result.newFileId);
        assertEquals(0, result.newFileRgCount);
        int[] fwd = result.forwardRgMappings.get(fileId).get(0);
        assertEquals(1, fwd.length);
        assertEquals(-1, fwd[0]);
    }

    /**
     * Single-row file, row survives → output has exactly 1 row.
     */
    @Test
    public void testSingleRowFileRewrite_survives() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long fileId = 31L;
        String srcPath = writeTestFile("src_single_surv.pxl", schema, new long[]{99L}, false, null);

        Map<String, long[]> bitmaps = new HashMap<>();

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(makeGroup(fileId, srcPath, schema), 100L, bitmaps);

        long[][] rows = readAllRows(result.newFilePath, schema, false);
        assertEquals(1, rows.length);
        assertEquals(99L, rows[0][0]);
        int[] fwd = result.forwardRgMappings.get(fileId).get(0);
        assertEquals(1, fwd.length);
        assertEquals(0, fwd[0]);
    }

    /**
     * Bitmap word-boundary correctness: 128 rows, delete rows at positions
     * 0, 63, 64, 127 (crossing the 64-bit word boundary).  Survivors are all
     * other 124 rows.  Verifies data and forward mapping at boundary positions.
     */
    @Test
    public void testBitmapWordBoundaryFiltering() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
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
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
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
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
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
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
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
     * Bitmap with exactly 64 rows (one bitmap word): delete the first row (bit 0)
     * and the last row (bit 63, the highest bit in the word).
     * Verifies correct handling when the bitmap is exactly one {@code long} word.
     */
    @Test
    public void testBitmapExactOneWord_64Rows() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        int totalRows = 64;
        long[] ids = new long[totalRows];
        for (int i = 0; i < totalRows; i++)
        {
            ids[i] = i;
        }
        long fileId = 34L;
        String srcPath = writeTestFile("src_64rows.pxl", schema, ids, false, null);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmapForRows(totalRows, 0, 63));

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(makeGroup(fileId, srcPath, schema), 100L, bitmaps);

        long[][] rows = readAllRows(result.newFilePath, schema, false);
        assertEquals("62 rows should survive (64 - 2 deleted)", 62, rows.length);

        int[] fwd = result.forwardRgMappings.get(fileId).get(0);
        assertEquals("row 0 deleted → -1", -1, fwd[0]);
        assertEquals("row 63 deleted → -1", -1, fwd[63]);
        assertEquals("row 1 survives → 0", 0, fwd[1]);
        assertEquals("row 62 survives → 61", 61, fwd[62]);

        assertEquals("first survivor should be id=1", 1L, rows[0][0]);
        assertEquals("last survivor should be id=62", 62L, rows[rows.length - 1][0]);

        assertRewriteResultConsistency(result, 62);
    }

    // =======================================================================
    // Section 6: dual-write functional tests
    // =======================================================================

    /**
     * Forward dual-write: a delete on the old file propagates to the new file.
     * Setup: 6-row file, delete rows 0,2,4 via rewrite (3 survivors at new offsets 0,1,2).
     * After registerDualWrite, deleteRecord(oldFile, rg0, row1, ts) should also mark
     * new file's corresponding position as deleted.
     */
    @Test
    public void testDualWrite_forwardPropagation() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long[] ids = {0L, 1L, 2L, 3L, 4L, 5L};
        long fileId = 30L;
        String srcPath = writeTestFile("dw_fwd_src.pxl", schema, ids, false, null);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmapForRows(6, 0, 2, 4));

        // Also register old file's Visibility so deleteRecord can operate on it
        retinaManager.addVisibility(fileId, 0, 6, 0L, null, false);

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(makeGroup(fileId, srcPath, schema), 100L, bitmaps);
        long newFileId = result.newFileId;
        assertTrue("new file should be registered", newFileId > 0);

        gc.registerDualWrite(result);

        // Survivor row 1 in old file → new global offset 0 → new rg0, offset 0
        long deleteTs = 200L;
        retinaManager.deleteRecord(fileId, 0, 1, deleteTs);

        // Verify: old file should show row 1 deleted at ts=200
        long[] oldBitmap = retinaManager.queryVisibility(fileId, 0, deleteTs, 0L);
        assertTrue("old file row 1 should be deleted",
                (oldBitmap[1 / 64] & (1L << (1 % 64))) != 0);

        // Verify: new file rg0 offset 0 should also be deleted at ts=200
        long[] newBitmap = retinaManager.queryVisibility(newFileId, 0, deleteTs, 0L);
        assertTrue("new file row 0 should be deleted via forward dual-write",
                (newBitmap[0 / 64] & (1L << (0 % 64))) != 0);

        gc.unregisterDualWrite(result);
    }

    /**
     * Backward dual-write: a delete on the new file propagates back to the old file.
     * Same setup as forward test but delete is issued against the new file.
     */
    @Test
    public void testDualWrite_backwardPropagation() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long[] ids = {0L, 1L, 2L, 3L, 4L, 5L};
        long fileId = 31L;
        String srcPath = writeTestFile("dw_bwd_src.pxl", schema, ids, false, null);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmapForRows(6, 0, 2, 4));

        retinaManager.addVisibility(fileId, 0, 6, 0L, null, false);

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(makeGroup(fileId, srcPath, schema), 100L, bitmaps);
        long newFileId = result.newFileId;

        gc.registerDualWrite(result);

        // New file row 2 corresponds to old file row 5 (the third survivor).
        // Backward mapping: newRg0 offset 2 → old global offset 5 → old rg0 offset 5.
        long deleteTs = 200L;
        retinaManager.deleteRecord(newFileId, 0, 2, deleteTs);

        // Verify: new file row 2 deleted
        long[] newBitmap = retinaManager.queryVisibility(newFileId, 0, deleteTs, 0L);
        assertTrue("new file row 2 should be deleted",
                (newBitmap[2 / 64] & (1L << (2 % 64))) != 0);

        // Verify: old file row 5 should also be deleted via backward dual-write
        long[] oldBitmap = retinaManager.queryVisibility(fileId, 0, deleteTs, 0L);
        assertTrue("old file row 5 should be deleted via backward dual-write",
                (oldBitmap[5 / 64] & (1L << (5 % 64))) != 0);

        gc.unregisterDualWrite(result);
    }

    /**
     * After unregisterDualWrite, deletes no longer propagate.
     */
    @Test
    public void testDualWrite_unregisterStops() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
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

    /**
     * When isDualWriteActive is false, deleteRecord has no extra overhead
     * beyond a single volatile read.  Verified by issuing deletes without
     * any registration and confirming they succeed with no side effects.
     */
    @Test
    public void testDualWrite_volatileFastPath() throws Exception
    {
        long fileId = 33L;
        retinaManager.addVisibility(fileId, 0, 4, 0L, null, false);

        // No dual-write registered — isDualWriteActive is false.
        // deleteRecord should work without errors and without trying to forward.
        long deleteTs = 200L;
        retinaManager.deleteRecord(fileId, 0, 1, deleteTs);

        long[] bitmap = retinaManager.queryVisibility(fileId, 0, deleteTs, 0L);
        assertTrue("direct delete should work on fast path",
                (bitmap[1 / 64] & (1L << (1 % 64))) != 0);
        assertFalse("row 0 should not be affected",
                (bitmap[0 / 64] & (1L << (0 % 64))) != 0);
    }

    // =======================================================================
    // Section 7: visibility synchronization tests
    // =======================================================================

    /**
     * exportChainItemsAfter returns only items with ts > safeGcTs.
     * Setup: 6-row file, delete rows at ts=50 (before) and ts=200,300 (after safeGcTs=100).
     * After rewrite, export from old file should only return the ts>100 items.
     */
    @Test
    public void testVisibilitySync_exportChainItemsAfterSafeGcTs() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long[] ids = {0L, 1L, 2L, 3L, 4L, 5L};
        long fileId = 40L;
        writeTestFile("vis_export_src.pxl", schema, ids, false, null);

        retinaManager.addVisibility(fileId, 0, 6, 0L, null, false);
        retinaManager.deleteRecord(fileId, 0, 0, 50L);
        retinaManager.deleteRecord(fileId, 0, 1, 200L);
        retinaManager.deleteRecord(fileId, 0, 2, 300L);

        long[] exported = retinaManager.exportChainItemsAfter(fileId, 0, 100L);

        assertNotNull("exported items should not be null", exported);
        assertTrue("should have items (interleaved pairs)", exported.length >= 4);

        Set<Long> exportedTs = new HashSet<>();
        for (int i = 0; i < exported.length; i += 2)
        {
            exportedTs.add(exported[i + 1]);
        }
        assertTrue("should contain ts=200", exportedTs.contains(200L));
        assertTrue("should contain ts=300", exportedTs.contains(300L));
        assertFalse("should NOT contain ts=50", exportedTs.contains(50L));
    }

    /**
     * importDeletionChain correctly marks rows in new file bitmap.
     * Rewrite a file, then import known items into the new file's RGVisibility
     * and verify via queryVisibility.
     */
    @Test
    public void testVisibilitySync_importDeletionItemsCorrect() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long[] ids = {0L, 1L, 2L, 3L, 4L, 5L};
        long fileId = 41L;
        String srcPath = writeTestFile("vis_import_src.pxl", schema, ids, false, null);

        retinaManager.addVisibility(fileId, 0, 6, 0L, null, false);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmapForRows(6, 0, 5));

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(makeGroup(fileId, srcPath, schema), 100L, bitmaps);
        long newFileId = result.newFileId;
        assertTrue("new file should be registered", newFileId > 0);

        long[] items = {0, 200, 1, 300};
        retinaManager.importDeletionChain(newFileId, 0, items);

        long[] bitmap = retinaManager.queryVisibility(newFileId, 0, 350L, 0L);
        assertTrue("new file row 0 should be deleted at ts=350",
                (bitmap[0 / 64] & (1L << (0 % 64))) != 0);
        assertTrue("new file row 1 should be deleted at ts=350",
                (bitmap[1 / 64] & (1L << (1 % 64))) != 0);
        assertFalse("new file row 2 should NOT be deleted",
                (bitmap[2 / 64] & (1L << (2 % 64))) != 0);
    }

    /**
     * Overlapping dual-write and import items are correctly deduplicated.
     * After registerDualWrite, a delete goes to both old and new files.
     * syncVisibility exports from old (capturing the overlap) and imports
     * to new. The truncation-dedup ensures no corruption.
     */
    @Test
    public void testVisibilitySync_truncationDedup() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long[] ids = {0L, 1L, 2L, 3L, 4L, 5L};
        long fileId = 42L;
        String srcPath = writeTestFile("vis_dedup_src.pxl", schema, ids, false, null);

        retinaManager.addVisibility(fileId, 0, 6, 0L, null, false);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmapForRows(6, 0, 5));

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(makeGroup(fileId, srcPath, schema), 100L, bitmaps);
        long newFileId = result.newFileId;

        gc.registerDualWrite(result);

        retinaManager.deleteRecord(fileId, 0, 1, 200L);

        gc.syncVisibility(result, 100L);

        long[] newBitmap = retinaManager.queryVisibility(newFileId, 0, 250L, 0L);
        int[] fwd = result.forwardRgMappings.get(fileId).get(0);
        int newRow1 = fwd[1];
        assertTrue("row mapped from old row 1 should be deleted in new file",
                newRow1 >= 0 && (newBitmap[newRow1 / 64] & (1L << (newRow1 % 64))) != 0);

        gc.unregisterDualWrite(result);
    }

    /**
     * Full realistic end-to-end: S2 rewrite → S3 dual-write → deletes at various phases →
     * S4 syncVisibility → multi-snap_ts consistency verification between old and new files.
     *
     * Timeline:
     *   Phase 1 (ts<=100): rows 0,9 deleted → physically removed by rewrite
     *   Phase 2 (100<ts<dual-write): rows 1,2 deleted → only in old chain, need export
     *   S3: registerDualWrite
     *   Phase 3 (dual-write window): rows 3,4 deleted → in both chains (overlap)
     *   S4: syncVisibility → export + coord transform + truncation dedup + import
     *   Phase 4 (post-sync): row 5 deleted → dual-write keeps both in sync
     *   Verify: for every snap_ts, old and new visibility match through forward mapping.
     */
    @Test
    public void testVisibilitySync_exportImportEndToEnd() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long[] ids = {0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L};
        long fileId = 43L;
        String srcPath = writeTestFile("vis_e2e_src.pxl", schema, ids, false, null);

        retinaManager.addVisibility(fileId, 0, 10, 0L, null, false);

        // Phase 1: deletes at ts <= safeGcTs (will be physically removed by rewrite)
        retinaManager.deleteRecord(fileId, 0, 0, 50L);
        retinaManager.deleteRecord(fileId, 0, 9, 80L);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", makeBitmapForRows(10, 0, 9));

        // S2: rewrite — rows 0,9 excluded, rows 1-8 survive as new rows 0-7
        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(makeGroup(fileId, srcPath, schema), 100L, bitmaps);
        long newFileId = result.newFileId;
        assertTrue("new file should be registered", newFileId > 0);
        assertRewriteResultConsistency(result, 8);

        int[] fwd = result.forwardRgMappings.get(fileId).get(0);
        assertEquals("old row 0 should be deleted (-1)", -1, fwd[0]);
        assertEquals("old row 9 should be deleted (-1)", -1, fwd[9]);

        // Phase 2: deletes after safeGcTs but BEFORE dual-write registration
        // These only go to the old file's chain — must be exported
        retinaManager.deleteRecord(fileId, 0, 1, 150L);
        retinaManager.deleteRecord(fileId, 0, 2, 180L);

        // S3: register dual-write
        gc.registerDualWrite(result);

        // Phase 3: deletes during dual-write window — written to BOTH old and new
        retinaManager.deleteRecord(fileId, 0, 3, 200L);
        retinaManager.deleteRecord(fileId, 0, 4, 250L);

        // S4: syncVisibility — export old chain → coord transform → truncation dedup → import
        gc.syncVisibility(result, 100L);

        // Phase 4: post-sync delete — dual-write keeps both files in sync
        retinaManager.deleteRecord(fileId, 0, 5, 300L);

        // ---- Verification: multi-snap_ts consistency ----
        // For every surviving old row (1-8), check that old and new visibility agree
        for (long snapTs : new long[]{100L, 150L, 180L, 200L, 250L, 300L, 500L})
        {
            long[] oldBitmap = retinaManager.queryVisibility(fileId, 0, snapTs, 0L);
            long[] newBitmap = retinaManager.queryVisibility(newFileId, 0, snapTs, 0L);

            for (int oldRow = 1; oldRow <= 8; oldRow++)
            {
                int newRow = fwd[oldRow];
                assertTrue("old row " + oldRow + " should map to valid new row", newRow >= 0);

                boolean oldDeleted = (oldBitmap[oldRow / 64] & (1L << (oldRow % 64))) != 0;
                boolean newDeleted = (newBitmap[newRow / 64] & (1L << (newRow % 64))) != 0;
                assertEquals("snap_ts=" + snapTs + " oldRow=" + oldRow + " newRow=" + newRow
                                + ": visibility mismatch",
                        oldDeleted, newDeleted);
            }
        }

        gc.unregisterDualWrite(result);
    }

    // =======================================================================
    // Section 8: index update + atomic switch tests (placeholder)
    // =======================================================================

    /** MainIndex entries (newRowId → newRowLocation) are correct after syncIndex. */
    @Ignore("index update and atomic switch not yet implemented")
    @Test
    public void testIndexSync_mainIndexEntries() throws Exception
    {
    }

    /** SinglePointIndex (IndexKey → newRowId) is correct after syncIndex. */
    @Ignore("index update and atomic switch not yet implemented")
    @Test
    public void testIndexSync_singlePointIndexUpdate() throws Exception
    {
    }

    /** atomicSwapFiles turns TEMPORARY → REGULAR and removes old files from catalog. */
    @Ignore("index update and atomic switch not yet implemented")
    @Test
    public void testAtomicSwap_swapFiles() throws Exception
    {
    }

    /** Rollback cleans up TEMPORARY, Visibility, dual-write, and index entries. */
    @Ignore("index update and atomic switch not yet implemented")
    @Test
    public void testAtomicSwap_rollbackCleansUp() throws Exception
    {
    }

    /** Delayed cleanup removes old files after lwm passes retireTs. */
    @Ignore("index update and atomic switch not yet implemented")
    @Test
    public void testAtomicSwap_delayedCleanup() throws Exception
    {
    }

    // =======================================================================
    // Section 9: end-to-end integration tests (placeholder)
    // =======================================================================

    /** Insert → delete → runGC → catalog swap → new file correct → query correct. */
    @Ignore("end-to-end not yet implemented")
    @Test
    public void testEndToEnd_fullGcCycle() throws Exception
    {
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

    /** Concurrent INSERT/DELETE/UPDATE + GC → all operations correct. */
    @Ignore("concurrency test not yet implemented")
    @Test
    public void testEndToEnd_concurrentCdcAndGc() throws Exception
    {
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
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to reset RetinaResourceManager state", e);
        }
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
                        f, filePath, fileId, rgCount, 1L, 0, 10L, 0.70, 0L);

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
                    f, path, fid, rgCount, 1L, 0, 5L, 0.70, 0L));
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
                    1048576, EncodingLevel.EL2);
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
                        stats[0], ratio, 0L));
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
                    1048576, EncodingLevel.EL2);
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
                    0, new int[0], new int[]{0}, new HashMap<>(), Collections.emptyList());
        }
    }
}
