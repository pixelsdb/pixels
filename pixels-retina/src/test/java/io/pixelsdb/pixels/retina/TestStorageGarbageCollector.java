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
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.DecimalColumnVector;
import io.pixelsdb.pixels.core.vector.DoubleColumnVector;
import io.pixelsdb.pixels.core.vector.FloatColumnVector;
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
import java.nio.ByteBuffer;
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
                1048576, EncodingLevel.EL2, 86_400_000L);
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
                1048576, EncodingLevel.EL2, 86_400_000L);

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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, 0L, Integer.MAX_VALUE, 10,
                1048576, EncodingLevel.EL2, 86_400_000L);

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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, 0L, Integer.MAX_VALUE, max,
                1048576, EncodingLevel.EL2, 86_400_000L);

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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, 0L, Integer.MAX_VALUE, 10,
                1048576, EncodingLevel.EL2, 86_400_000L);
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
                1048576, EncodingLevel.EL2, 86_400_000L);

        // Each file is 100 MB on disk with 40% deleted → 60 MB effective
        List<StorageGarbageCollector.FileCandidate> candidates = Arrays.asList(
                new StorageGarbageCollector.FileCandidate(
                        makeFile(1, 1), "f1", 1, 1, 1L, 0, 0.40, 100 * 1024 * 1024L),
                new StorageGarbageCollector.FileCandidate(
                        makeFile(2, 1), "f2", 2, 1, 1L, 0, 0.40, 100 * 1024 * 1024L),
                new StorageGarbageCollector.FileCandidate(
                        makeFile(3, 1), "f3", 3, 1, 1L, 0, 0.40, 100 * 1024 * 1024L)
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
                1048576, EncodingLevel.EL2, 86_400_000L);

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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, target, Integer.MAX_VALUE, 10,
                1048576, EncodingLevel.EL2, 86_400_000L);

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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, target, Integer.MAX_VALUE, 10,
                1048576, EncodingLevel.EL2, 86_400_000L);

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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, 0L, 2, 100,
                1048576, EncodingLevel.EL2, 86_400_000L);

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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, hugeTarget, 3, 100,
                1048576, EncodingLevel.EL2, 86_400_000L);

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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, target, Integer.MAX_VALUE, 100,
                1048576, EncodingLevel.EL2, 86_400_000L);

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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, 0L, Integer.MAX_VALUE, 10,
                1048576, EncodingLevel.EL2, 86_400_000L);

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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, target, Integer.MAX_VALUE, 10,
                1048576, EncodingLevel.EL2, 86_400_000L);

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
     * With {@code maxFilesPerGroup=1}, every file must form its own group
     * regardless of size.
     */
    @Test
    public void testGroupAndMerge_maxFilesPerGroupOne()
    {
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, 0L, 1, 100,
                1048576, EncodingLevel.EL2, 86_400_000L);

        List<StorageGarbageCollector.FileCandidate> candidates = Arrays.asList(
                new StorageGarbageCollector.FileCandidate(
                        makeFile(1, 1), "f1", 1, 1, 1L, 0, 0.90, 0L),
                new StorageGarbageCollector.FileCandidate(
                        makeFile(2, 1), "f2", 2, 1, 1L, 0, 0.80, 0L),
                new StorageGarbageCollector.FileCandidate(
                        makeFile(3, 1), "f3", 3, 1, 1L, 0, 0.70, 0L)
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
                1048576, EncodingLevel.EL2, 86_400_000L);

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
        StorageGarbageCollector gc = new StorageGarbageCollector(
                null, null, 0.5, 0L, 0, 10,
                1048576, EncodingLevel.EL2, 86_400_000L);

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
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
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
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
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
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
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
     * Full realistic end-to-end: rewrite → dual-write → deletes at various phases →
     * syncVisibility → multi-snap_ts consistency verification between old and new files.
     *
     * Timeline:
     *   Phase 1 (ts<=100): rows 0,9 deleted → physically removed by rewrite
     *   Phase 2 (100<ts<dual-write): rows 1,2 deleted → only in old chain, need export
     *   Phase 3 (dual-write registered): rows 3,4 deleted → in both chains (overlap)
     *   Phase 4 (syncVisibility): export + coord transform + truncation dedup + import
     *   Phase 5 (post-sync): row 5 deleted → dual-write keeps both in sync
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

        // Rewrite — rows 0,9 excluded, rows 1-8 survive as new rows 0-7
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

        // Register dual-write
        gc.registerDualWrite(result);

        // Phase 3: deletes during dual-write window — written to BOTH old and new
        retinaManager.deleteRecord(fileId, 0, 3, 200L);
        retinaManager.deleteRecord(fileId, 0, 4, 250L);

        // syncVisibility — export old chain → coord transform → truncation dedup → import
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
    // Section 7b: rgIdForGlobalRowOffset boundary tests
    // =======================================================================

    /**
     * Single RG: any offset within [0, totalRows) must return rgId=0.
     */
    @Test
    public void testRgIdForGlobalRowOffset_singleRg()
    {
        int[] rgRowStart = {0, 256};
        assertEquals(0, RetinaResourceManager.rgIdForGlobalRowOffset(0, rgRowStart));
        assertEquals(0, RetinaResourceManager.rgIdForGlobalRowOffset(128, rgRowStart));
        assertEquals(0, RetinaResourceManager.rgIdForGlobalRowOffset(255, rgRowStart));
    }

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
     * Last valid offset (sentinel - 1) returns the last RG id.
     */
    @Test
    public void testRgIdForGlobalRowOffset_lastOffset()
    {
        int[] rgRowStart = {0, 50, 100, 150};
        assertEquals(2, RetinaResourceManager.rgIdForGlobalRowOffset(149, rgRowStart));
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
     * dual-write active.  Verifies that all deletes are correctly propagated
     * in both forward and backward directions under contention.
     */
    @Test
    public void testDualWrite_concurrentPressure() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        int numRows = 64;
        long[] ids = new long[numRows];
        for (int i = 0; i < numRows; i++)
        {
            ids[i] = i;
        }
        long fileId = 50L;
        String srcPath = writeTestFile("dw_conc_src.pxl", schema, ids, false, null);

        long[] gcBitmap = makeBitmapForRows(numRows);
        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(fileId + "_0", gcBitmap);

        retinaManager.addVisibility(fileId, 0, numRows, 0L, null, false);

        StorageGarbageCollector.RewriteResult result =
                gc.rewriteFileGroup(makeGroup(fileId, srcPath, schema), 100L, bitmaps);
        long newFileId = result.newFileId;
        assertTrue("new file should be registered", newFileId > 0);

        gc.registerDualWrite(result);

        int numThreads = 8;
        int deletesPerThread = numRows / numThreads;
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        AtomicInteger errors = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < numThreads; t++)
        {
            int threadId = t;
            futures.add(executor.submit(() ->
            {
                try
                {
                    barrier.await();
                    for (int d = 0; d < deletesPerThread; d++)
                    {
                        int rowOffset = threadId * deletesPerThread + d;
                        long deleteTs = 200L + rowOffset;
                        if (rowOffset % 2 == 0)
                        {
                            retinaManager.deleteRecord(fileId, 0, rowOffset, deleteTs);
                        }
                        else
                        {
                            int newOff = result.forwardRgMappings.get(fileId).get(0)[rowOffset];
                            if (newOff >= 0)
                            {
                                int newRgId = RetinaResourceManager.rgIdForGlobalRowOffset(
                                        newOff, result.newFileRgRowStart);
                                int newRgOff = newOff - result.newFileRgRowStart[newRgId];
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

        long queryTs = 200L + numRows;

        // Verify all rows are deleted in old file
        long[] oldBitmap = retinaManager.queryVisibility(fileId, 0, queryTs, 0L);
        for (int r = 0; r < numRows; r++)
        {
            assertTrue("old file row " + r + " should be deleted",
                    (oldBitmap[r / 64] & (1L << (r % 64))) != 0);
        }

        // Verify all corresponding rows are deleted in new file
        for (int r = 0; r < numRows; r++)
        {
            int newGlobal = result.forwardRgMappings.get(fileId).get(0)[r];
            if (newGlobal >= 0)
            {
                int newRgId = RetinaResourceManager.rgIdForGlobalRowOffset(
                        newGlobal, result.newFileRgRowStart);
                int newRgOff = newGlobal - result.newFileRgRowStart[newRgId];
                long[] newBitmap = retinaManager.queryVisibility(newFileId, newRgId, queryTs, 0L);
                assertTrue("new file rgId=" + newRgId + " row " + newRgOff + " (from old row " + r + ") should be deleted",
                        (newBitmap[newRgOff / 64] & (1L << (newRgOff % 64))) != 0);
            }
        }

        gc.unregisterDualWrite(result);
    }

    // =======================================================================
    // Section 8: index update + atomic switch + rollback + delayed cleanup
    // =======================================================================

    /** atomicSwapFiles turns TEMPORARY → REGULAR and removes old files from catalog. */
    @Test
    public void testAtomicSwap_swapFiles() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");

        long[] ids = new long[10];
        long[] ts = new long[10];
        for (int i = 0; i < 10; i++)
        {
            ids[i] = i;
            ts[i] = 100;
        }
        String filePath = writeTestFile("swap_old.pxl", schema, ids, true, ts);

        File oldFile = new File();
        oldFile.setName("swap_old.pxl");
        oldFile.setType(File.Type.REGULAR);
        oldFile.setNumRowGroup(1);
        oldFile.setMinRowId(0);
        oldFile.setMaxRowId(9);
        oldFile.setPathId(testPathId);
        metadataService.addFiles(Collections.singletonList(oldFile));
        long oldFileId = metadataService.getFileId(filePath);
        assertTrue("Old file should have a valid id", oldFileId > 0);

        File newFile = new File();
        newFile.setName("swap_new.pxl");
        newFile.setType(File.Type.TEMPORARY);
        newFile.setNumRowGroup(1);
        newFile.setMinRowId(0);
        newFile.setMaxRowId(9);
        newFile.setPathId(testPathId);
        metadataService.addFiles(Collections.singletonList(newFile));
        String newFilePath = testOrderedPathUri + "/swap_new.pxl";
        long newFileId = metadataService.getFileId(newFilePath);
        assertTrue("New file should have a valid id", newFileId > 0);

        metadataService.atomicSwapFiles(newFileId, Collections.singletonList(oldFileId));

        File swapped = metadataService.getFileById(newFileId);
        assertNotNull("New file should still exist after swap", swapped);
        assertEquals("New file should be REGULAR after swap",
                File.Type.REGULAR, swapped.getType());

        File gone = metadataService.getFileById(oldFileId);
        assertTrue("Old file should be gone from catalog after swap",
                gone == null || gone.getId() == 0);
    }

    /**
     * Atomicity with multiple old files: one TEMPORARY new file and three REGULAR
     * old files are swapped in a single call.  Verifies that after the call the new
     * file is promoted to REGULAR and <b>all</b> old files are removed from the
     * catalog—i.e., the UPDATE and DELETE execute as one indivisible transaction.
     */
    @Test
    public void testAtomicSwap_multipleOldFilesAtomicity() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long[] ids = {0, 1};
        long[] ts = {100, 100};

        String pathOld1 = writeTestFile("atom_old1.pxl", schema, ids, true, ts);
        String pathOld2 = writeTestFile("atom_old2.pxl", schema, ids, true, ts);
        String pathOld3 = writeTestFile("atom_old3.pxl", schema, ids, true, ts);

        File old1 = new File();
        old1.setName("atom_old1.pxl");
        old1.setType(File.Type.REGULAR);
        old1.setNumRowGroup(1);
        old1.setMinRowId(0);
        old1.setMaxRowId(1);
        old1.setPathId(testPathId);

        File old2 = new File();
        old2.setName("atom_old2.pxl");
        old2.setType(File.Type.REGULAR);
        old2.setNumRowGroup(1);
        old2.setMinRowId(0);
        old2.setMaxRowId(1);
        old2.setPathId(testPathId);

        File old3 = new File();
        old3.setName("atom_old3.pxl");
        old3.setType(File.Type.REGULAR);
        old3.setNumRowGroup(1);
        old3.setMinRowId(0);
        old3.setMaxRowId(1);
        old3.setPathId(testPathId);

        metadataService.addFiles(Arrays.asList(old1, old2, old3));
        long oldId1 = metadataService.getFileId(pathOld1);
        long oldId2 = metadataService.getFileId(pathOld2);
        long oldId3 = metadataService.getFileId(pathOld3);
        assertTrue("Old file 1 should have a valid id", oldId1 > 0);
        assertTrue("Old file 2 should have a valid id", oldId2 > 0);
        assertTrue("Old file 3 should have a valid id", oldId3 > 0);

        File newFile = new File();
        newFile.setName("atom_new.pxl");
        newFile.setType(File.Type.TEMPORARY);
        newFile.setNumRowGroup(1);
        newFile.setMinRowId(0);
        newFile.setMaxRowId(1);
        newFile.setPathId(testPathId);
        metadataService.addFiles(Collections.singletonList(newFile));
        long newFileId = metadataService.getFileId(testOrderedPathUri + "/atom_new.pxl");
        assertTrue("New TEMPORARY file should have a valid id", newFileId > 0);

        File preSwapNew = metadataService.getFileById(newFileId);
        assertNotNull("New file must exist before swap", preSwapNew);
        assertEquals("New file should be TEMPORARY before swap",
                File.Type.TEMPORARY, preSwapNew.getType());

        metadataService.atomicSwapFiles(newFileId, Arrays.asList(oldId1, oldId2, oldId3));

        File swapped = metadataService.getFileById(newFileId);
        assertNotNull("New file must still exist after swap", swapped);
        assertEquals("New file should be REGULAR after swap",
                File.Type.REGULAR, swapped.getType());

        for (long oldId : new long[]{oldId1, oldId2, oldId3})
        {
            File g = metadataService.getFileById(oldId);
            assertTrue("Old file " + oldId + " should be gone after swap",
                    g == null || g.getId() == 0);
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
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long[] ids = {0, 1, 2};
        long[] ts = {100, 100, 100};

        String pathOld = writeTestFile("idem_old.pxl", schema, ids, true, ts);

        File oldFile = new File();
        oldFile.setName("idem_old.pxl");
        oldFile.setType(File.Type.REGULAR);
        oldFile.setNumRowGroup(1);
        oldFile.setMinRowId(0);
        oldFile.setMaxRowId(2);
        oldFile.setPathId(testPathId);
        metadataService.addFiles(Collections.singletonList(oldFile));
        long oldFileId = metadataService.getFileId(pathOld);

        File newFile = new File();
        newFile.setName("idem_new.pxl");
        newFile.setType(File.Type.TEMPORARY);
        newFile.setNumRowGroup(1);
        newFile.setMinRowId(0);
        newFile.setMaxRowId(2);
        newFile.setPathId(testPathId);
        metadataService.addFiles(Collections.singletonList(newFile));
        long newFileId = metadataService.getFileId(testOrderedPathUri + "/idem_new.pxl");

        metadataService.atomicSwapFiles(newFileId, Collections.singletonList(oldFileId));

        File afterFirst = metadataService.getFileById(newFileId);
        assertNotNull(afterFirst);
        assertEquals(File.Type.REGULAR, afterFirst.getType());

        metadataService.atomicSwapFiles(newFileId, Collections.singletonList(oldFileId));

        File afterSecond = metadataService.getFileById(newFileId);
        assertNotNull("File must still exist after idempotent retry", afterSecond);
        assertEquals("File should remain REGULAR after idempotent retry",
                File.Type.REGULAR, afterSecond.getType());
        File stillGone = metadataService.getFileById(oldFileId);
        assertTrue("Old file should remain absent after idempotent retry",
                stillGone == null || stillGone.getId() == 0);
    }

    /**
     * TEMPORARY visibility semantics: before the swap, {@code getFiles(pathId)} must
     * <b>not</b> return the TEMPORARY new file (the DAO filters {@code FILE_TYPE <> 0}).
     * After the swap the promoted file is visible and the old file disappears.
     */
    @Test
    public void testAtomicSwap_temporaryInvisibleViaGetFiles() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long[] ids = {0, 1};
        long[] ts = {100, 100};
        writeTestFile("vis_old.pxl", schema, ids, true, ts);

        File oldFile = new File();
        oldFile.setName("vis_old.pxl");
        oldFile.setType(File.Type.REGULAR);
        oldFile.setNumRowGroup(1);
        oldFile.setMinRowId(0);
        oldFile.setMaxRowId(1);
        oldFile.setPathId(testPathId);

        File tempFile = new File();
        tempFile.setName("vis_new_temp.pxl");
        tempFile.setType(File.Type.TEMPORARY);
        tempFile.setNumRowGroup(1);
        tempFile.setMinRowId(0);
        tempFile.setMaxRowId(1);
        tempFile.setPathId(testPathId);

        metadataService.addFiles(Arrays.asList(oldFile, tempFile));
        long oldFileId = metadataService.getFileId(testOrderedPathUri + "/vis_old.pxl");
        long tempFileId = metadataService.getFileId(testOrderedPathUri + "/vis_new_temp.pxl");
        assertTrue(oldFileId > 0);
        assertTrue(tempFileId > 0);

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

    /**
     * Concurrent atomicity: multiple threads each perform an independent
     * {@code atomicSwapFiles} on their own (newFile, oldFile) pair.  All swaps
     * must succeed without interference, and every new file ends up REGULAR
     * while every old file is removed.
     */
    @Test
    public void testAtomicSwap_concurrentSwapsDifferentGroups() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long[] ids = {0};
        long[] ts = {100};
        int nThreads = 8;

        long[] newFileIds = new long[nThreads];
        long[] oldFileIds = new long[nThreads];

        for (int i = 0; i < nThreads; i++)
        {
            String oldName = "conc_old_" + i + ".pxl";
            String newName = "conc_new_" + i + ".pxl";
            writeTestFile(oldName, schema, ids, true, ts);

            File oldFile = new File();
            oldFile.setName(oldName);
            oldFile.setType(File.Type.REGULAR);
            oldFile.setNumRowGroup(1);
            oldFile.setMinRowId(0);
            oldFile.setMaxRowId(0);
            oldFile.setPathId(testPathId);

            File newFile = new File();
            newFile.setName(newName);
            newFile.setType(File.Type.TEMPORARY);
            newFile.setNumRowGroup(1);
            newFile.setMinRowId(0);
            newFile.setMaxRowId(0);
            newFile.setPathId(testPathId);

            metadataService.addFiles(Arrays.asList(oldFile, newFile));
            oldFileIds[i] = metadataService.getFileId(testOrderedPathUri + "/" + oldName);
            newFileIds[i] = metadataService.getFileId(testOrderedPathUri + "/" + newName);
            assertTrue("Old file " + i + " id must be valid", oldFileIds[i] > 0);
            assertTrue("New file " + i + " id must be valid", newFileIds[i] > 0);
        }

        CyclicBarrier barrier = new CyclicBarrier(nThreads);
        ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        AtomicInteger failures = new AtomicInteger(0);

        for (int i = 0; i < nThreads; i++)
        {
            final int idx = i;
            pool.submit(() ->
            {
                try
                {
                    barrier.await();
                    metadataService.atomicSwapFiles(newFileIds[idx],
                            Collections.singletonList(oldFileIds[idx]));
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                    failures.incrementAndGet();
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS);

        assertEquals("All concurrent swaps should succeed", 0, failures.get());

        for (int i = 0; i < nThreads; i++)
        {
            File promoted = metadataService.getFileById(newFileIds[i]);
            assertNotNull("Promoted file " + i + " must exist", promoted);
            assertEquals("Promoted file " + i + " must be REGULAR",
                    File.Type.REGULAR, promoted.getType());

            File removed = metadataService.getFileById(oldFileIds[i]);
            assertTrue("Old file " + i + " should be gone",
                    removed == null || removed.getId() == 0);
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
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long[] ids = {0, 1};
        long[] ts = {100, 100};

        writeTestFile("partial_old1.pxl", schema, ids, true, ts);
        writeTestFile("partial_old2.pxl", schema, ids, true, ts);

        File old1 = new File();
        old1.setName("partial_old1.pxl");
        old1.setType(File.Type.REGULAR);
        old1.setNumRowGroup(1);
        old1.setMinRowId(0);
        old1.setMaxRowId(1);
        old1.setPathId(testPathId);

        File old2 = new File();
        old2.setName("partial_old2.pxl");
        old2.setType(File.Type.REGULAR);
        old2.setNumRowGroup(1);
        old2.setMinRowId(0);
        old2.setMaxRowId(1);
        old2.setPathId(testPathId);

        metadataService.addFiles(Arrays.asList(old1, old2));
        long oldId1 = metadataService.getFileId(testOrderedPathUri + "/partial_old1.pxl");
        long oldId2 = metadataService.getFileId(testOrderedPathUri + "/partial_old2.pxl");

        metadataService.deleteFiles(Collections.singletonList(oldId1));
        File preCheck = metadataService.getFileById(oldId1);
        assertTrue("old1 should be gone before swap", preCheck == null || preCheck.getId() == 0);

        File newFile = new File();
        newFile.setName("partial_new.pxl");
        newFile.setType(File.Type.TEMPORARY);
        newFile.setNumRowGroup(1);
        newFile.setMinRowId(0);
        newFile.setMaxRowId(1);
        newFile.setPathId(testPathId);
        metadataService.addFiles(Collections.singletonList(newFile));
        long newFileId = metadataService.getFileId(testOrderedPathUri + "/partial_new.pxl");

        metadataService.atomicSwapFiles(newFileId, Arrays.asList(oldId1, oldId2));

        File swapped = metadataService.getFileById(newFileId);
        assertNotNull("New file must exist after swap", swapped);
        assertEquals("New file must be REGULAR", File.Type.REGULAR, swapped.getType());

        File g2 = metadataService.getFileById(oldId2);
        assertTrue("Remaining old file should be gone",
                g2 == null || g2.getId() == 0);
    }

    /**
     * Rollback after rewrite + dual-write: verifies that Visibility entries for the new
     * file are removed, dual-write is unregistered, the TEMPORARY catalog entry is deleted,
     * and the physical file is cleaned up.
     */
    @Test
    public void testAtomicSwap_rollbackCleansUp() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long[] ids = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        long[] ts = new long[10];
        Arrays.fill(ts, 100);
        String filePath = writeTestFile("rollback_src.pxl", schema, ids, true, ts);

        File srcFile = new File();
        srcFile.setName("rollback_src.pxl");
        srcFile.setType(File.Type.REGULAR);
        srcFile.setNumRowGroup(1);
        srcFile.setMinRowId(0);
        srcFile.setMaxRowId(9);
        srcFile.setPathId(testPathId);
        metadataService.addFiles(Collections.singletonList(srcFile));
        long srcFileId = metadataService.getFileId(filePath);

        StorageGarbageCollector.FileGroup group = makeGroup(srcFileId, filePath, schema);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(RetinaUtils.buildRgKey(srcFileId, 0), makeBitmap(10, 6));

        retinaManager.addVisibility(srcFileId, 0, 10, 50, null, true);

        StorageGarbageCollector.RewriteResult result = gc.rewriteFileGroup(group, 100, bitmaps);
        assertTrue("New file should be created", result.newFileId > 0);

        gc.registerDualWrite(result);

        gc.rollback(result);

        assertFalse("New file should be deleted after rollback",
                fileStorage.exists(result.newFilePath));

        File catalogEntry = metadataService.getFileById(result.newFileId);
        assertTrue("Catalog entry should be deleted after rollback",
                catalogEntry == null || catalogEntry.getId() == 0);
    }

    /** Delayed cleanup removes old file Visibility and physical file after wall-clock deadline passes. */
    @Test
    public void testAtomicSwap_delayedCleanup() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
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

    /** processFileGroup completes the full pipeline without throwing. */
    @Test
    public void testProcessFileGroup_fullPipeline() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long[] ids = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        long[] ts = new long[10];
        Arrays.fill(ts, 100);
        String filePath = writeTestFile("pipeline_src.pxl", schema, ids, true, ts);

        File srcFile = new File();
        srcFile.setName("pipeline_src.pxl");
        srcFile.setType(File.Type.REGULAR);
        srcFile.setNumRowGroup(1);
        srcFile.setMinRowId(0);
        srcFile.setMaxRowId(9);
        srcFile.setPathId(testPathId);
        metadataService.addFiles(Collections.singletonList(srcFile));
        long srcFileId = metadataService.getFileId(filePath);

        StorageGarbageCollector.FileGroup group = makeGroup(srcFileId, filePath, schema);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(RetinaUtils.buildRgKey(srcFileId, 0), makeBitmap(10, 6));

        retinaManager.addVisibility(srcFileId, 0, 10, 50, null, true);

        gc.processFileGroup(group, 100, bitmaps);

        assertTrue("Bitmaps should be consumed after processFileGroup", bitmaps.isEmpty());
    }

    /**
     * processFileGroup success path using {@link NoIndexSyncGC} (which stubs out
     * syncIndex).  Verifies that after the pipeline completes:
     * <ul>
     *   <li>Old file is removed from the catalog</li>
     *   <li>A new REGULAR file exists</li>
     *   <li>Rewritten data contains only survivors</li>
     *   <li>gcSnapshotBitmaps are fully consumed</li>
     * </ul>
     */
    @Test
    public void testProcessFileGroup_successWithNoIndex() throws Exception
    {
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        long[] ids = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        long[] ts = new long[10];
        Arrays.fill(ts, 100);
        String filePath = writeTestFile("pipeline_ok_src.pxl", schema, ids, true, ts);

        File srcFile = new File();
        srcFile.setName("pipeline_ok_src.pxl");
        srcFile.setType(File.Type.REGULAR);
        srcFile.setNumRowGroup(1);
        srcFile.setMinRowId(0);
        srcFile.setMaxRowId(9);
        srcFile.setPathId(testPathId);
        metadataService.addFiles(Collections.singletonList(srcFile));
        long srcFileId = metadataService.getFileId(filePath);

        retinaManager.addVisibility(srcFileId, 0, 10, 50, null, true);

        Map<String, long[]> bitmaps = new HashMap<>();
        bitmaps.put(RetinaUtils.buildRgKey(srcFileId, 0), makeBitmap(10, 6));

        Set<Long> beforeFileIds = new HashSet<>();
        for (File f : metadataService.getFiles(testPathId))
        {
            beforeFileIds.add(f.getId());
        }

        NoIndexSyncGC noIdxGc = new NoIndexSyncGC(retinaManager, metadataService,
                0.5, 134_217_728L, Integer.MAX_VALUE, 10, 1048576, EncodingLevel.EL2, 86_400_000L);

        StorageGarbageCollector.FileGroup group = makeGroup(srcFileId, filePath, schema);

        noIdxGc.processFileGroup(group, 100, bitmaps);

        assertTrue("Bitmaps should be consumed", bitmaps.isEmpty());

        File oldFileCheck = metadataService.getFileById(srcFileId);
        assertTrue("Old file should be gone from catalog after successful GC",
                oldFileCheck == null || oldFileCheck.getId() == 0);

        List<File> afterFiles = metadataService.getFiles(testPathId);
        boolean foundNewRegular = false;
        for (File f : afterFiles)
        {
            if (!beforeFileIds.contains(f.getId()) && f.getType() == File.Type.REGULAR)
            {
                foundNewRegular = true;
                break;
            }
        }
        assertTrue("A new REGULAR file should exist after successful pipeline", foundNewRegular);
    }

    // =======================================================================
    // Section 8b: TypeDescription.convertColumnVectorToByte type coverage
    // =======================================================================

    /** INT column: value serialized as 4-byte big-endian. */
    @Test
    public void testConvertColumnVectorToByte_int()
    {
        LongColumnVector col = new LongColumnVector(2);
        col.vector[0] = 42;
        col.vector[1] = -1;

        byte[] bytes0 = TypeDescription.createInt().convertColumnVectorToByte(col, 0);
        assertEquals(Integer.BYTES, bytes0.length);
        assertEquals(42, ByteBuffer.wrap(bytes0).getInt());

        byte[] bytes1 = TypeDescription.createInt().convertColumnVectorToByte(col, 1);
        assertEquals(-1, ByteBuffer.wrap(bytes1).getInt());
    }

    /** LONG column: value serialized as 8-byte big-endian. */
    @Test
    public void testConvertColumnVectorToByte_long()
    {
        LongColumnVector col = new LongColumnVector(1);
        col.vector[0] = Long.MAX_VALUE;

        byte[] bytes = TypeDescription.createLong().convertColumnVectorToByte(col, 0);
        assertEquals(Long.BYTES, bytes.length);
        assertEquals(Long.MAX_VALUE, ByteBuffer.wrap(bytes).getLong());
    }

    /** FLOAT column: raw int bits serialized as 4-byte big-endian. */
    @Test
    public void testConvertColumnVectorToByte_float()
    {
        FloatColumnVector col = new FloatColumnVector(1);
        col.vector[0] = Float.floatToIntBits(3.14f);

        byte[] bytes = TypeDescription.createFloat().convertColumnVectorToByte(col, 0);
        assertEquals(Integer.BYTES, bytes.length);
        assertEquals(Float.floatToIntBits(3.14f), ByteBuffer.wrap(bytes).getInt());
    }

    /** DOUBLE column: raw long bits serialized as 8-byte big-endian. */
    @Test
    public void testConvertColumnVectorToByte_double()
    {
        DoubleColumnVector col = new DoubleColumnVector(1);
        col.vector[0] = Double.doubleToLongBits(2.718);

        byte[] bytes = TypeDescription.createDouble().convertColumnVectorToByte(col, 0);
        assertEquals(Long.BYTES, bytes.length);
        assertEquals(Double.doubleToLongBits(2.718), ByteBuffer.wrap(bytes).getLong());
    }

    /** VARCHAR/STRING column: variable-length byte slice extracted correctly. */
    @Test
    public void testConvertColumnVectorToByte_string()
    {
        BinaryColumnVector col = new BinaryColumnVector(2);
        byte[] hello = "hello".getBytes();
        byte[] world = "world!".getBytes();
        col.setVal(0, hello);
        col.setVal(1, world);

        byte[] bytes0 = TypeDescription.createVarchar(255).convertColumnVectorToByte(col, 0);
        assertTrue("string bytes must match", Arrays.equals(hello, bytes0));

        byte[] bytes1 = TypeDescription.createString().convertColumnVectorToByte(col, 1);
        assertTrue("string bytes must match", Arrays.equals(world, bytes1));
    }

    /** SHORT DECIMAL column: value serialized as 8-byte big-endian long. */
    @Test
    public void testConvertColumnVectorToByte_shortDecimal()
    {
        DecimalColumnVector col = new DecimalColumnVector(10, 2);
        col.vector[0] = 12345L;

        TypeDescription decType = TypeDescription.createDecimal(10, 2);
        byte[] bytes = decType.convertColumnVectorToByte(col, 0);
        assertEquals(Long.BYTES, bytes.length);
        assertEquals(12345L, ByteBuffer.wrap(bytes).getLong());
    }

    /** BOOLEAN column: value serialized as single byte. */
    @Test
    public void testConvertColumnVectorToByte_boolean()
    {
        LongColumnVector col = new LongColumnVector(2);
        col.vector[0] = 1;
        col.vector[1] = 0;

        byte[] bytes0 = TypeDescription.createBoolean().convertColumnVectorToByte(col, 0);
        assertEquals(1, bytes0.length);
        assertEquals(1, bytes0[0]);

        byte[] bytes1 = TypeDescription.createBoolean().convertColumnVectorToByte(col, 1);
        assertEquals(1, bytes1.length);
        assertEquals(0, bytes1[0]);
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
        TypeDescription schema = TypeDescription.fromString("struct<id:long>");
        int numRows = 10;
        long[] ids = new long[numRows];
        long[] createTs = new long[numRows];
        for (int i = 0; i < numRows; i++)
        {
            ids[i] = i * 10;
            createTs[i] = 50L;
        }
        String srcPath = writeTestFile("e2e_full_src.pxl", schema, ids, true, createTs);

        File srcFile = new File();
        srcFile.setName("e2e_full_src.pxl");
        srcFile.setType(File.Type.REGULAR);
        srcFile.setNumRowGroup(1);
        srcFile.setMinRowId(0);
        srcFile.setMaxRowId(numRows - 1);
        srcFile.setPathId(testPathId);
        metadataService.addFiles(Collections.singletonList(srcFile));
        long srcFileId = metadataService.getFileId(srcPath);
        assertTrue("source file must have a valid catalog id", srcFileId > 0);

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
            assertTrue("row " + r + " should be in GC bitmap",
                    (gcBitmap[r / 64] & (1L << (r % 64))) != 0);
        }
        for (int r : new int[]{1, 3, 5, 7})
        {
            assertFalse("row " + r + " should NOT be in GC bitmap",
                    (gcBitmap[r / 64] & (1L << (r % 64))) != 0);
        }

        NoIndexSyncGC e2eGc = new NoIndexSyncGC(retinaManager, metadataService,
                0.5, 134_217_728L, Integer.MAX_VALUE, 10, 1048576,
                EncodingLevel.EL2, 86_400_000L);

        StorageGarbageCollector.FileGroup group = makeGroup(srcFileId, srcPath, schema);

        StorageGarbageCollector.RewriteResult result =
                e2eGc.rewriteFileGroup(group, safeGcTs, bitmaps);
        long newFileId = result.newFileId;
        assertTrue("new file must be created", newFileId > 0);
        assertRewriteResultConsistency(result, 4);

        long[][] rows = readAllRows(result.newFilePath, schema, true);
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
                (dualBm[newRowForOld3 / 64] & (1L << (newRowForOld3 % 64))) != 0);

        e2eGc.syncVisibility(result, safeGcTs);

        int newRowForOld1 = fwd[1];
        long[] syncBm = retinaManager.queryVisibility(newFileId, 0, 150L, 0L);
        assertTrue("sync: new row " + newRowForOld1 + " should show old row 1 deleted at ts=150",
                (syncBm[newRowForOld1 / 64] & (1L << (newRowForOld1 % 64))) != 0);

        retinaManager.deleteRecord(srcFileId, 0, 5, 300L);
        int newRowForOld5 = fwd[5];
        assertTrue("fwd[5] should be valid", newRowForOld5 >= 0);

        e2eGc.syncIndex(result, group.tableId);
        e2eGc.commitFileGroup(result);

        File newFileCheck = metadataService.getFileById(newFileId);
        assertNotNull("new file should exist after commit", newFileCheck);
        assertEquals("new file should be REGULAR after commit",
                File.Type.REGULAR, newFileCheck.getType());

        File oldFileCheck = metadataService.getFileById(srcFileId);
        assertTrue("old file should be gone from catalog after commit",
                oldFileCheck == null || oldFileCheck.getId() == 0);

        assertTrue("old physical file should still exist (delayed cleanup, not yet due)",
                fileStorage.exists(srcPath));

        for (long snap : new long[]{100L, 149L, 150L, 199L, 200L, 299L, 300L, 500L})
        {
            long[] bm = retinaManager.queryVisibility(newFileId, 0, snap, 0L);

            boolean r0del = (bm[0] & (1L << 0)) != 0;
            assertEquals("snap=" + snap + " newRow0 (old row1, del@150)", snap >= 150, r0del);

            boolean r1del = (bm[0] & (1L << 1)) != 0;
            assertEquals("snap=" + snap + " newRow1 (old row3, del@200)", snap >= 200, r1del);

            boolean r2del = (bm[0] & (1L << 2)) != 0;
            assertEquals("snap=" + snap + " newRow2 (old row5, del@300)", snap >= 300, r2del);

            boolean r3del = (bm[0] & (1L << 3)) != 0;
            assertFalse("snap=" + snap + " newRow3 (old row7) should never be deleted", r3del);
        }

        for (long snap : new long[]{100L, 150L, 200L, 300L, 500L})
        {
            long[] oldBm = retinaManager.queryVisibility(srcFileId, 0, snap, 0L);
            long[] newBm = retinaManager.queryVisibility(newFileId, 0, snap, 0L);
            for (int oldRow = 1; oldRow <= 7; oldRow += 2)
            {
                int newRow = fwd[oldRow];
                assertTrue("old row " + oldRow + " should have valid mapping", newRow >= 0);
                boolean oldDel = (oldBm[oldRow / 64] & (1L << (oldRow % 64))) != 0;
                boolean newDel = (newBm[newRow / 64] & (1L << (newRow % 64))) != 0;
                assertEquals("snap=" + snap + " old row " + oldRow + " vs new row " + newRow
                        + " visibility mismatch", oldDel, newDel);
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

            Field retiredField = RetinaResourceManager.class.getDeclaredField("retiredFiles");
            retiredField.setAccessible(true);
            ((java.util.Queue<?>) retiredField.get(retinaManager)).clear();
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
