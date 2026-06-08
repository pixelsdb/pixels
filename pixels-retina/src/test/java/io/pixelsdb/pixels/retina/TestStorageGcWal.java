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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the Affero
 * GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.retina;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.index.IndexOption;
import io.pixelsdb.pixels.common.index.RollbackEntry;
import io.pixelsdb.pixels.common.index.service.IndexService;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.index.IndexProto;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the Storage GC append-only WAL: persistence, state machine,
 * store queries, and startup recovery.
 */
public class TestStorageGcWal
{
    @Before
    public void setUp() throws IOException
    {
        cleanupWalDir();
    }

    private static void cleanupWalDir() throws IOException
    {
        String walDir = ConfigFactory.Instance().getProperty("retina.storage.gc.journal.dir");
        Storage storage = StorageFactory.Instance().getStorage(walDir);
        if (!storage.exists(walDir))
        {
            return;
        }
        for (String path : storage.listPaths(walDir))
        {
            storage.delete(path, false);
        }
    }

    private static StorageGcWal newWal()
    {
        return new StorageGcWal();
    }

    // ─── WAL file format and state machine ───────────────────────────────────

    @Test
    public void testWal_createAndReplayTask() throws IOException
    {
        StorageGcWal wal = newWal();
        try (StorageGcWal.Writer w = wal.createTask(
                "task-1", 11L, 2, Arrays.asList(101L, 102L),
                201L, "file:///tmp/pixels/gc-201.pxl", 3000L, 2))
        {
            w.appendRollbackEntry(key("pk-a"), 10L, 20L);
            w.flush();
            w.markSwapped(); // closes the writer
        }

        Optional<StorageGcWal.Task> loaded = wal.getTask("task-1");
        assertTrue(loaded.isPresent());
        StorageGcWal.Task task = loaded.get();
        assertEquals("task-1", task.getTaskId());
        assertEquals(11L, task.getTableId());
        assertEquals(2, task.getVirtualNodeId());
        assertEquals(Arrays.asList(101L, 102L), task.getOldFileIds());
        assertEquals(201L, task.getNewFileId());
        assertEquals(3000L, task.getNewRowIdStart());
        assertEquals(2, task.getNewRowCount());
        assertEquals(StorageGcWal.State.SWAPPED_NOT_CHECKPOINTED, task.getState());
        assertEquals(1, task.getRollbackEntries().size());
        assertEquals(10L, task.getRollbackEntries().get(0).getOldRowId());
        assertEquals(20L, task.getRollbackEntries().get(0).getNewRowId());
    }

    @Test
    public void testWal_stateMachine_indexSwitchingToAborted() throws IOException
    {
        StorageGcWal wal = newWal();
        try (StorageGcWal.Writer w = wal.createTask(
                "task-abort", 11L, 2, Collections.singletonList(101L),
                201L, "", 3000L, 2))
        {
            w.appendRollbackEntry(key("pk-x"), 5L, 15L);
            w.markAborted(); // closes
        }

        StorageGcWal.Task task = wal.getTask("task-abort").get();
        assertEquals(StorageGcWal.State.ABORTED, task.getState());
        assertEquals(1, task.getRollbackEntries().size());
    }

    @Test
    public void testWal_markCheckpointed_coldPath() throws IOException
    {
        StorageGcWal wal = newWal();
        try (StorageGcWal.Writer w = wal.createTask(
                "task-ck", 11L, 2, Collections.singletonList(101L),
                201L, "", 3000L, 2))
        {
            w.markSwapped();
        }
        assertEquals(StorageGcWal.State.SWAPPED_NOT_CHECKPOINTED,
                wal.getTask("task-ck").get().getState());

        wal.markCheckpointed("task-ck");

        assertEquals(StorageGcWal.State.CHECKPOINTED,
                wal.getTask("task-ck").get().getState());
    }

    // ─── Store queries ────────────────────────────────────────────────────────

    @Test
    public void testWal_gcWorkflowAndQueries() throws IOException
    {
        StorageGcWal wal = newWal();

        // Pending task
        try (StorageGcWal.Writer w = wal.createTask(
                "task-1", 11L, 2, Arrays.asList(101L, 102L), 201L, "", 3000L, 2))
        {
            w.appendRollbackEntry(key("pk-a"), 10L, 20L);
            w.flush();
            w.markSwapped();
        }
        assertTrue(wal.collectPendingFileIds().containsAll(Arrays.asList(101L, 102L, 201L)));

        // Terminal tasks
        try (StorageGcWal.Writer w = wal.createTask(
                "task-terminal", 12L, 3, Collections.singletonList(102L), 202L, "", 4000L, 3))
        {
            w.markSwapped();
        }
        wal.markCheckpointed("task-terminal");

        try (StorageGcWal.Writer w = wal.createTask(
                "task-aborted", 13L, 4, Collections.singletonList(103L), 203L, "", 5000L, 4))
        {
            w.markAborted();
        }

        List<StorageGcWal.Task> terminalTasks = wal.listTerminalTasks();
        assertEquals(2, terminalTasks.size());

        wal.deleteTerminalTasks(Arrays.asList("task-terminal", "task-aborted"));
        assertFalse(wal.getTask("task-terminal").isPresent());
        assertFalse(wal.getTask("task-aborted").isPresent());

        // After marking task-1 checkpointed, pending set should be empty
        wal.markCheckpointed("task-1");
        assertTrue(wal.collectPendingFileIds().isEmpty());

        // Non-terminal task may not be deleted
        try (StorageGcWal.Writer w = wal.createTask(
                "task-pending", 14L, 5, Collections.singletonList(104L), 204L, "", 6000L, 5))
        {
            w.markSwapped();
        }
        try
        {
            wal.deleteTerminalTasks(Collections.singletonList("task-pending"));
            fail("deleteTerminalTasks must reject non-terminal tasks");
        }
        catch (IllegalArgumentException expected)
        {
            // expected
        }
    }

    // ─── Recovery tests ───────────────────────────────────────────────────────

    @Test
    public void testRecovery_acceptSwapAndMultiRoundLifecycle() throws Exception
    {
        StorageGcWal wal = newWal();

        // First round: already CHECKPOINTED
        try (StorageGcWal.Writer w = wal.createTask(
                "first-round", 11L, 2, Collections.singletonList(101L), 201L, "", 3000L, 2))
        {
            w.markSwapped();
        }
        wal.markCheckpointed("first-round");

        // Second round: SWAPPED, not yet checkpointed
        try (StorageGcWal.Writer w = wal.createTask(
                "second-round", 11L, 3, Collections.singletonList(201L), 301L, "", 4000L, 3))
        {
            w.markSwapped();
        }

        StorageGcWal.RecoveryHandler handler = new StorageGcWal.RecoveryHandler(
                wal, mock(MetadataService.class), mock(IndexService.class));
        // Baseline includes both 201L (first-round output) and 301L (second-round output)
        handler.recover(Set.of(201L, 301L));

        assertEquals(StorageGcWal.State.CHECKPOINTED, wal.getTask("second-round").get().getState());
        assertEquals(StorageGcWal.State.CHECKPOINTED, wal.getTask("first-round").get().getState());

        wal.deleteTerminalTasks(Arrays.asList("first-round", "second-round"));
        assertFalse(wal.getTask("first-round").isPresent());
        assertFalse(wal.getTask("second-round").isPresent());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testRecovery_rejectedSwapRollsBackCatalogAndIndex() throws Exception
    {
        StorageGcWal wal = newWal();
        IndexProto.IndexKey indexKey = key("pk-a");
        try (StorageGcWal.Writer w = wal.createTask(
                "task-rejected", 11L, 2, Collections.singletonList(101L), 201L, "", 3000L, 2))
        {
            w.appendRollbackEntry(indexKey, 10L, 20L);
            w.flush();
            w.markSwapped();
        }

        MetadataService metadataService = mock(MetadataService.class);
        IndexService indexService = mock(IndexService.class);
        SinglePointIndex primaryIndex = new SinglePointIndex();
        primaryIndex.setId(22L);
        File oldFile = catalogFile(101L, File.Type.RETIRED, 123456L);
        File newFile = catalogFile(201L, File.Type.REGULAR, null);

        when(metadataService.getPrimaryIndex(11L)).thenReturn(primaryIndex);
        when(metadataService.getFileById(101L)).thenReturn(oldFile);
        when(metadataService.getFileById(201L)).thenReturn(newFile);
        when(metadataService.updateFile(any(File.class))).thenReturn(true);
        when(metadataService.deleteFiles(Collections.singletonList(201L))).thenReturn(true);

        StorageGcWal.RecoveryHandler handler = new StorageGcWal.RecoveryHandler(
                wal, metadataService, indexService);
        handler.recover(Collections.singleton(101L));

        assertEquals(StorageGcWal.State.ABORTED, wal.getTask("task-rejected").get().getState());
        assertEquals(File.Type.REGULAR, oldFile.getType());
        assertEquals(null, oldFile.getCleanupAt());

        ArgumentCaptor<List> rollbackCaptor = ArgumentCaptor.forClass(List.class);
        verify(indexService).restorePrimaryIndexEntries(
                eq(11L), eq(22L), rollbackCaptor.capture(), any(IndexOption.class));
        RollbackEntry rollbackEntry = (RollbackEntry) rollbackCaptor.getValue().get(0);
        assertEquals(indexKey, rollbackEntry.getIndexKey());
        assertEquals(10L, rollbackEntry.getOldRowId());
        assertEquals(20L, rollbackEntry.getNewRowId());
        verify(indexService).deleteMainIndexRange(11L, 201L, 3000L, 2);
        verify(metadataService).updateFile(oldFile);
        verify(metadataService).deleteFiles(Collections.singletonList(201L));
    }

    @Test
    public void testRecovery_incompleteAndAbortedTasks() throws Exception
    {
        StorageGcWal wal = newWal();
        IndexProto.IndexKey indexKey = key("pk-index-switching");

        // INDEX_SWITCHING: WAL has rollback entry but no state transition (crash mid-sync)
        try (StorageGcWal.Writer w = wal.createTask(
                "task-index-switching", 11L, 2, Collections.singletonList(101L), 201L, "", 3000L, 2))
        {
            w.appendRollbackEntry(indexKey, 10L, 20L);
            w.flush();
            // no markSwapped/markAborted — simulates crash; Writer.close() leaves INDEX_SWITCHING
        }

        // Already ABORTED
        try (StorageGcWal.Writer w = wal.createTask(
                "task-aborted", 11L, 2, Collections.singletonList(105L), 205L, "", 5000L, 5))
        {
            w.markAborted();
        }

        MetadataService metadataService = mock(MetadataService.class);
        IndexService indexService = mock(IndexService.class);
        SinglePointIndex primaryIndex = new SinglePointIndex();
        primaryIndex.setId(22L);
        File switchingOldFile = catalogFile(101L, File.Type.REGULAR, null);
        File switchingNewFile = catalogFile(201L, File.Type.TEMPORARY_GC, null);
        File abortedNewFile   = catalogFile(205L, File.Type.TEMPORARY_GC, null);

        when(metadataService.getPrimaryIndex(11L)).thenReturn(primaryIndex);
        when(metadataService.getFileById(101L)).thenReturn(switchingOldFile);
        when(metadataService.getFileById(201L)).thenReturn(switchingNewFile);
        when(metadataService.getFileById(205L)).thenReturn(abortedNewFile);
        when(metadataService.deleteFiles(any())).thenReturn(true);

        StorageGcWal.RecoveryHandler handler = new StorageGcWal.RecoveryHandler(
                wal, metadataService, indexService);
        handler.recover(Collections.emptySet());

        assertEquals(StorageGcWal.State.ABORTED,
                wal.getTask("task-index-switching").get().getState());
        assertEquals(StorageGcWal.State.ABORTED,
                wal.getTask("task-aborted").get().getState());
        verify(indexService).restorePrimaryIndexEntries(
                eq(11L), eq(22L),
                org.mockito.ArgumentMatchers.<List<RollbackEntry>>any(),
                any(IndexOption.class));
        verify(indexService).deleteMainIndexRange(11L, 201L, 3000L, 2);
        verify(indexService).deleteMainIndexRange(11L, 205L, 5000L, 5);
        verify(metadataService).deleteFiles(Collections.singletonList(201L));
        verify(metadataService).deleteFiles(Collections.singletonList(205L));
        verify(metadataService, never()).updateFile(switchingOldFile);
    }

    @Test
    public void testRecovery_checkpointedMissingFromBaselineFailsClosed() throws Exception
    {
        StorageGcWal wal = newWal();
        try (StorageGcWal.Writer w = wal.createTask(
                "task-final", 11L, 2, Collections.singletonList(101L), 201L, "", 3000L, 2))
        {
            w.markSwapped();
        }
        wal.markCheckpointed("task-final");

        StorageGcWal.RecoveryHandler handler = new StorageGcWal.RecoveryHandler(
                wal, mock(MetadataService.class), mock(IndexService.class));
        try
        {
            handler.recover(Collections.singleton(101L));
            fail("CHECKPOINTED WAL task missing from baseline must fail closed");
        }
        catch (RetinaException e)
        {
            assertEquals(StorageGcWal.State.CHECKPOINTED,
                    wal.getTask("task-final").get().getState());
        }
    }

    /**
     * Guards the GC ordering invariant from the recovery side: a checkpoint baseline can
     * only contain a newFile whose task already reached SWAPPED_NOT_CHECKPOINTED. If an
     * INDEX_SWITCHING task's newFile is found in the baseline, the invariant was violated
     * and recovery must fail closed rather than commit a half-switched primary index.
     */
    @Test
    public void testRecovery_indexSwitchingInBaseline_failsClosed() throws Exception
    {
        StorageGcWal wal = newWal();
        // Crash in the swap→markSwapped window leaves the task in INDEX_SWITCHING.
        try (StorageGcWal.Writer w = wal.createTask(
                "task-switching-in-baseline", 11L, 2, Collections.singletonList(101L), 201L, "", 3000L, 2))
        {
            w.appendRollbackEntry(key("pk-a"), 10L, 20L);
            w.flush();
            // no markSwapped/markAborted — leaves INDEX_SWITCHING
        }

        StorageGcWal.RecoveryHandler handler = new StorageGcWal.RecoveryHandler(
                wal, mock(MetadataService.class), mock(IndexService.class));
        try
        {
            // newFileId 201 present in the baseline while the task is still INDEX_SWITCHING.
            handler.recover(Collections.singleton(201L));
            fail("INDEX_SWITCHING task present in baseline must fail closed");
        }
        catch (RetinaException e)
        {
            assertTrue("error message must flag the invariant violation",
                    e.getMessage().contains("invariant violation"));
            // State must be left untouched: neither committed nor aborted.
            assertEquals(StorageGcWal.State.INDEX_SWITCHING,
                    wal.getTask("task-switching-in-baseline").get().getState());
        }
    }

    /**
     * INDEX_SWITCHING task whose newFile is NOT in the baseline, but whose catalog shows the
     * swap already committed (old RETIRED, new REGULAR). The rollback must restore the old
     * file catalog — {@code restoreOldFiles} is derived from {@link
     * StorageGcWal.RecoveryHandler} via {@code isSwapCommitted}, not hard-coded by state. This
     * complements {@link #testRecovery_incompleteAndAbortedTasks}, which covers the
     * swap-not-committed branch where the old catalog is left alone.
     */
    @Test
    public void testRecovery_indexSwitchingNotInBaseline_swapCommitted_restoresOldFiles() throws Exception
    {
        StorageGcWal wal = newWal();
        IndexProto.IndexKey indexKey = key("pk-a");
        try (StorageGcWal.Writer w = wal.createTask(
                "task-switching-committed", 11L, 2, Collections.singletonList(101L), 201L, "", 3000L, 2))
        {
            w.appendRollbackEntry(indexKey, 10L, 20L);
            w.flush();
            // no markSwapped — INDEX_SWITCHING, but the catalog below shows the swap committed
        }

        MetadataService metadataService = mock(MetadataService.class);
        IndexService indexService = mock(IndexService.class);
        SinglePointIndex primaryIndex = new SinglePointIndex();
        primaryIndex.setId(22L);
        // Swap committed before crash: old RETIRED, new REGULAR → isSwapCommitted() == true
        File oldFile = catalogFile(101L, File.Type.RETIRED, 123456L);
        File newFile = catalogFile(201L, File.Type.REGULAR, null);
        when(metadataService.getPrimaryIndex(11L)).thenReturn(primaryIndex);
        when(metadataService.getFileById(101L)).thenReturn(oldFile);
        when(metadataService.getFileById(201L)).thenReturn(newFile);
        when(metadataService.updateFile(any(File.class))).thenReturn(true);
        when(metadataService.deleteFiles(Collections.singletonList(201L))).thenReturn(true);

        StorageGcWal.RecoveryHandler handler = new StorageGcWal.RecoveryHandler(
                wal, metadataService, indexService);
        handler.recover(Collections.emptySet());

        assertEquals(StorageGcWal.State.ABORTED,
                wal.getTask("task-switching-committed").get().getState());
        // Swap committed → old file catalog restored to REGULAR with cleanupAt cleared.
        assertEquals(File.Type.REGULAR, oldFile.getType());
        assertEquals(null, oldFile.getCleanupAt());
        verify(metadataService).updateFile(oldFile);
        verify(indexService).deleteMainIndexRange(11L, 201L, 3000L, 2);
    }

    @Test
    public void testBug1_restoreRegularOldFile_updateFileMustBeCalled() throws Exception
    {
        StorageGcWal wal = newWal();
        try (StorageGcWal.Writer w = wal.createTask(
                "task-regular-old", 11L, 2, Collections.singletonList(101L), 201L, "", 3000L, 2))
        {
            w.markSwapped();
        }

        MetadataService metadataService = mock(MetadataService.class);
        IndexService indexService = mock(IndexService.class);
        File oldFile = catalogFile(101L, File.Type.REGULAR, 123456L);
        File newFile = catalogFile(201L, File.Type.REGULAR, null);

        when(metadataService.getFileById(101L)).thenReturn(oldFile);
        when(metadataService.getFileById(201L)).thenReturn(newFile);
        when(metadataService.updateFile(any(File.class))).thenReturn(true);
        when(metadataService.deleteFiles(Collections.singletonList(201L))).thenReturn(true);

        StorageGcWal.RecoveryHandler handler = new StorageGcWal.RecoveryHandler(
                wal, metadataService, indexService);
        handler.recover(Collections.emptySet());

        assertEquals(null, oldFile.getCleanupAt());
        verify(metadataService).updateFile(oldFile);
    }

    @Test
    public void testCorruptedWalFile_failsWithActionableMessage() throws IOException
    {
        StorageGcWal wal = newWal();
        // Create a valid task so listAllTasks has something to iterate
        try (StorageGcWal.Writer w = wal.createTask(
                "valid-task", 11L, 2, Collections.singletonList(101L), 201L, "", 3000L, 2))
        {
            // leave in INDEX_SWITCHING (no markSwapped/markAborted)
        }

        // Inject a corrupted WAL file
        String walDir = ConfigFactory.Instance().getProperty("retina.storage.gc.journal.dir");
        Storage storage = StorageFactory.Instance().getStorage(walDir);
        String corruptedPath = io.pixelsdb.pixels.common.utils.RetinaUtils
                .buildStorageGcJournalPath(walDir, "corrupted-99");
        try (DataOutputStream out = storage.create(corruptedPath, true, 64))
        {
            out.write(new byte[]{0x01, 0x02, 0x03});
        }

        try
        {
            wal.listAllTasks();
            fail("expected IllegalStateException for corrupted WAL file");
        }
        catch (IllegalStateException e)
        {
            assertTrue("error message must contain the corrupted file path",
                    e.getMessage().contains("corrupted-99"));
            assertTrue("error message must guide operators to delete the file",
                    e.getMessage().contains("Delete this file"));
        }
    }

    @Test
    public void testRecovery_preAbortedTaskOnlyCleanedUp() throws Exception
    {
        StorageGcWal wal = newWal();

        // Already ABORTED from a previous run — only its new file is cleaned up again.
        try (StorageGcWal.Writer w = wal.createTask(
                "pre-aborted", 11L, 2, Collections.singletonList(100L), 200L, "", 2000L, 1))
        {
            w.markAborted();
        }

        // Needs rollback this run
        try (StorageGcWal.Writer w = wal.createTask(
                "needs-rollback", 11L, 2, Collections.singletonList(101L), 201L, "", 3000L, 2))
        {
            w.markSwapped();
        }

        MetadataService metadataService = mock(MetadataService.class);
        IndexService indexService = mock(IndexService.class);
        File oldFile         = catalogFile(101L, File.Type.RETIRED, 123456L);
        File newFile         = catalogFile(201L, File.Type.REGULAR, null);
        File preAbortedNew   = catalogFile(200L, File.Type.TEMPORARY_GC, null);
        when(metadataService.getFileById(101L)).thenReturn(oldFile);
        when(metadataService.getFileById(201L)).thenReturn(newFile);
        when(metadataService.getFileById(200L)).thenReturn(preAbortedNew);
        when(metadataService.updateFile(any(File.class))).thenReturn(true);
        when(metadataService.deleteFiles(any())).thenReturn(true);

        StorageGcWal.RecoveryHandler handler = new StorageGcWal.RecoveryHandler(
                wal, metadataService, indexService);
        handler.recover(Collections.emptySet());

        // Pre-aborted task: only its new file is cleaned up again (idempotent), no catalog restore.
        assertEquals(StorageGcWal.State.ABORTED, wal.getTask("pre-aborted").get().getState());
        verify(metadataService).deleteFiles(Collections.singletonList(200L));
        // Needs-rollback task: full rollback — old file catalog restored and new file cleaned up.
        assertEquals(StorageGcWal.State.ABORTED, wal.getTask("needs-rollback").get().getState());
        verify(metadataService).updateFile(oldFile);
        verify(metadataService).deleteFiles(Collections.singletonList(201L));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testRollbackEntry_negativeOldRowId_isSkippedInIndexRestore() throws Exception
    {
        StorageGcWal wal = newWal();
        try (StorageGcWal.Writer w = wal.createTask(
                "task-sentinel", 11L, 2, Collections.singletonList(101L), 201L, "", 3000L, 2))
        {
            w.appendRollbackEntry(key("pk-sentinel"), -1L, 20L);
            w.flush();
            w.markSwapped();
        }

        MetadataService metadataService = mock(MetadataService.class);
        IndexService indexService = mock(IndexService.class);
        File oldFile = catalogFile(101L, File.Type.RETIRED, 123456L);
        File newFile = catalogFile(201L, File.Type.REGULAR, null);
        when(metadataService.getFileById(101L)).thenReturn(oldFile);
        when(metadataService.getFileById(201L)).thenReturn(newFile);
        when(metadataService.updateFile(any(File.class))).thenReturn(true);
        when(metadataService.deleteFiles(any())).thenReturn(true);

        StorageGcWal.RecoveryHandler handler = new StorageGcWal.RecoveryHandler(
                wal, metadataService, indexService);
        handler.recover(Collections.emptySet());

        // oldRowId=-1 → no prior entry → restorePrimaryIndexEntries must NOT be called
        verify(indexService, never()).restorePrimaryIndexEntries(
                anyLong(), anyLong(), any(List.class), any(IndexOption.class));
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private static File catalogFile(long id, File.Type type, Long cleanupAt)
    {
        File file = new File();
        file.setId(id);
        file.setName("f-" + id + ".pxl");
        file.setType(type);
        file.setNumRowGroup(1);
        file.setMinRowId(0L);
        file.setMaxRowId(9L);
        file.setPathId(1L);
        file.setCleanupAt(cleanupAt);
        return file;
    }

    private static IndexProto.IndexKey key(String value)
    {
        return IndexProto.IndexKey.newBuilder()
                .setTableId(11L)
                .setIndexId(22L)
                .setKey(ByteString.copyFromUtf8(value))
                .setTimestamp(33L)
                .build();
    }
}
