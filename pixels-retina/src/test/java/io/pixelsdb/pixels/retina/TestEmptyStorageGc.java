package io.pixelsdb.pixels.retina;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.index.service.IndexService;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import org.junit.Test;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class TestEmptyStorageGc
{
    private StorageGarbageCollector.RewriteResult emptyResult()
    {
        File file = new File();
        file.setId(101);
        file.setType(File.Type.REGULAR);
        StorageGarbageCollector.FileCandidate candidate = new StorageGarbageCollector.FileCandidate(
                file, "file:///tmp/obsolete.pxl", 101, 1, 11, 2, 1.0, 100);
        return new StorageGarbageCollector.RewriteResult(
                new StorageGarbageCollector.FileGroup(11, 2, Collections.singletonList(candidate)),
                "", -1, 0, new int[0], new int[]{0}, Collections.emptyMap(),
                Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void fullyDeletedGroupRetiresWithoutReplacementOrIndexWrites() throws Exception
    {
        ConfigFactory.Instance().addProperty("retina.storage.gc.journal.dir",
                Files.createTempDirectory("empty-gc-").toUri().toString());
        StorageGcWal wal = new StorageGcWal();
        RetinaResourceManager resource = mock(RetinaResourceManager.class);
        MetadataService metadata = mock(MetadataService.class);
        IndexService index = mock(IndexService.class);
        StorageGarbageCollector gc = new StorageGarbageCollector(resource, metadata, index,
                .5, 1024, 20, 10, 10240, EncodingLevel.from(2), 600000, wal);
        gc.retireEmptyFileGroup(emptyResult(), 500);
        verify(metadata).atomicSwapFiles(eq(0L), eq(Collections.singletonList(101L)), anyLong());
        verify(resource).scheduleRetiredFile(any(RetinaResourceManager.RetiredFile.class));
        verifyNoInteractions(index);
        StorageGcWal.Task task = wal.listAllTasks().get(0);
        assertTrue(task.isRetirementOnly());
        assertEquals(500, task.getRetirementSafeGcTs());
        assertEquals(StorageGcWal.State.SWAPPED_NOT_CHECKPOINTED, task.getState());
        assertEquals(Collections.singleton(101L), wal.collectPendingFileIds());
    }

    @Test
    public void failedCatalogRetirementAbortsJournalAndQueuesNoDeletion() throws Exception
    {
        ConfigFactory.Instance().addProperty("retina.storage.gc.journal.dir",
                Files.createTempDirectory("empty-gc-failed-").toUri().toString());
        StorageGcWal wal = new StorageGcWal();
        RetinaResourceManager resource = mock(RetinaResourceManager.class);
        MetadataService metadata = mock(MetadataService.class);
        IndexService index = mock(IndexService.class);
        File old = new File();
        old.setId(101); old.setType(File.Type.REGULAR);
        when(metadata.getFileById(101)).thenReturn(old);
        doThrow(new MetadataException("Injected retirement failure"))
                .when(metadata).atomicSwapFiles(eq(0L), anyList(), anyLong());
        StorageGarbageCollector gc = new StorageGarbageCollector(resource, metadata, index,
                .5, 1024, 20, 10, 10240, EncodingLevel.from(2), 600000, wal);
        try
        {
            gc.retireEmptyFileGroup(emptyResult(), 500);
            fail("Expected injected failure");
        }
        catch (MetadataException expected) { assertTrue(expected.getMessage().contains("Injected")); }
        verifyNoInteractions(resource, index);
        assertEquals(StorageGcWal.State.ABORTED, wal.listAllTasks().get(0).getState());
        assertTrue(wal.collectPendingFileIds().isEmpty());
        assertEquals(File.Type.REGULAR, old.getType());
    }

    @Test
    public void equallyInvalidGroupsChooseOldestFilesBeforeNewlyCreatedGroups() throws Exception
    {
        StorageGarbageCollector gc = new StorageGarbageCollector(null, null, null,
                .5, 1024, 20, 1, 10240, EncodingLevel.from(2), 600000, null);
        File newer = new File(); newer.setId(200);
        File older = new File(); older.setId(100);
        java.util.List<StorageGarbageCollector.FileGroup> groups = gc.groupAndMerge(Arrays.asList(
                new StorageGarbageCollector.FileCandidate(newer, "", 200, 1, 11, 0, 1.0, 100),
                new StorageGarbageCollector.FileCandidate(older, "", 100, 1, 11, 1, 1.0, 100)));
        assertEquals(1, groups.size());
        assertEquals(100, groups.get(0).files.get(0).fileId);
    }
}
