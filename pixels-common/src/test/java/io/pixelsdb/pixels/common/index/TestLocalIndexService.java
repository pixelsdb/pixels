/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.common.index;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.index.service.LocalIndexService;
import io.pixelsdb.pixels.index.IndexProto;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TestLocalIndexService
{

    private static LocalIndexService indexService;
    private static final long TABLE_ID = 1L;
    private static final long PRIMARY_INDEX_ID = 100L;
    private static final long SECONDARY_INDEX_ID = 200L;
    private static IndexOption indexOption;
    private static IndexProto.PrimaryIndexEntry primaryEntry;
    private static IndexProto.SecondaryIndexEntry secondaryEntry;

    @BeforeAll
    static void setup() throws Exception
    {
        indexService = LocalIndexService.Instance();

        // open index
        assertTrue(indexService.openIndex(TABLE_ID, PRIMARY_INDEX_ID, true, indexOption));
        assertTrue(indexService.openIndex(TABLE_ID, SECONDARY_INDEX_ID, false, indexOption));

        // delicate RowId
        IndexProto.RowIdBatch batch = indexService.allocateRowIdBatch(TABLE_ID, 1);
        assertNotNull(batch);
        long rowId = batch.getRowIdStart();

        // build PrimaryEntry
        primaryEntry = IndexProto.PrimaryIndexEntry.newBuilder()
                .setRowId(rowId)
                .setIndexKey(IndexProto.IndexKey.newBuilder()
                        .setTableId(TABLE_ID)
                        .setIndexId(PRIMARY_INDEX_ID)
                        .setKey(ByteString.copyFromUtf8("key1"))
                        .setTimestamp(12345678))
                .setRowLocation(IndexProto.RowLocation.newBuilder()
                        .setFileId(1)
                        .setRgId(1)
                        .setRgRowOffset(0))
                .build();

        // build SecondaryEntry
        secondaryEntry = IndexProto.SecondaryIndexEntry.newBuilder()
                .setRowId(rowId)
                .setIndexKey(IndexProto.IndexKey.newBuilder()
                        .setTableId(TABLE_ID)
                        .setIndexId(SECONDARY_INDEX_ID)
                        .setKey(ByteString.copyFromUtf8("key1"))
                        .setTimestamp(12345678))
                .build();

        indexOption = IndexOption.builder().vNodeId(0).build();
    }

    @Test
    @Order(1)
    void testPutPrimaryAndSecondaryIndex() throws Exception
    {
        assertTrue(indexService.putPrimaryIndexEntry(primaryEntry, indexOption));
        assertTrue(indexService.putSecondaryIndexEntry(secondaryEntry, indexOption));
    }

    @Test
    @Order(2)
    void testLookupIndex() throws Exception
    {
        // lookup primary
        IndexProto.RowLocation primaryLocation = indexService.lookupUniqueIndex(primaryEntry.getIndexKey(), indexOption);
        assertNotNull(primaryLocation);
        assertEquals(1, primaryLocation.getFileId());

        // lookup secondary
        List<IndexProto.RowLocation> secondaryLocations = indexService.lookupNonUniqueIndex(secondaryEntry.getIndexKey(), indexOption);
        assertNotNull(secondaryLocations);
        assertEquals(1, secondaryLocations.size());
    }

    @Test
    @Order(3)
    void testUpdateIndex() throws Exception
    {
        long newRowId = primaryEntry.getRowId() + 1;
        IndexProto.PrimaryIndexEntry updatedPrimary = primaryEntry.toBuilder()
                .setRowId(newRowId)
                .build();
        IndexProto.RowLocation prevLocation = indexService.updatePrimaryIndexEntry(updatedPrimary, indexOption);
        assertNotNull(prevLocation);

        List<Long> prevSecondaryRowIds = indexService.updateSecondaryIndexEntry(secondaryEntry, indexOption);
        assertNotNull(prevSecondaryRowIds);
    }

    @Test
    @Order(4)
    void testDeleteIndex() throws Exception
    {
        // delete primary
        IndexProto.RowLocation deletedPrimaryLocation = indexService.deletePrimaryIndexEntry(primaryEntry.getIndexKey(), indexOption);
        assertNotNull(deletedPrimaryLocation);

        // delete secondary
        List<Long> deletedSecondaryRowIds = indexService.deleteSecondaryIndexEntry(secondaryEntry.getIndexKey(), indexOption);
        assertEquals(1, deletedSecondaryRowIds.size());
    }

    @Test
    @Order(5)
    void testPurgeAndFlush() throws Exception
    {
        assertTrue(indexService.putPrimaryIndexEntry(primaryEntry, indexOption));
        assertTrue(indexService.putSecondaryIndexEntry(secondaryEntry, indexOption));

        // purge primary
        boolean purged = indexService.purgeIndexEntries(TABLE_ID, PRIMARY_INDEX_ID,
                Collections.singletonList(primaryEntry.getIndexKey()), true, indexOption);
        assertTrue(purged);

        // flush primary
        assertTrue(indexService.flushIndexEntriesOfFile(TABLE_ID, PRIMARY_INDEX_ID, 1L, true, indexOption));
    }

    @Test
    @Order(6)
    void testCloseAndRemoveIndex() throws Exception
    {
        // close
        assertTrue(indexService.closeIndex(TABLE_ID, PRIMARY_INDEX_ID, true, indexOption));
        assertTrue(indexService.closeIndex(TABLE_ID, SECONDARY_INDEX_ID, false, indexOption));

        // remove
        assertTrue(indexService.removeIndex(TABLE_ID, PRIMARY_INDEX_ID, true, indexOption));
        assertTrue(indexService.removeIndex(TABLE_ID, SECONDARY_INDEX_ID, false, indexOption));
    }

    // =====================================================================
    // Staged primary-index API tests. These run after the legacy tests have
    // closed/removed the index, so each test re-opens its own (tableId, indexId)
    // pair to stay isolated.
    // =====================================================================

    private static final long STAGED_TABLE_ID = 9001L;
    private static final long STAGED_PRIMARY_INDEX_ID = 9002L;

    private static IndexProto.PrimaryIndexEntry stagedEntry(String keyStr, long rowId, long fileId, int rgId, int rgOffset)
    {
        return IndexProto.PrimaryIndexEntry.newBuilder()
                .setRowId(rowId)
                .setIndexKey(IndexProto.IndexKey.newBuilder()
                        .setTableId(STAGED_TABLE_ID)
                        .setIndexId(STAGED_PRIMARY_INDEX_ID)
                        .setKey(ByteString.copyFromUtf8(keyStr))
                        .setTimestamp(1000L))
                .setRowLocation(IndexProto.RowLocation.newBuilder()
                        .setFileId(fileId).setRgId(rgId).setRgRowOffset(rgOffset))
                .build();
    }

    @Test
    @Order(10)
    void testStagedPutMainIndexThenPutPrimaryRoundTrip() throws Exception
    {
        IndexOption opt = IndexOption.builder().vNodeId(0).build();
        assertTrue(indexService.openIndex(STAGED_TABLE_ID, STAGED_PRIMARY_INDEX_ID, true, opt));

        IndexProto.RowIdBatch batch = indexService.allocateRowIdBatch(STAGED_TABLE_ID, 2);
        long row0 = batch.getRowIdStart();
        long row1 = row0 + 1;
        IndexProto.PrimaryIndexEntry e0 = stagedEntry("staged-k0", row0, 100L, 0, 0);
        IndexProto.PrimaryIndexEntry e1 = stagedEntry("staged-k1", row1, 100L, 0, 1);

        indexService.putMainIndexEntriesOnly(STAGED_TABLE_ID, Arrays.asList(e0, e1));
        indexService.putPrimaryIndexEntriesOnly(STAGED_TABLE_ID, STAGED_PRIMARY_INDEX_ID, Arrays.asList(e0, e1), opt);

        List<Optional<ResolvedPrimary>> resolved = indexService.resolvePrimary(
                STAGED_TABLE_ID, STAGED_PRIMARY_INDEX_ID,
                Arrays.asList(e0.getIndexKey(), e1.getIndexKey()), opt);
        assertEquals(2, resolved.size());
        assertTrue(resolved.get(0).isPresent());
        assertEquals(row0, resolved.get(0).get().getRowId());
        assertEquals(100L, resolved.get(0).get().getRowLocation().getFileId());
        assertTrue(resolved.get(1).isPresent());
        assertEquals(row1, resolved.get(1).get().getRowId());
    }

    @Test
    @Order(11)
    void testStagedResolvePrimaryReturnsEmptyForUnknownKey() throws Exception
    {
        IndexOption opt = IndexOption.builder().vNodeId(0).build();
        IndexProto.IndexKey unknown = IndexProto.IndexKey.newBuilder()
                .setTableId(STAGED_TABLE_ID)
                .setIndexId(STAGED_PRIMARY_INDEX_ID)
                .setKey(ByteString.copyFromUtf8("staged-not-there"))
                .setTimestamp(1000L)
                .build();

        List<Optional<ResolvedPrimary>> resolved = indexService.resolvePrimary(
                STAGED_TABLE_ID, STAGED_PRIMARY_INDEX_ID, Collections.singletonList(unknown), opt);
        assertEquals(1, resolved.size());
        assertFalse(resolved.get(0).isPresent());
    }

    @Test
    @Order(13)
    void testStagedTombstonePrimaryResolvedIsIdempotent() throws Exception
    {
        IndexOption opt = IndexOption.builder().vNodeId(0).build();
        IndexProto.IndexKey k0 = stagedEntry("staged-k0", 0L, 100L, 0, 0).getIndexKey();

        // First tombstone removes the live primary entry.
        indexService.deletePrimaryIndexEntriesOnly(STAGED_TABLE_ID, STAGED_PRIMARY_INDEX_ID,
                Collections.singletonList(k0), opt);

        List<Optional<ResolvedPrimary>> resolved = indexService.resolvePrimary(
                STAGED_TABLE_ID, STAGED_PRIMARY_INDEX_ID, Collections.singletonList(k0), opt);
        assertFalse(resolved.get(0).isPresent());

        // Repeated tombstone of an already-tombstoned key must be a no-op (idempotency invariant).
        assertDoesNotThrow(() -> indexService.deletePrimaryIndexEntriesOnly(
                STAGED_TABLE_ID, STAGED_PRIMARY_INDEX_ID, Collections.singletonList(k0), opt));
    }

    @Test
    @Order(14)
    void testStagedUpdateResolvedThenRestorePrimaryEntries() throws Exception
    {
        IndexOption opt = IndexOption.builder().vNodeId(0).build();
        IndexProto.IndexKey k1 = stagedEntry("staged-k1", 0L, 100L, 0, 1).getIndexKey();
        long oldRowId = indexService.resolvePrimary(STAGED_TABLE_ID, STAGED_PRIMARY_INDEX_ID,
                Collections.singletonList(k1), opt).get(0).get().getRowId();

        long newRowId = oldRowId + 100;
        IndexProto.PrimaryIndexEntry newEntry = stagedEntry("staged-k1", newRowId, 101L, 0, 0);
        indexService.putMainIndexEntriesOnly(STAGED_TABLE_ID, Collections.singletonList(newEntry));
        indexService.updatePrimaryIndexEntriesOnly(STAGED_TABLE_ID, STAGED_PRIMARY_INDEX_ID,
                Collections.singletonList(newEntry), opt);

        Optional<ResolvedPrimary> updated = indexService.resolvePrimary(
                STAGED_TABLE_ID, STAGED_PRIMARY_INDEX_ID, Collections.singletonList(k1), opt).get(0);
        assertTrue(updated.isPresent());
        assertEquals(newRowId, updated.get().getRowId());

        indexService.restorePrimaryIndexEntries(STAGED_TABLE_ID, STAGED_PRIMARY_INDEX_ID,
                Collections.singletonList(new RollbackEntry(k1, oldRowId, newRowId)), opt);

        Optional<ResolvedPrimary> restored = indexService.resolvePrimary(
                STAGED_TABLE_ID, STAGED_PRIMARY_INDEX_ID, Collections.singletonList(k1), opt).get(0);
        assertTrue(restored.isPresent());
        assertEquals(oldRowId, restored.get().getRowId());
    }

    @Test
    @Order(15)
    void testStagedRestorePrimaryEntriesSkipsNonMatchingCurrent() throws Exception
    {
        IndexOption opt = IndexOption.builder().vNodeId(0).build();
        IndexProto.IndexKey k1 = stagedEntry("staged-k1", 0L, 100L, 0, 1).getIndexKey();
        long currentRowId = indexService.resolvePrimary(STAGED_TABLE_ID, STAGED_PRIMARY_INDEX_ID,
                Collections.singletonList(k1), opt).get(0).get().getRowId();

        // Rollback entry says: switch from newRowId=currentRowId+5 back to oldRowId=currentRowId-7.
        // Since the actual current pointer is `currentRowId` (not newRowId=currentRowId+5), the
        // restore must be a no-op.
        RollbackEntry entry = new RollbackEntry(k1, currentRowId - 7, currentRowId + 5);
        indexService.restorePrimaryIndexEntries(STAGED_TABLE_ID, STAGED_PRIMARY_INDEX_ID,
                Collections.singletonList(entry), opt);

        // Verify primary still points at the original rowId, not the spurious oldRowId.
        Optional<ResolvedPrimary> after = indexService.resolvePrimary(
                STAGED_TABLE_ID, STAGED_PRIMARY_INDEX_ID, Collections.singletonList(k1), opt).get(0);
        assertTrue(after.isPresent());
        assertEquals(currentRowId, after.get().getRowId());
    }
}
