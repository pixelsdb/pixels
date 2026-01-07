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

import java.util.Collections;
import java.util.List;

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
}
