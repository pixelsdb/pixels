/*
 * Copyright 2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.pixelsdb.pixels.daemon.index;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.index.IndexOption;
import io.pixelsdb.pixels.common.index.service.LocalIndexService;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.index.IndexProto;
import org.junit.jupiter.api.*;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestIndexUpsert
{
    private static LocalIndexService indexService;
    private static final long TABLE_ID = 3622L;
    private static final long PRIMARY_INDEX_ID = 3560L;
    private static IndexOption indexOption;
    private static IndexProto.PrimaryIndexEntry missingEntry;

    @BeforeAll
    static void setup() throws Exception
    {
        ConfigFactory.Instance().addProperty("retina.upsert-mode.enabled", "true");
        indexService = LocalIndexService.Instance();
        indexOption = IndexOption.builder().vNodeId(0).build();
        // 1. Enable Upsert Mode (Ensure your config utility supports this)
        // PixelsConfig.set("retina.index.upsert-mode.enabled", "true");

        indexService.openIndex(TABLE_ID, PRIMARY_INDEX_ID, true, indexOption);

        // Define an entry that is NOT in the database yet
        missingEntry = IndexProto.PrimaryIndexEntry.newBuilder()
                .setRowId(9999L)
                .setIndexKey(IndexProto.IndexKey.newBuilder()
                        .setTableId(TABLE_ID)
                        .setIndexId(PRIMARY_INDEX_ID)
                        .setKey(ByteString.copyFromUtf8("ghost_key"))
                        .setTimestamp(System.currentTimeMillis()))
                .setRowLocation(IndexProto.RowLocation.newBuilder()
                        .setFileId(10)
                        .setRgId(1)
                        .setRgRowOffset(500))
                .build();
    }

    @Test
    @Order(1)
    @DisplayName("Test UPDATE on missing key (Should Insert)")
    void testUpdateMissingKey() throws Exception
    {
        // In standard mode, this would throw IndexException.
        // In Upsert mode, it returns null and inserts the data.
        IndexProto.RowLocation prevLocation = indexService.updatePrimaryIndexEntry(missingEntry, indexOption);

        // Assertions
        assertNull(prevLocation, "Upsert should return null when no previous entry exists");

        // Verify the data was actually inserted
        IndexProto.RowLocation currentLoc = indexService.lookupUniqueIndex(missingEntry.getIndexKey(), indexOption);
        assertNotNull(currentLoc, "Entry should have been inserted by the update call");
        assertEquals(10, currentLoc.getFileId());
    }

    @Test
    @Order(2)
    @DisplayName("Test DELETE on missing key (Should Ignore)")
    void testDeleteMissingKey() throws Exception
    {
        // Create a key that definitely doesn't exist
        IndexProto.IndexKey nonExistentKey = missingEntry.getIndexKey().toBuilder()
                .setKey(ByteString.copyFromUtf8("never_existed"))
                .build();

        // In Upsert mode, this should NOT throw exception
        IndexProto.RowLocation deletedLocation = indexService.deletePrimaryIndexEntry(nonExistentKey, indexOption);

        assertNull(deletedLocation, "Delete on missing key should return null in upsert mode");
    }

    @Test
    @Order(3)
    @DisplayName("Test Batch UPDATE with missing keys")
    void testBatchUpdateUpsert() throws Exception
    {
        List<IndexProto.PrimaryIndexEntry> entries = Collections.singletonList(
                missingEntry.toBuilder()
                        .setRowId(8888L)
                        .setIndexKey(missingEntry.getIndexKey().toBuilder().setKey(ByteString.copyFromUtf8("batch_ghost")))
                        .build()
        );

        // Should execute without throwing exception
        List<IndexProto.RowLocation> prevLocations = indexService.updatePrimaryIndexEntries(TABLE_ID, PRIMARY_INDEX_ID, entries, indexOption);

        // Even if some or all were missing, the returned list can be empty or only contain found locations
        assertNotNull(prevLocations);
    }

    @AfterAll
    static void tearDown() throws Exception
    {
        indexService.removeIndex(TABLE_ID, PRIMARY_INDEX_ID, true, indexOption);
        // PixelsConfig.set("retina.index.upsert-mode.enabled", "false");
    }
}