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

import io.pixelsdb.pixels.common.exception.RetinaException;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestIngestFileMetadataRegistry
{
    @Test
    public void tracksMetadataByFileIdAndStream() throws Exception
    {
        IngestFileMetadataRegistry registry = new IngestFileMetadataRegistry();

        registry.register(100L, 7L, 3, 0L);
        registry.register(100L, 7L, 3, 0L);
        registry.register(200L, 7L, 3, 10L);

        IngestFileMetadataRegistry.Entry entry = registry.get(100L);
        assertEquals(100L, entry.getFileId());
        assertEquals(7L, entry.getTableId());
        assertEquals(3, entry.getVirtualNodeId());
        assertEquals(0L, entry.getFirstBlockId());

        List<IngestFileMetadataRegistry.Entry> streamEntries = registry.listByStream(7L, 3);
        assertEquals(2, streamEntries.size());
        assertEquals(100L, streamEntries.get(0).getFileId());
        assertEquals(200L, streamEntries.get(1).getFileId());
    }

    @Test
    public void rejectsConflictsAndUnregisters() throws Exception
    {
        IngestFileMetadataRegistry registry = new IngestFileMetadataRegistry();
        registry.register(100L, 7L, 3, 0L);

        try
        {
            registry.register(100L, 7L, 3, 1L);
            fail("Expected conflicting registration to fail");
        } catch (RetinaException expected)
        {
            assertTrue(expected.getMessage().contains("Conflicting"));
        }

        registry.unregister(100L);
        assertTrue(registry.listByStream(7L, 3).isEmpty());
        assertFalse(registry.contains(100L));

        try
        {
            registry.get(100L);
            fail("Expected unregistered file metadata lookup to fail");
        } catch (RetinaException expected)
        {
            assertTrue(expected.getMessage().contains("Missing ingest metadata"));
        }
    }

    @Test
    public void unregisterRemovesOnlyMatchingStreamEntry() throws Exception
    {
        IngestFileMetadataRegistry registry = new IngestFileMetadataRegistry();

        registry.register(100L, 7L, 3, 0L);
        registry.register(200L, 7L, 3, 10L);
        registry.register(300L, 7L, 4, 0L);

        registry.unregister(100L);
        registry.unregister(999L);

        List<IngestFileMetadataRegistry.Entry> streamEntries = registry.listByStream(7L, 3);
        assertEquals(1, streamEntries.size());
        assertEquals(200L, streamEntries.get(0).getFileId());
        assertEquals(1, registry.listByStream(7L, 4).size());
    }

    @Test
    public void rejectsOutOfOrderRegistrationWithinStream() throws Exception
    {
        IngestFileMetadataRegistry registry = new IngestFileMetadataRegistry();
        registry.register(200L, 7L, 3, 10L);

        try
        {
            registry.register(100L, 7L, 3, 0L);
            fail("Expected out-of-order registration to fail");
        } catch (RetinaException expected)
        {
            assertTrue(expected.getMessage().contains("Out-of-order"));
        }

        try
        {
            registry.register(300L, 7L, 3, 10L);
            fail("Expected non-strictly-increasing firstBlockId to fail");
        } catch (RetinaException expected)
        {
            assertTrue(expected.getMessage().contains("Out-of-order"));
        }

        registry.register(300L, 7L, 4, 0L);
        assertEquals(1, registry.listByStream(7L, 4).size());
    }
}
