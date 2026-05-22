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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * In-process source of truth for the ingest-path metadata
 * {@code (fileId, tableId, virtualNodeId, firstBlockId)} of published REGULAR
 * files.
 * <p>
 * Callers must {@link #register} files of a given {@code (tableId, virtualNodeId)}
 * stream in strictly increasing {@code firstBlockId} order. That ordering is the
 * stream-append ordering enforced by {@link IngestFilePublisher} at publish time,
 * and is what allows {@link #listByStream} to return ingest order via plain
 * insertion order without an explicit sort. Out-of-order registration is treated
 * as a publisher contract violation and fails closed.
 * <p>
 * Commit timestamp bounds are intentionally omitted. A REGULAR file's commit
 * timestamp bounds are persisted by PixelsWriter in
 * {@code footer.hiddenColumnStats}, and callers that need per-segment
 * timestamps for not-yet-published data consult the source memtable directly
 * via {@link MemTable#getMinCommitTs()}.
 */
public class IngestFileMetadataRegistry
{
    private final Map<Long, Entry> entriesByFileId = new HashMap<>();
    private final Map<StreamKey, List<Entry>> entriesByStream = new HashMap<>();

    IngestFileMetadataRegistry()
    {
    }

    synchronized void register(long fileId, long tableId, int virtualNodeId,
                               long firstBlockId) throws RetinaException
    {
        Entry entry = new Entry(fileId, tableId, virtualNodeId, firstBlockId);
        Entry existing = entriesByFileId.get(fileId);
        if (existing != null)
        {
            if (existing.equals(entry))
            {
                return;
            }
            throw new RetinaException("Conflicting ingest metadata registration for fileId=" + fileId);
        }

        StreamKey streamKey = new StreamKey(tableId, virtualNodeId);
        List<Entry> streamEntries = entriesByStream.get(streamKey);
        if (streamEntries != null && !streamEntries.isEmpty())
        {
            Entry tail = streamEntries.get(streamEntries.size() - 1);
            if (firstBlockId <= tail.getFirstBlockId())
            {
                throw new RetinaException("Out-of-order ingest metadata registration for fileId=" + fileId
                        + ": firstBlockId=" + firstBlockId
                        + " must be strictly greater than prior tail firstBlockId=" + tail.getFirstBlockId());
            }
        }
        if (streamEntries == null)
        {
            streamEntries = new ArrayList<>();
            entriesByStream.put(streamKey, streamEntries);
        }
        entriesByFileId.put(fileId, entry);
        streamEntries.add(entry);
    }

    synchronized void unregister(long fileId)
    {
        Entry removed = entriesByFileId.remove(fileId);
        if (removed == null)
        {
            return;
        }
        StreamKey streamKey = new StreamKey(removed.getTableId(), removed.getVirtualNodeId());
        List<Entry> streamEntries = entriesByStream.get(streamKey);
        if (streamEntries == null)
        {
            return;
        }
        streamEntries.removeIf(entry -> entry.getFileId() == fileId);
        if (streamEntries.isEmpty())
        {
            entriesByStream.remove(streamKey);
        }
    }

    synchronized Entry get(long fileId) throws RetinaException
    {
        Entry entry = entriesByFileId.get(fileId);
        if (entry == null)
        {
            throw new RetinaException("Missing ingest metadata for fileId=" + fileId);
        }
        return entry;
    }

    synchronized boolean contains(long fileId)
    {
        return entriesByFileId.containsKey(fileId);
    }

    synchronized List<Entry> listByStream(long tableId, int virtualNodeId)
    {
        List<Entry> streamEntries = entriesByStream.get(new StreamKey(tableId, virtualNodeId));
        if (streamEntries == null)
        {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<>(streamEntries));
    }

    static final class Entry
    {
        private final long fileId;
        private final long tableId;
        private final int virtualNodeId;
        private final long firstBlockId;

        Entry(long fileId, long tableId, int virtualNodeId, long firstBlockId)
        {
            this.fileId = fileId;
            this.tableId = tableId;
            this.virtualNodeId = virtualNodeId;
            this.firstBlockId = firstBlockId;
        }

        long getFileId()
        {
            return this.fileId;
        }

        long getTableId()
        {
            return this.tableId;
        }

        int getVirtualNodeId()
        {
            return this.virtualNodeId;
        }

        long getFirstBlockId()
        {
            return this.firstBlockId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (!(o instanceof Entry))
            {
                return false;
            }
            Entry entry = (Entry) o;
            return fileId == entry.fileId && tableId == entry.tableId &&
                    virtualNodeId == entry.virtualNodeId && firstBlockId == entry.firstBlockId;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(fileId, tableId, virtualNodeId, firstBlockId);
        }
    }

    private static final class StreamKey
    {
        private final long tableId;
        private final int virtualNodeId;

        private StreamKey(long tableId, int virtualNodeId)
        {
            this.tableId = tableId;
            this.virtualNodeId = virtualNodeId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (!(o instanceof StreamKey))
            {
                return false;
            }
            StreamKey streamKey = (StreamKey) o;
            return tableId == streamKey.tableId && virtualNodeId == streamKey.virtualNodeId;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableId, virtualNodeId);
        }
    }
}
