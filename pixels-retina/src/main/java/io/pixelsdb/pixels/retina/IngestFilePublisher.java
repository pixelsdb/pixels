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
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Publishes prepared ingest files in stream-append order.
 * <p>
 * The scheduled fast-path inside {@link PixelsWriteBuffer} already drains
 * {@code fileWriterManagers} in FIFO order on a single thread, so admission
 * naturally arrives sorted by {@code firstBlockId}. This class is what keeps
 * the ordering invariant intact on the {@code close()} path, where multiple
 * drivers (the scheduler and the buffer's close thread) may race to admit
 * the same manager: every publish action runs synchronously inside the
 * monitor, and admissions whose predecessor has not yet been published are
 * parked in {@link #readyFiles} until the head of the run is publishable.
 */
final class IngestFilePublisher
{
    interface PublishAction
    {
        void publish(FileWriterManager fileWriterManager) throws RetinaException;
    }

    private final NavigableMap<Long, FileWriterManager> readyFiles = new TreeMap<>();
    private long nextCommitFirstBlockId;

    IngestFilePublisher(long nextCommitFirstBlockId)
    {
        this.nextCommitFirstBlockId = nextCommitFirstBlockId;
    }

    /**
     * The {@code firstBlockId} of the next FileWriterManager waiting to be
     * published. Since block ids are assigned monotonically and commit
     * timestamps are monotonic across blocks, this is the block whose
     * minimum ts equals the buffer's earliest not-yet-published commit ts.
     */
    synchronized long getNextCommitFirstBlockId()
    {
        return this.nextCommitFirstBlockId;
    }

    synchronized List<FileWriterManager> admitReady(FileWriterManager fileWriterManager,
                                                    PublishAction publishAction) throws RetinaException
    {
        long firstBlockId = fileWriterManager.getFirstBlockId();
        if (firstBlockId < this.nextCommitFirstBlockId)
        {
            // Already published in a previous admission. Re-admission is a
            // benign no-op so that callers (the scheduler and the close()
            // driver) can both attempt to publish without coordinating.
            return new ArrayList<>();
        }

        FileWriterManager existing = this.readyFiles.putIfAbsent(firstBlockId, fileWriterManager);
        if (existing != null && existing != fileWriterManager)
        {
            throw new RetinaException("Conflicting ingest file publisher admission for firstBlockId=" + firstBlockId);
        }

        return publishReadyPrefix(publishAction);
    }

    private List<FileWriterManager> publishReadyPrefix(PublishAction publishAction) throws RetinaException
    {
        List<FileWriterManager> published = new ArrayList<>();
        while (true)
        {
            FileWriterManager next = this.readyFiles.get(this.nextCommitFirstBlockId);
            if (next == null)
            {
                return published;
            }

            publishAction.publish(next);
            this.readyFiles.remove(this.nextCommitFirstBlockId);
            this.nextCommitFirstBlockId = next.getLastBlockId() + 1;
            published.add(next);
        }
    }
}
