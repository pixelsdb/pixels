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
package io.pixelsdb.pixels.common.index;

import io.pixelsdb.pixels.index.IndexProto;

import java.util.Objects;

/**
 * Journal record for restoring one primary index pointer from newRowId
 * back to oldRowId. restorePrimaryIndexEntries writes back oldRowId only when
 * the current pointer still equals newRowId, skipping entries that have
 * been tombstoned or moved on to a third rowId.
 */
public final class RollbackEntry
{
    private final IndexProto.IndexKey indexKey;
    private final long oldRowId;
    private final long newRowId;

    public RollbackEntry(IndexProto.IndexKey indexKey, long oldRowId, long newRowId)
    {
        this.indexKey = Objects.requireNonNull(indexKey, "indexKey");
        this.oldRowId = oldRowId;
        this.newRowId = newRowId;
    }

    public IndexProto.IndexKey getIndexKey()
    {
        return indexKey;
    }

    public long getOldRowId()
    {
        return oldRowId;
    }

    public long getNewRowId()
    {
        return newRowId;
    }
}
