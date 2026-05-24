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
 * Result of a successful primary index resolution, returned wrapped in
 * {@link java.util.Optional}: present = key is live; empty = key missing or
 * maps to an orphan / non-baseline-visible location; backend failures surface
 * as {@link io.pixelsdb.pixels.common.exception.IndexException}.
 */
public final class ResolvedPrimary
{
    private final long rowId;
    private final IndexProto.RowLocation rowLocation;

    public ResolvedPrimary(long rowId, IndexProto.RowLocation rowLocation)
    {
        this.rowId = rowId;
        this.rowLocation = Objects.requireNonNull(rowLocation, "rowLocation");
    }

    public long getRowId()
    {
        return rowId;
    }

    public IndexProto.RowLocation getRowLocation()
    {
        return rowLocation;
    }
}
