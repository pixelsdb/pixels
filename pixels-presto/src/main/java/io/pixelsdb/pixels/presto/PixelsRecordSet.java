/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.presto;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import io.pixelsdb.pixels.cache.MemoryMappedFile;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.PixelsFooterCache;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Created at: 17/02/2022
 * Author: hank
 */
public class PixelsRecordSet implements RecordSet
{
    private final PixelsSplit split;
    private final List<PixelsColumnHandle> columnHandles;
    private final Storage storage;
    private final MemoryMappedFile cacheFile;
    private final MemoryMappedFile indexFile;
    private final PixelsFooterCache footerCache;
    private final String connectorId;
    private final List<Type> columnTypes;

    public PixelsRecordSet(PixelsSplit split, List<PixelsColumnHandle> columnHandles, Storage storage,
                            MemoryMappedFile cacheFile, MemoryMappedFile indexFile, PixelsFooterCache footerCache,
                            String connectorId)
    {
        this.split = requireNonNull(split, "split is null");
        this. columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.storage = requireNonNull(storage, "storage is null");
        this.cacheFile = cacheFile;
        this.indexFile = indexFile;
        this.footerCache = requireNonNull(footerCache, "footerCache is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.columnTypes = new ArrayList<>(columnHandles.size());
        for (PixelsColumnHandle columnHandle : columnHandles)
        {
            this.columnTypes.add(columnHandle.getColumnType());
        }
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return this.columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new PixelsRecordCursor(this.split, this.columnHandles, this.storage,
                this.cacheFile, this.indexFile, this.footerCache, this.connectorId);
    }
}
