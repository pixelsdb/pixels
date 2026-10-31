/*
 * Copyright 2017-2019 PixelsDB.
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
package io.pixelsdb.pixels.core;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsFooterCache
{
    private final Cache<String, PixelsProto.FileTail> fileTailCacheMap;
    private final Cache<String, PixelsProto.RowGroupFooter> rowGroupFooterCacheMap;

    public PixelsFooterCache()
    {
        this.fileTailCacheMap = CacheBuilder.newBuilder().initialCapacity(200).build();
        this.rowGroupFooterCacheMap = CacheBuilder.newBuilder().initialCapacity(5000).build();
    }

    /** Entry limits for long-lived workers; active readers retain their own proto references. */
    public PixelsFooterCache(long fileTails, long rowGroupFooters)
    {
        this.fileTailCacheMap = CacheBuilder.newBuilder().maximumSize(fileTails).build();
        this.rowGroupFooterCacheMap = CacheBuilder.newBuilder().maximumSize(rowGroupFooters).build();
    }

    public void putFileTail(String id, PixelsProto.FileTail fileTail)
    {
        fileTailCacheMap.asMap().putIfAbsent(id, fileTail);
    }

    public PixelsProto.FileTail getFileTail(String id)
    {
        return fileTailCacheMap.getIfPresent(id);
    }

    public void putRGFooter(String id, PixelsProto.RowGroupFooter footer)
    {
        rowGroupFooterCacheMap.asMap().putIfAbsent(id, footer);
    }

    public PixelsProto.RowGroupFooter getRGFooter(String id)
    {
        return rowGroupFooterCacheMap.getIfPresent(id);
    }
}
