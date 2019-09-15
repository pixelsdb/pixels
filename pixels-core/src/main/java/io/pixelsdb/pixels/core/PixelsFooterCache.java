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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core;

import java.util.concurrent.ConcurrentHashMap;

/**
 * pixels
 *
 * @author guodong
 */
public class PixelsFooterCache
{
    private final ConcurrentHashMap<String, PixelsProto.FileTail> fileTailCacheMap;
    private final ConcurrentHashMap<String, PixelsProto.RowGroupFooter> rowGroupFooterCacheMap;

    public PixelsFooterCache()
    {
        this.fileTailCacheMap = new ConcurrentHashMap<>(200);
        this.rowGroupFooterCacheMap = new ConcurrentHashMap<>(5000);
    }

    public void putFileTail(String id, PixelsProto.FileTail fileTail)
    {
        fileTailCacheMap.putIfAbsent(id, fileTail);
    }

    public PixelsProto.FileTail getFileTail(String id)
    {
        return fileTailCacheMap.get(id);
    }

    public void putRGFooter(String id, PixelsProto.RowGroupFooter footer)
    {
        rowGroupFooterCacheMap.putIfAbsent(id, footer);
    }

    public PixelsProto.RowGroupFooter getRGFooter(String id)
    {
        return rowGroupFooterCacheMap.get(id);
    }
}
