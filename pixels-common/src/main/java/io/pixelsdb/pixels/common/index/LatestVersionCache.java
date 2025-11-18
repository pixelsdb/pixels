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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Cache;
import io.pixelsdb.pixels.index.IndexProto;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class LatestVersionCache
{
    private final Cache<String, String> cache;

    public LatestVersionCache(long capacity, long expirationSeconds)
    {
        this.cache = Caffeine.newBuilder()
                .maximumSize(capacity)
                .expireAfterWrite(expirationSeconds, TimeUnit.SECONDS)
                .build();
    }

    public String get(String key)
    {
        return cache.getIfPresent(key);
    }

    public void put(String key, String value)
    {
        cache.put(key, value);
    }

    public void invalidate(String key)
    {
        cache.invalidate(key);
    }

    public static String buildCacheKey(IndexProto.IndexKey key)
    {
        String indexKey = key.getKey().toString(StandardCharsets.ISO_8859_1);
        return new StringBuilder(20 + 20 + indexKey.length())
                .append(key.getTableId())
                .append(key.getIndexId())
                .append(indexKey)
                .toString();
    }

    public static String buildCacheValue(long timestamp, long rowId)
    {
        char[] chars = new char[16];
        for (int i = 0; i < 8; i++)
        {
            chars[i] = (char) ((timestamp >> (i * 8)) & 0xFF);
            chars[8 + i] = (char) ((rowId >> (i * 8)) & 0xFF);
        }
        return new String(chars);
    }

    public static long[] parseCacheValue(String value)
    {
        if (value == null || value.length() != 16)
        {
            return null;
        }
        long timestamp = 0;
        long rowId = 0;
        char[] chars = value.toCharArray();

        for (int i = 0; i < 8; i++)
        {
            timestamp |= (long) (chars[i] & 0xFF) << (i * 8);
            rowId |= (long) (chars[8 + i] & 0xFF) << (i * 8);
        }
        return new long[] { timestamp, rowId };
    }
}
