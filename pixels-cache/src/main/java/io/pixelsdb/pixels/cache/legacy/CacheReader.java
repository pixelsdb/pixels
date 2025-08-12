/*
 * Copyright 2019 PixelsDB.
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
package io.pixelsdb.pixels.cache.legacy;

import io.pixelsdb.pixels.cache.PixelsCacheIdx;
import io.pixelsdb.pixels.cache.PixelsCacheKey;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface CacheReader
{
    PixelsCacheIdx search(PixelsCacheKey key);

    default PixelsCacheIdx search(long blockId, short rowGroupId, short columnId)
    {
        return search(new PixelsCacheKey(blockId, rowGroupId, columnId));
    }

    // this has a problem that the buf might not be enough to hold the result
    // we can give caller a return value to indicate this situation, but it will involve several calls
    // on the get, which might not be good; but we can do a optimization to make the CacheReader stateful
    // to memorize the last called key.
    // or the input parameter could be a DynamicArray which resizes itself.
    // TODO: caller should make sure the size is sufficient, we do the experiment first with this api
    int get(PixelsCacheKey key, byte[] buf, int size) throws IOException;

    // Note: this is not safe, as the writer might begin to write the returned ByteBuffer
    ByteBuffer getZeroCopy(PixelsCacheKey key) throws IOException;
}
