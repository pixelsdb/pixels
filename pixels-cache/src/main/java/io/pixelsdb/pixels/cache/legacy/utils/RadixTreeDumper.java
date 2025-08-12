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
package io.pixelsdb.pixels.cache.legacy.utils;

import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;

// dump the little-endian radix tree to a txt file
// note that in current cache(including partitioned radix), we use big-endian radix tree more
// because the radix tree writer is directly adapted from previous java code
// yet, you can convert a big-endian radix index file by RadixIndexEndianRewriter
public class RadixTreeDumper
{
    // little endian radix tree file
    private final MemoryMappedFile indexFile;

    static
    {
        System.loadLibrary("RadixTreeDumper");
    }

    RadixTreeDumper(MemoryMappedFile indexFile)
    {
        this.indexFile = indexFile;
    }

    // traverse the index
    // currently the output file name is fixed to dumpedCache.txt in c file
    public native void nativeTraverse();

    public static void main(String[] args)
    {
        try
        {
            MemoryMappedFile index = new MemoryMappedFile("/dev/shm/pixels.index.little", 102400000);

            RadixTreeDumper cis = new RadixTreeDumper(index);
            cis.nativeTraverse();

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
