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
package io.pixelsdb.pixels.cache.utils;

import io.pixelsdb.pixels.common.physical.natives.MemoryMappedFile;

// convert all big-endian to little-endian
// inplace rewrite!!!
public class RadixIndexEndianRewriter
{
    private final MemoryMappedFile indexFile;

    static
    {
        System.loadLibrary("RadixIndexEndianRewriter");
    }

    public RadixIndexEndianRewriter(MemoryMappedFile indexFile)
    {
        this.indexFile = indexFile;
    }

    public native void rewrite();

    public static void main(String[] args)
    {
        try
        {
            MemoryMappedFile index = new MemoryMappedFile("/dev/shm/pixels.index.little", 102400000);
            RadixIndexEndianRewriter cir = new RadixIndexEndianRewriter(index);
            cir.rewrite();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
