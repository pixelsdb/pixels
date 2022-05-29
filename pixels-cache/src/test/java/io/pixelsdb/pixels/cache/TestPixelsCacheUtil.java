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
package io.pixelsdb.pixels.cache;

import org.junit.Test;

/**
 * @author guodong
 */
public class TestPixelsCacheUtil
{
    @Test
    public void testHeader()
    {
        // start writing

        // done writing

        // start reading

        // done reading

        // start writing
    }

    @Test
    public void testLogicalPartitionToPhyiscal() {
        int partitions = 4;
        int free = 4; // initial status
        int startPhysical = 0;
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(0, free, startPhysical, partitions) == 0);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(1, free, startPhysical, partitions) == 1);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(2, free, startPhysical, partitions) == 2);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(3, free, startPhysical, partitions) == 3);
        try {
            PixelsCacheUtil.logicalPartitionToPhyiscal(4, free, startPhysical, partitions);
            assert (false); // should throw before
        } catch (IndexOutOfBoundsException ignored) {}

        free = 0;
        startPhysical = 4;
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(0, free, startPhysical, partitions) == 4);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(1, free, startPhysical, partitions) == 1);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(2, free, startPhysical, partitions) == 2);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(3, free, startPhysical, partitions) == 3);

        free = 1;
        startPhysical = 4;
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(0, free, startPhysical, partitions) == 4);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(1, free, startPhysical, partitions) == 0);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(2, free, startPhysical, partitions) == 2);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(3, free, startPhysical, partitions) == 3);

        free = 2;
        startPhysical = 4;
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(0, free, startPhysical, partitions) == 4);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(1, free, startPhysical, partitions) == 0);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(2, free, startPhysical, partitions) == 1);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(3, free, startPhysical, partitions) == 3);

        free = 3;
        startPhysical = 4;
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(0, free, startPhysical, partitions) == 4);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(1, free, startPhysical, partitions) == 0);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(2, free, startPhysical, partitions) == 1);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(3, free, startPhysical, partitions) == 2);

        free = 4;
        startPhysical = 3;
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(0, free, startPhysical, partitions) == 3);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(1, free, startPhysical, partitions) == 0);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(2, free, startPhysical, partitions) == 1);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(3, free, startPhysical, partitions) == 2);

        free = 0;
        startPhysical = 3;
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(0, free, startPhysical, partitions) == 3);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(1, free, startPhysical, partitions) == 4);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(2, free, startPhysical, partitions) == 1);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(3, free, startPhysical, partitions) == 2);

        free = 1;
        startPhysical = 3;
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(0, free, startPhysical, partitions) == 3);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(1, free, startPhysical, partitions) == 4);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(2, free, startPhysical, partitions) == 0);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(3, free, startPhysical, partitions) == 2);

        free = 2;
        startPhysical = 3;
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(0, free, startPhysical, partitions) == 3);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(1, free, startPhysical, partitions) == 4);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(2, free, startPhysical, partitions) == 0);
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(3, free, startPhysical, partitions) == 1);

        partitions = 1;
        free = 1;
        startPhysical = 0;
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(0, free, startPhysical, partitions) == 0);

        free = 0;
        startPhysical = 1;
        assert(PixelsCacheUtil.logicalPartitionToPhyiscal(0, free, startPhysical, partitions) == 1);




    }
}
