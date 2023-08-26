/*
 * Copyright 2021 PixelsDB.
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
package io.pixelsdb.pixels.core.utils;

import org.junit.Test;

/**
 * Created at: 30/04/2021
 * Author: hank
 */
public class TestBitUtils
{
    @Test
    public void testPerformance()
    {
        /**
         * Issue #99:
         * Before issue #99, bitWiseDeCompact was implemented as follows:
         *
         *     public static void bitWiseDeCompact(byte[] bits, byte[] input, int offset, int length)
         *     {
         *         int bitsToRead = 1;
         *         int bitsLeft = 8;
         *         int current;
         *         byte mask = 0x01;
         *
         *         int index = 0;
         *         for (int i = offset; i < offset + length; i++)
         *         {
         *             while (bitsLeft > 0)
         *             {
         *                 bitsLeft -= bitsToRead;
         *                 current = 0x01 & (input[i] >> bitsLeft);
         *                 bits[index++] = (byte) current;
         *             }
         *             bitsLeft = 8;
         *         }
         *     }
         *
         * The 4 variables declared at the beginning of this method takes 13 byte
         * footprint in stack. By reducing the footprint to 1 byte, this method can
         * be about 30% more efficient than the original implementation if length
         * parameter is 1.
         *
         * Besides,
         * for (int i = offset; i < offset + length; i++) {}
         * is more efficient than:
         * length += offset;
         * for (int i = offset; i < length; i++) {}
         *
         * The reason is that memory access/allocation is more expensive than calculations
         * in CPU registers. Not to mention that javac may have ways to optimize (offset +
         * length) in the loop condition.
         *
         * It is similar to the other three overrides of bitWiseDeCompact.
         */
        byte[] buffer = new byte[100];
        byte[] bits = new byte[8];

        long start = System.currentTimeMillis();

        for (int i = 0; i < 1000000; ++i)
        {
            for (int j = 0; j < 100; ++j)
            {
                BitUtils.bitWiseDeCompactBE(bits, buffer, j, 1);
            }
        }

        long end = System.currentTimeMillis();

        System.out.println((end - start)/1000.0);
    }
}
