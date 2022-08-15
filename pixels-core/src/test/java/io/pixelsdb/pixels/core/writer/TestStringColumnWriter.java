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
package io.pixelsdb.pixels.core.writer;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

/**
 * @author hank
 * @date 8/13/22
 */
public class TestStringColumnWriter
{
    @Test
    public void test() throws IOException
    {
        BinaryColumnVector stringColumnVector = new BinaryColumnVector(10000);
        stringColumnVector.reset();
        stringColumnVector.init();
        for (int i = 0; i < 10000; ++i)
        {
            stringColumnVector.add(UUID.randomUUID().toString());
        }
        StringColumnWriter stringColumnWriter = new StringColumnWriter(
                TypeDescription.createString(), 10000, true);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i)
        {
            stringColumnWriter.write(stringColumnVector, stringColumnVector.getLength());
        }
        System.out.println(System.currentTimeMillis() - startTime);
    }
}
