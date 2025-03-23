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
package io.pixelsdb.pixels.retina;

import io.pixelsdb.pixels.core.TypeDescription;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class PixelsWriterBufferTest
{
    @Test
    public void addRowTest()
    {
        try
        {
            List<String> columnNames = new ArrayList<>();
            columnNames.add("id");
            columnNames.add("name");
            List<String> columnTypes = new ArrayList<>();
            columnTypes.add("int");
            columnTypes.add("int");
            TypeDescription schema = TypeDescription.createSchemaFromStrings(columnNames, columnTypes);
            String targetOrderDirPath = "file:///home/gengdy/data/test/ordered/";
            String targetCompactDirPath = "file:///home/gengdy/data/test/compact/";
            PixelsWriterBuffer buffer = new PixelsWriterBuffer(schema, targetOrderDirPath, targetCompactDirPath);
            CountDownLatch latch = new CountDownLatch(1);

            for (int i = 0; i < 1; ++i)
            {
                final int index = i;
                new Thread(() -> {
                    try
                    {
                        byte[][] values = new byte[columnTypes.size()][];
                        values[0] = String.valueOf(index + 1).getBytes();
                        values[1] = String.valueOf(index + 2).getBytes();
                        buffer.addRow(values, index);
                        latch.countDown();
                    } catch (Exception e)
                    {
                        e.printStackTrace();
                        latch.countDown();
                    }
                }).start();
            }
            latch.await();
            Thread.sleep(2000);
            buffer.close();
        } catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
