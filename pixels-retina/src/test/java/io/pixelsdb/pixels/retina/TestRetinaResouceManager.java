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

import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class TestRetinaResouceManager
{
    private final RetinaResourceManager retinaResourceManager = RetinaResourceManager.Instance();

    private void printVisibility(long[] visibility)
    {
        for (long num : visibility)
        {
            System.out.print(String.format(
                    "%64s", Long.toBinaryString(num)).replace(' ', '0'));
        }
        System.out.println();
    }

    private boolean checkVisibility(long[] visibility, int rowId)
    {
        long targetLong = visibility[rowId / 64];
        return (targetLong & (1L << (rowId % 64))) != 0;
    }

    @Test
    public void TestVisibility()
    {
        try
        {
            long fileId = 999;
            int rgId = 666;
            int recordNum = 100;
            retinaResourceManager.addVisibility(fileId, rgId, recordNum);
            long [] visibility = retinaResourceManager.queryVisibility(fileId, rgId, 0);
            printVisibility(visibility);

            // delete row
            retinaResourceManager.deleteRecord(fileId, rgId, 33, 5);
            visibility = retinaResourceManager.queryVisibility(fileId, rgId, 0);
            printVisibility(visibility);
            visibility = retinaResourceManager.queryVisibility(fileId, rgId, 10);
            printVisibility(visibility);
            System.out.println(checkVisibility(visibility, 33));
        } catch (RetinaException e)
        {
            throw new RuntimeException(e);
        }
    }

    private byte[][] createTpchNationRow(int nationKey, String name, int regionKey, String comment)
    {
        byte[][] row  = new byte[4][];
        row[0] = String.valueOf(nationKey).getBytes();
        row[1] = name.getBytes();
        row[2] = String.valueOf(regionKey).getBytes();
        row[3] = comment.getBytes();
        return row;
    }

    @Test
    public void testWriterBuffer()
    {
        try
        {
            String schemaName = "tpch";
            String tableName = "nation";
            retinaResourceManager.addWriterBuffer(schemaName, tableName);

            // insert data
            byte[][] colValues = createTpchNationRow(2333, "ALGERIA",
                    0, "haggle. carefully final deposits detect slyly agai");
            retinaResourceManager.insertRecord(schemaName, tableName, colValues, 5);
            RetinaProto.GetWriterBufferResponse response =
                    retinaResourceManager.getWriterBuffer(schemaName, tableName, 0);
            System.out.println(response.getData().isEmpty() ? "empty"  :
                    VectorizedRowBatch.deserialize(response.getData().toByteArray()));
            System.out.println(response.getBitmaps(0).getBitmapCount());
            System.out.println(response.getBitmaps(0));

            // delete record
            long fileId = 322;  // obtained during debug
            int rgId = 0;
            int rgRowId = 0;
            retinaResourceManager.deleteRecord(fileId, rgId, rgRowId, 10);
            long [] visibility = retinaResourceManager.queryVisibility(fileId, rgId, 0);
            printVisibility(visibility);
            visibility = retinaResourceManager.queryVisibility(fileId, rgId, 10);
            printVisibility(visibility);
            System.out.println(checkVisibility(visibility, rgRowId));
        } catch (RetinaException e)
        {
            throw new RuntimeException(e);
        }
    }
}
