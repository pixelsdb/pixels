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
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.reader;

import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestFileMetadata
{
    @Test
    public void test()
    {
        PixelsReader pixelsReader = null;
        //String filePath = "hdfs://presto00:9000/pixels/testNull_pixels/201806190954180.pxl";
        String filePath = "hdfs://presto00:9000/pixels/pixels/testnull_pixels/v_0_order/";
        try
        {
            Storage storage = StorageFactory.Instance().getStorage("hdfs");
            List<Status> fileStatuses = storage.listStatus(filePath);
            int i = 0;
            for (Status fileStatus : fileStatuses)
            {
                pixelsReader = PixelsReaderImpl.newBuilder()
                        .setStorage(storage)
                        .setPath(fileStatus.getPath())
                        .build();
//                System.out.println(pixelsReader.getRowGroupNum());
                if (pixelsReader.getFooter().getRowGroupStatsList().size() != 1)
                {
                    System.out.println("Path: " + fileStatus.getPath() + ", RGNum: " + pixelsReader.getRowGroupNum());
                }
                i++;
                pixelsReader.close();
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
