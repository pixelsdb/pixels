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
package io.pixelsdb.pixels.common.physical;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static io.pixelsdb.pixels.common.physical.storage.Redis.ConfigRedis;

/**
 * @author hank
 * Created at: 22/08/2022
 */
public class TestRedis
{
    @Test
    public void testReadWrite() throws IOException
    {
        ConfigRedis("localhost:6379", "", "");
        Storage redis = StorageFactory.Instance().getStorage(Storage.Scheme.redis);

        PhysicalWriter writer = PhysicalWriterUtil.newPhysicalWriter(redis, "test1", true);

        byte[] buffer = new byte[4096];
        buffer[1] = 2;
        for (int i = 0; i < 1024 * 100; ++i)
        {
            writer.append(buffer, 0, buffer.length);
        }
        writer.close();

        PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(redis, "test1");
        int length = (int) (reader.getFileLength());
        System.out.println(length);
        Arrays.fill(buffer, (byte) 0);
        reader.readFully(buffer, 0, 4096);
        System.out.println(buffer[1]);
        reader.close();
    }
}
