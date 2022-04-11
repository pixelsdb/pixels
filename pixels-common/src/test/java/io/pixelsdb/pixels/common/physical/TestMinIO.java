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
import java.util.List;

import static io.pixelsdb.pixels.common.physical.storage.MinIO.ConfigMinIO;

/**
 * @author hank
 * Created at: 10/04/2022
 */
public class TestMinIO
{
    @Test
    public void testReadWrite() throws IOException
    {
        ConfigMinIO("http://localhost:9000", "minio", "password");
        Storage minio = StorageFactory.Instance().getStorage(Storage.Scheme.minio);
        List<String> files = minio.listPaths("test/");
        for (String file : files)
        {
            System.out.println(file);
        }
        PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(minio, files.get(0));
        System.out.println(reader.supportsAsync());
        PhysicalWriter writer = PhysicalWriterUtil.newPhysicalWriter(minio,
                files.get(0) + ".out", true);

        byte[] buffer = new byte[4096];
        int offset = 0;
        while (offset + buffer.length < reader.getFileLength())
        {
            reader.readFully(buffer);
            writer.append(buffer, 0, buffer.length);
            offset += buffer.length;
        }

        int length = (int) (reader.getFileLength()-offset);
        reader.readFully(buffer, 0, length);
        writer.append(buffer, 0, length);

        reader.close();
        writer.close();
    }
}
