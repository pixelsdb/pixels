/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.storage.s3;

import io.pixelsdb.pixels.common.physical.*;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Dongyang Geng
 * @create 2026-07-07
 */
public class TestCos
{
    @Test
    public void testReadWrite() throws IOException
    {
        Storage cos = StorageFactory.Instance().getStorage(Storage.Scheme.cos);
        String path = "cos://bucket/pixels-cos-test/";
        List<String> files = cos.listPaths(path);
        for (String file : files)
        {
            System.out.println(file);
        }

        String testPath = path + "pixels-cos-test-" + UUID.randomUUID().toString();
        String content = "hello from pixels cos storage";

        // write the file
        PhysicalWriter writer = PhysicalWriterUtil.newPhysicalWriter(cos, testPath, true);
        writer.append(content.getBytes(StandardCharsets.UTF_8), 0, content.length());
        writer.close();

        // read the file
        PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(cos, testPath);
        byte[] actual = new byte[(int) reader.getFileLength()];
        reader.readFully(actual);
        reader.close();
        String actualContent = new String(actual, StandardCharsets.UTF_8);
        assertEquals(content, actualContent);

        // delete the file
        cos.delete(testPath, false);
        assertFalse(cos.exists(testPath));
    }
}
