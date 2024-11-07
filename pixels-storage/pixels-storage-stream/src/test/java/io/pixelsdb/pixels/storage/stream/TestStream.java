/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.storage.stream;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TestStream
{
    @Test
    public void test() throws IOException
    {
        Storage stream = StorageFactory.Instance().getStorage(Storage.Scheme.httpstream);
        InputStream fileInput = Files.newInputStream(Paths.get("/tmp/test1"));
        OutputStream outputStream = stream.create("stream:///localhost:29920", false, 4096);
        InputStream inputStream = stream.open("stream:///localhost:29920");
        OutputStream fileOutput = Files.newOutputStream(Paths.get("/tmp/test2"));IOUtils.copyBytes(fileInput, outputStream, 4096, true);
        IOUtils.copyBytes(inputStream, fileOutput, 4096, true);
    }
}
