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
package io.pixelsdb.pixels.storage.localfs;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Created at: 12/02/2022
 * Author: hank
 */
public class TestLocal
{
    @Test
    public void test() throws IOException
    {
        Storage local = StorageFactory.Instance().getStorage("file:///home/hank/test");
        List<String> paths = local.listPaths("file:///home/hank/");
        for (String path : paths)
        {
            System.out.println(path);
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(local.open("file:///home/hank/test")));
        String line;
        while ((line = reader.readLine()) != null)
        {
            System.out.println(line);
        }
    }
}
