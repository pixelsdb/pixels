/*
 * Copyright 2020 PixelsDB.
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
package io.pixelsdb.pixels.storage.hdfs;

import org.junit.Test;

import java.io.IOException;

/**
 * @author hank
 * @create 2020-05-18
 */
public class TestOzone
{
    @Test
    public void test () throws IOException
    {
        /*
        OzoneClient ozClient = OzoneClientFactory.getClient();
        ObjectStore objectStore = ozClient.getObjectStore();
        OzoneVolume assets = objectStore.getVolume("assets");
        OzoneBucket video = assets.getBucket("videos");
        video.readKey("").skip(3);
        */
    }
}
