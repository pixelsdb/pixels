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
import io.pixelsdb.pixels.common.physical.*;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.pixelsdb.pixels.storage.s3.Minio.ConfigMinio;

public class TestObjectStorage
{
    @Test
    public void testObjectStorageManager()
    {
        try
        {
            ObjectStorageManager objectStorageManager = ObjectStorageManager.Instance();
            long tableId = 0;
            long entryId = 0;

            // test write
            byte[] bytes = new byte[]{2, 0, 2, 0, 2, 0, 1, 5, 4, 5};
            objectStorageManager.write(tableId, entryId, bytes);

            // test exist
            assert (objectStorageManager.exist(tableId, entryId));

            // test read
            ByteBuffer readBuffer = objectStorageManager.read(tableId, entryId);
            byte[] readBytes = new byte[readBuffer.remaining()];
            readBuffer.get(readBytes);
            System.out.println(Arrays.toString(readBytes));
            assert (Arrays.equals(bytes, readBytes));

            // test delete
            objectStorageManager.delete(tableId, entryId);

            // test exists
            assert (!objectStorageManager.exist(tableId, entryId));

        } catch (RetinaException e)
        {
            System.out.println("Failed to initialize ObjectStorageManager: " + e.getMessage());
        }
    }
}
