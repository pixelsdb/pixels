/*
 * Copyright 2021 PixelsDB.
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

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.*;
import io.pixelsdb.pixels.common.physical.storage.GCS;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static io.pixelsdb.pixels.common.utils.Constants.GCS_BUFFER_SIZE;

/**
 * @author hank
 * @date 9/24/22
 */
public class TestGCS
{
    @Test
    public void testRawUpload()
    {
        Storage storage = StorageOptions.newBuilder().setProjectId("pixels-lab").build().getService();
        BlobId blobId = BlobId.of("pixels-cf-test", "test");
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        byte[] buffer = new byte[1024];
        Arrays.fill(buffer, (byte) 0);
        buffer[2] = 2;
        storage.create(blobInfo, buffer);
    }

    @Test
    public void testRawDownload()
    {
        Storage storage = StorageOptions.newBuilder().setProjectId("pixels-lab").build().getService();
        BlobId blobId = BlobId.of("pixels-cf-test", "test");
        //Blob blob = storage.get(blobId);
        StorageBatch storageBatch = storage.batch();
        StorageBatchResult<Blob> result = storageBatch.get(blobId);
        storageBatch.submit();
        byte[] buffer = result.get().getContent();
        System.out.println("buffer[2]:" + buffer[2] + "," + buffer[1]);

        try (ReadChannel from = storage.reader(blobId))
        {
            from.seek(2);
            from.limit(4);
            ByteBuffer byteBuffer = ByteBuffer.allocate(2);
            from.read(byteBuffer);
            System.out.println(byteBuffer.get(0));
            System.out.println(byteBuffer.get(1));
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void testMkdir() throws IOException
    {
        GCS.ConfigGCS("pixels-lab", "EUROPE-WEST6");
        io.pixelsdb.pixels.common.physical.Storage storage = StorageFactory.Instance().getStorage(
                io.pixelsdb.pixels.common.physical.Storage.Scheme.gcs);
        storage.mkdirs("pixels-test/hello-world/");
        storage.close();
    }

    @Test
    public void testCreateObject() throws IOException
    {
        GCS.ConfigGCS("pixels-lab", "EUROPE-WEST6");
        io.pixelsdb.pixels.common.physical.Storage storage = StorageFactory.Instance().getStorage(
                io.pixelsdb.pixels.common.physical.Storage.Scheme.gcs);
        DataOutputStream output = storage.create("pixels-test/hello-world/test", false, GCS_BUFFER_SIZE);
        byte[] buffer = new byte[1024];
        Arrays.fill(buffer, (byte) 0);
        buffer[2] = 2;
        output.write(buffer);
        output.flush();
        output.close();
        storage.close();
    }

    @Test
    public void testOpenObject() throws IOException
    {
        GCS.ConfigGCS("pixels-lab", "EUROPE-WEST6");
        io.pixelsdb.pixels.common.physical.Storage storage = StorageFactory.Instance().getStorage(
                io.pixelsdb.pixels.common.physical.Storage.Scheme.gcs);
        DataInputStream input = storage.open("pixels-test/hello-world/test");
        byte[] buffer = new byte[1024];
        input.readFully(buffer);
        System.out.println(buffer[0]);
        System.out.println(buffer[2]);
        storage.close();
    }
}
