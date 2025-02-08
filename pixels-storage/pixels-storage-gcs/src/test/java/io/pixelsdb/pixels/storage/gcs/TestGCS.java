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
package io.pixelsdb.pixels.storage.gcs;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.*;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.common.physical.PhysicalWriterUtil;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

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
        io.pixelsdb.pixels.common.physical.Storage storage = io.pixelsdb.pixels.common.physical.StorageFactory.Instance().getStorage(
                io.pixelsdb.pixels.common.physical.Storage.Scheme.gcs);
        storage.mkdirs("pixels-test/hello-world/");
    }

    @Test
    public void testCreateObject() throws IOException
    {
        GCS.ConfigGCS("pixels-lab", "EUROPE-WEST6");
        io.pixelsdb.pixels.common.physical.Storage storage = io.pixelsdb.pixels.common.physical.StorageFactory.Instance().getStorage(
                io.pixelsdb.pixels.common.physical.Storage.Scheme.gcs);
        DataOutputStream output = storage.create("pixels-test/hello-world/test", false, GCS_BUFFER_SIZE);
        byte[] buffer = new byte[1024];
        Arrays.fill(buffer, (byte) 0);
        buffer[2] = 2;
        output.write(buffer);
        output.flush();
        output.close();
    }

    @Test
    public void testOpenObject() throws IOException
    {
        GCS.ConfigGCS("pixels-lab", "EUROPE-WEST6");
        io.pixelsdb.pixels.common.physical.Storage storage = io.pixelsdb.pixels.common.physical.StorageFactory.Instance().getStorage(
                io.pixelsdb.pixels.common.physical.Storage.Scheme.gcs);
        DataInputStream input = storage.open("pixels-test/hello-world/test");
        byte[] buffer = new byte[1024];
        input.readFully(buffer);
        System.out.println(buffer[0]);
        System.out.println(buffer[2]);
    }

    @Test
    public void testGCSReader() throws IOException, ExecutionException, InterruptedException
    {
        GCS.ConfigGCS("pixels-lab", "EUROPE-WEST6");
        io.pixelsdb.pixels.common.physical.Storage storage = io.pixelsdb.pixels.common.physical.StorageFactory.Instance().getStorage(
                io.pixelsdb.pixels.common.physical.Storage.Scheme.gcs);
        PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(storage, "pixels-test/hello-world/test1");
        long start = System.currentTimeMillis();
        ByteBuffer buffer = reader.readAsync(2, 8).get();
        System.out.println(buffer.get(0));
        System.out.println(buffer.get(1));
        System.out.println(System.currentTimeMillis() - start);
    }

    @Test
    public void testGCSWriter() throws IOException, ExecutionException, InterruptedException
    {
        GCS.ConfigGCS("pixels-lab", "EUROPE-WEST6");
        io.pixelsdb.pixels.common.physical.Storage storage = io.pixelsdb.pixels.common.physical.StorageFactory.Instance().getStorage(
                io.pixelsdb.pixels.common.physical.Storage.Scheme.gcs);
        PhysicalWriter writer = PhysicalWriterUtil.newPhysicalWriter(
                storage, "pixels-test/hello-world/test1", true);
        long start = System.currentTimeMillis();
        writer.prepare(1024*1024);
        byte[] buffer = new byte[1024*1024];
        Arrays.fill(buffer, (byte) 0);
        buffer[2] = 2;
        writer.append(buffer, 0, 1024*1024);
        writer.flush();
        writer.close();
        System.out.println(System.currentTimeMillis() - start);
    }
}
