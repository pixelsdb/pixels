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
package io.pixelsdb.pixels.common;

import io.pixelsdb.pixels.common.physical.*;
import io.pixelsdb.pixels.common.physical.impl.S3OutputStream;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Created at: 9/8/21
 * Author: hank
 */
public class TestS3
{
    @Test
    public void testStorageScheme()
    {
        System.out.println(Storage.Scheme.fromPath("s3://container/object"));
    }

    @Test
    public void testS3Writer() throws IOException
    {
        PhysicalWriter writer = PhysicalWriterUtil.newPhysicalWriter(Storage.Scheme.s3, "pixels-01/object-4",
                0, (short) 1, false);
        ByteBuffer buffer = ByteBuffer.allocate(10240);
        buffer.putLong(1);
        writer.append(buffer);
        writer.flush();
        writer.close();
    }

    @Test
    public void testS3OutputStream() throws IOException
    {
        S3AsyncClient s3 = S3AsyncClient.builder().build();
        InputStream input = new FileInputStream("/home/hank/Downloads/JData/JData_Action_201603.csv");
        OutputStream output = new S3OutputStream(s3, "pixels-01", "object-6");
        IOUtils.copyBytes(input, output, 1024*1024, true);
    }

    @Test
    public void testS3Download() throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage("s3://pixels-00/20200828093836_0.compact_copy_20210929102009_0.pxl");
        InputStream input = storage.open("s3://pixels-00/20200828093836_0.compact_copy_20210929102009_0.pxl");
        OutputStream output = new FileOutputStream("20200828093836_0.compact_copy_20210929102009_0.pxl");
        IOUtils.copyBytes(input, output, 1024*1024, true);
    }

    @Test
    public void testGetStatus() throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage("s3://pixels-00/20200828093836_0.compact_copy_20210929102009_0.pxl");
        Status status = storage.getStatus("/pixels-00/20200828093836_0.compact_copy_20210929102009_0.pxl");
        System.out.println(status.getLength());
        System.out.println(status.getName());
        System.out.println(status.getPath());
    }

    @Test
    public void testS3Reader() throws IOException
    {
        PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(Storage.Scheme.s3, "pixels-01/object-4");
        CompletableFuture<ByteBuffer> future = reader.readAsync(8);
        future.whenComplete((resp, err) ->
        {
            if (resp != null)
            {
                System.out.println(resp.getLong());
            }
            else
            {
                err.printStackTrace();
            }
        });
        future.join();
    }
}
